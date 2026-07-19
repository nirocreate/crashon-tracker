"""
Crash-On Analysis - Crash game WebSocket recorder backend.

Connects to the GameON Crash game (game id 42) Socket.IO endpoint, mimics a
real browser connection, records all crash-related events, computes per-round
analytics, persists everything to a SQLite database, and exposes the data
through a local Flask server with a live HTML dashboard.

Why a browser is used for the WebSocket:
  The public host `crash.gameonworld.ai` sits behind Cloudflare bot
  protection. A plain WebSocket client (websocket-client / aiohttp) receives
  HTTP 400 on the handshake because it lacks the browser's `cf_clearance`
  cookie and TLS/JA3 fingerprint. We therefore drive a real headless Chromium
  (Playwright): it obtains the Cloudflare clearance cookie by loading the game
  page, then opens the Socket.IO WebSocket from inside the page context where
  the cookie and fingerprint are valid. The Socket.IO protocol itself is
  spoken manually:

    - Engine.IO v4 handshake:  wss://crash.gameonworld.ai/socket.io/?EIO=4&transport=websocket
    - server -> 0{...open...}      client -> 40   (join default namespace)
    - server -> 40{...}            client -> 42["joinMarket"]
    - server emits: crashState, crashTick, roundCrashed, liquidityUpdate,
                    playerCashedOut
    - ping(2) / pong(3) keepalive

Per-round metrics computed from the events:
  round ID, total bet value, number of bets, cashout multiplier(s),
  house edge, available liquidity, amount won by players, house profit.
  The richest event is `roundCrashed` (carries serverSeed, houseEdge,
  houseProfit and the final bet list), so the authoritative round record and
  DB row are built from it.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, timezone

from flask import Flask, jsonify, make_response, render_template, request, Response
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("crash-on")

GAME_URL = "https://gameonworld.ai/game/42"
WS_URL = "wss://crash.gameonworld.ai/socket.io/?EIO=4&transport=websocket"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crashon.db")

# In-memory store ---------------------------------------------------------

lock = threading.Lock()
rounds = deque(maxlen=500)          # finished rounds (metrics-enriched)
live_state = None
ticks = deque(maxlen=2000)
events = deque(maxlen=500)
connected = False
last_msg_ts = 0.0
start_ts = time.time()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def record_event(etype, payload):
    with lock:
        events.append({"type": etype, "ts": now_iso(), "payload": payload})


# SQLite ----------------------------------------------------------------

_db = sqlite3.connect(DB_PATH, check_same_thread=False)
_db.row_factory = sqlite3.Row
_db_lock = threading.Lock()


def init_db():
    with _db_lock:
        _db.execute("""
            CREATE TABLE IF NOT EXISTS rounds (
                round_id          TEXT PRIMARY KEY,
                crash_point       REAL,
                phase             TEXT,
                server_seed       TEXT,
                server_seed_hash  TEXT,
                public_seed       TEXT,
                total_bets        INTEGER,
                total_bet_value   REAL,
                amount_won        REAL,
                house_edge        REAL,
                house_profit      REAL,
                available_liquidity REAL,
                cashouts          TEXT,    -- JSON array of {displayName, stake, cashoutMultiplier, payout}
                crashed_at        INTEGER,
                recorded_at       TEXT
            )
        """)
        _db.commit()


_round_cache = {}   # roundId -> enriched record (merged across events)


def _bets_value(bets, key):
    return round(sum(float(b.get(key) or 0) for b in bets), 2)


def compute_metrics(arg):
    """Build/merge an enriched round record with the requested analytics."""
    rid = arg.get("roundId")
    if not rid:
        return None
    with lock:
        rec = _round_cache.get(rid, {"roundId": rid})

    # Pull every field we can from whichever event carries it.
    if arg.get("crashPoint") is not None:
        rec["crashPoint"] = arg.get("crashPoint")
    if arg.get("phase"):
        rec["phase"] = arg.get("phase")
    if arg.get("serverSeed") is not None:
        rec["serverSeed"] = arg.get("serverSeed")
    if arg.get("serverSeedHash") is not None:
        rec["serverSeedHash"] = arg.get("serverSeedHash")
    if arg.get("publicSeed") is not None:
        rec["publicSeed"] = arg.get("publicSeed")
    if arg.get("houseEdge") is not None:
        rec["houseEdge"] = arg.get("houseEdge")
    if arg.get("houseProfit") is not None:
        rec["houseProfit"] = arg.get("houseProfit")
    if arg.get("availableLiquidity") is not None:
        rec["availableLiquidity"] = arg.get("availableLiquidity")

    bets = arg.get("bets", []) or []
    if bets:
        rec["totalBets"] = len(bets)
        rec["totalBetValue"] = _bets_value(bets, "stake")
        rec["amountWon"] = _bets_value(bets, "payout")
        rec["cashouts"] = [
            {
                "displayName": b.get("displayName"),
                "stake": b.get("stake"),
                "cashoutMultiplier": b.get("cashoutMultiplier"),
                "payout": b.get("payout"),
            }
            for b in bets
            if b.get("cashedOut")
        ]

    # Crash time: prefer explicit crashedAt, else derive from roundId timestamp.
    if arg.get("crashedAt"):
        rec["crashedAt"] = arg.get("crashedAt")
    elif rec.get("crashedAt") is None and rid.startswith("CR-"):
        try:
            rec["crashedAt"] = int(rid.split("CR-", 1)[1])
        except Exception:
            pass

    rec["recordedAt"] = now_iso()
    with lock:
        _round_cache[rid] = rec
    return rec


def persist_round(rec):
    """Insert/update the round in SQLite (idempotent on round_id)."""
    with _db_lock:
        _db.execute(
            """
            INSERT INTO rounds (
                round_id, crash_point, phase, server_seed, server_seed_hash,
                public_seed, total_bets, total_bet_value, amount_won,
                house_edge, house_profit, available_liquidity, cashouts,
                crashed_at, recorded_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(round_id) DO UPDATE SET
                crash_point=excluded.crash_point,
                phase=excluded.phase,
                server_seed=excluded.server_seed,
                total_bets=excluded.total_bets,
                total_bet_value=excluded.total_bet_value,
                amount_won=excluded.amount_won,
                house_edge=excluded.house_edge,
                house_profit=excluded.house_profit,
                available_liquidity=excluded.available_liquidity,
                cashouts=excluded.cashouts,
                crashed_at=excluded.crashed_at,
                recorded_at=excluded.recorded_at
            """,
            (
                rec["roundId"], rec.get("crashPoint"), rec.get("phase"),
                rec.get("serverSeed"), rec.get("serverSeedHash"),
                rec.get("publicSeed"), rec.get("totalBets"),
                rec.get("totalBetValue"), rec.get("amountWon"),
                rec.get("houseEdge"), rec.get("houseProfit"),
                rec.get("availableLiquidity"), json.dumps(rec.get("cashouts", [])),
                rec.get("crashedAt"), rec.get("recordedAt"),
            ),
        )
        _db.commit()


def flush_round(rid):
    """Persist the merged round record for rid and add it to the in-memory deque."""
    with lock:
        rec = _round_cache.get(rid)
        if not rec:
            return
        for r in rounds:
            if r.get("roundId") == rid:
                break
        else:
            rounds.append(rec)
    persist_round(rec)


def handle_event(event, args):
    global live_state
    arg = args[0] if args else {}

    if event == "crashState":
        with lock:
            live_state = arg
        record_event("crashState", arg)
        if arg.get("phase") == "crashed":
            compute_metrics(arg)
            flush_round(arg.get("roundId"))
        log.info("crashState phase=%s round=%s mult=%s",
                 arg.get("phase"), arg.get("roundId"), arg.get("multiplier"))
    elif event == "crashTick":
        with lock:
            ticks.append({"ts": now_iso(), **arg})
        record_event("crashTick", arg)
    elif event == "roundCrashed":
        compute_metrics(arg)
        flush_round(arg.get("roundId"))
        record_event("roundCrashed", arg)
        rec = _round_cache.get(arg.get("roundId"), {})
        log.info("roundCrashed round=%s crashPoint=%s bets=%d won=%s profit=%s",
                 arg.get("roundId"), arg.get("crashPoint"),
                 rec.get("totalBets"), rec.get("amountWon"), arg.get("houseProfit"))
    elif event == "liquidityUpdate":
        record_event("liquidityUpdate", arg)
    elif event == "playerCashedOut":
        record_event("playerCashedOut", arg)
    else:
        record_event(event, arg)
        log.info("unhandled event=%s", event)


# In-page socket driver (runs inside the browser) ------------------------
#
# This string is executed in the page context. It opens the Socket.IO
# WebSocket and forwards each decoded event to window.__onCrashEvent(type, json).

IN_PAGE_DRIVER = r"""
(() => {
  const url = %r;
  const ws = new WebSocket(url);
  let joined = false;
  window.__crashWs = ws;

  function emit(type, payload) {
    try { window.__onCrashEvent(type, JSON.stringify(payload)); } catch (e) {}
  }

  ws.onopen = () => { window.__onCrashEvent('socketOpen', '{}'); };

  ws.onmessage = (e) => {
    const d = e.data;
    if (d[0] === '0') {                 // engine.io open
      ws.send('40');                     // join default namespace
    } else if (d[0] === '4' && d[1] === '0') {   // socket.io connect ack
      if (!joined) { ws.send('42["joinMarket"]'); joined = true; }
    } else if (d[0] === '2') {          // ping
      ws.send('3');                      // pong
    } else if (d[0] === '4' && d[1] === '2') {   // socket.io event
      try {
        const arr = JSON.parse(d.slice(2));
        emit(arr[0], arr.length > 1 ? arr[1] : {});
      } catch (_) {}
    }
  };

  ws.onclose = () => { window.__onCrashEvent('socketClose', '{}'); };
  ws.onerror = () => { window.__onCrashEvent('socketError', '{}'); };
})();
""" % WS_URL


def browser_collector_loop():
    """Run a headless browser, load the game page (to get cf_clearance), then
    open the Socket.IO socket from inside the page and funnel events out."""
    global connected, last_msg_ts
    while True:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(ignore_https_errors=True)
                page = ctx.new_page()

                def on_event(ev_type, payload_json):
                    global last_msg_ts, connected
                    last_msg_ts = time.time()
                    if ev_type in ("socketOpen", "socketClose", "socketError"):
                        connected = (ev_type == "socketOpen")
                        record_event(ev_type, json.loads(payload_json or "{}"))
                        return
                    try:
                        payload = json.loads(payload_json or "{}")
                    except Exception:
                        payload = {}
                    connected = True
                    last_msg_ts = time.time()
                    if isinstance(payload, list):
                        handle_event(ev_type, payload)
                    else:
                        handle_event(ev_type, [payload])

                page.expose_function("__onCrashEvent", on_event)

                log.info("Loading game page to obtain Cloudflare clearance: %s", GAME_URL)
                page.goto(GAME_URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2500)

                log.info("Opening in-page Socket.IO connection: %s", WS_URL)
                page.evaluate(IN_PAGE_DRIVER)

                while True:
                    time.sleep(5)
                    state = page.evaluate(
                        "() => window.__crashWs ? window.__crashWs.readyState : -1"
                    )
                    if state is None or state in (2, 3):  # closing/closed
                        log.info("In-page socket state=%s, re-opening", state)
                        page.evaluate(IN_PAGE_DRIVER)

        except Exception as e:
            connected = False
            log.error("Browser collector error: %s", e)
        finally:
            connected = False
            try:
                browser.close()
            except Exception:
                pass
            log.info("Browser collector stopped, retrying in 5s")
            time.sleep(5)


# Flask app ---------------------------------------------------------------

app = Flask(__name__)


@app.route("/")
def index():
    resp = make_response(render_template("index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/api/status")
def api_status():
    with lock:
        return jsonify({
            "connected": connected,
            "uptime_seconds": int(time.time() - start_ts),
            "last_message_seconds_ago": (
                int(time.time() - last_msg_ts) if last_msg_ts else None
            ),
            "rounds_recorded": len(rounds),
            "ticks_recorded": len(ticks),
            "events_recorded": len(events),
            "db_rounds": db_count(),
            "live_phase": live_state.get("phase") if live_state else None,
            "live_round": live_state.get("roundId") if live_state else None,
            "live_multiplier": live_state.get("multiplier") if live_state else None,
        })


@app.route("/api/rounds")
def api_rounds():
    with lock:
        return jsonify(list(rounds))


def db_count():
    with _db_lock:
        row = _db.execute("SELECT COUNT(*) AS c FROM rounds").fetchone()
    return row["c"] if row else 0


@app.route("/api/db/count")
def api_db_count():
    return jsonify({"count": db_count()})


@app.route("/api/db/aggregates")
def api_db_aggregates():
    """Cumulative, server-independent analytics for the live chart.

    House profit is computed locally as: sum(total_bet_value) - sum(amount_won)
    (the server's reported houseProfit / availableLiquidity are NOT trusted).
    Returns overall totals plus an ordered time-series (oldest -> newest) so
    the chart can plot cumulative house profit / bets / wins over time.
    """
    with _db_lock:
        rows = _db.execute(
            "SELECT round_id, crashed_at, total_bet_value, amount_won "
            "FROM rounds WHERE crashed_at IS NOT NULL "
            "ORDER BY crashed_at ASC"
        ).fetchall()

    series = []
    cum_bets = 0.0
    cum_won = 0.0
    for r in rows:
        bets = float(r["total_bet_value"] or 0)
        won = float(r["amount_won"] or 0)
        cum_bets += bets
        cum_won += won
        cum_profit = cum_bets - cum_won
        series.append({
            "round_id": r["round_id"],
            "crashed_at": r["crashed_at"],
            "bets": round(bets, 2),
            "won": round(won, 2),
            "profit": round(bets - won, 2),
            "cum_bets": round(cum_bets, 2),
            "cum_won": round(cum_won, 2),
            "cum_profit": round(cum_profit, 2),
        })

    totals = series[-1] if series else {
        "cum_bets": 0.0, "cum_won": 0.0, "cum_profit": 0.0
    }
    agg_bets = round(sum(s["bets"] for s in series), 2)
    agg_won = round(sum(s["won"] for s in series), 2)
    return jsonify({
        "rounds": len(series),
        "total_bets": totals["cum_bets"],
        "total_won": totals["cum_won"],
        "house_profit": totals["cum_profit"],
        "agg_bets": agg_bets,
        "agg_won": agg_won,
        "agg_profit": round(agg_bets - agg_won, 2),
        "series": series,
    })


@app.route("/api/db/rounds")
def api_db_rounds():
    """Rounds from the database (most recent first). Optional ?limit=N (0=all)."""
    try:
        limit = int(request.args.get("limit", 20))
    except Exception:
        limit = 20
    with _db_lock:
        if limit and limit > 0:
            rows = _db.execute(
                "SELECT * FROM rounds ORDER BY crashed_at DESC LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = _db.execute(
                "SELECT * FROM rounds ORDER BY crashed_at DESC"
            ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["cashouts"] = json.loads(d["cashouts"]) if d["cashouts"] else []
        except Exception:
            d["cashouts"] = []
        out.append(d)
    return jsonify(out)


@app.route("/api/db/rounds/all")
def api_db_rounds_all():
    with _db_lock:
        rows = _db.execute(
            "SELECT * FROM rounds ORDER BY crashed_at DESC"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["cashouts"] = json.loads(d["cashouts"]) if d["cashouts"] else []
        except Exception:
            d["cashouts"] = []
        out.append(d)
    return jsonify(out)


@app.route("/api/db/predictions")
def api_db_predictions():
    """Guess the next crash multiplier and report guess accuracy.

    Heuristic (transparent, data-driven, not trusting server seeds):
      - Take the most recent N crash points (default 30).
      - predicted_next = median of recent crashes, blended 50/50 with the
        mean. This is a simple, defensible baseline ("the next crash looks
        like recent crashes").
      - Back-test: for each round i (after the first window), predict from the
        preceding window and compare to the actual crash point, producing
        accuracy stats (avg absolute error, % within 1x / 2x).
    """
    try:
        N = int(request.args.get("window", 30))
    except Exception:
        N = 30
    if N < 3:
        N = 3

    with _db_lock:
        rows = _db.execute(
            "SELECT round_id, crash_point FROM rounds "
            "WHERE crash_point IS NOT NULL ORDER BY crashed_at ASC"
        ).fetchall()

    pts = [(r["round_id"], float(r["crash_point"])) for r in rows]
    if len(pts) < N + 1:
        return jsonify({
            "window": N,
            "samples": len(pts),
            "predicted_next": None,
            "method": "need at least %d rounds" % (N + 1),
            "accuracy": None,
            "series": [],
        })

    recent = [p for _, p in pts[-N:]]
    median = sorted(recent)[len(recent) // 2]
    mean = sum(recent) / len(recent)
    predicted_next = round((median + mean) / 2, 2)

    # Back-test: predict round i using the N points before it.
    preds = []
    errs = []
    for i in range(N, len(pts)):
        window = [p for _, p in pts[i - N:i]]
        m = sorted(window)[len(window) // 2]
        avg = sum(window) / len(window)
        guess = (m + avg) / 2
        actual = pts[i][1]
        err = abs(guess - actual)
        errs.append(err)
        preds.append({
            "round_id": pts[i][0],
            "guess": round(guess, 2),
            "actual": round(actual, 2),
            "error": round(err, 2),
        })

    avg_err = round(sum(errs) / len(errs), 3)
    acc1 = round(100.0 * sum(1 for e in errs if e <= 1.0) / len(errs), 1)
    acc2 = round(100.0 * sum(1 for e in errs if e <= 2.0) / len(errs), 1)

    # RTP derived from observed house edge (WebSocket reports houseEdge per
    # round; insider-confirmed at 1% => RTP 0.99). Fallback to 0.99.
    with _db_lock:
        he = _db.execute(
            "SELECT AVG(house_edge) AS h FROM rounds WHERE house_edge IS NOT NULL"
        ).fetchone()
    obs_edge = float(he["h"]) if he and he["h"] is not None else 0.01
    if not (0 < obs_edge < 1):
        obs_edge = 0.01
    rtp = round(1.0 - obs_edge, 4)

    return jsonify({
        "window": N,
        "samples": len(pts),
        "predicted_next": predicted_next,
        "rtp": rtp,
        "house_edge": round(obs_edge, 4),
        "method": "median+mean of last %d crashes" % N,
        "accuracy": {
            "guesses": len(preds),
            "avg_error": avg_err,
            "within_1x": acc1,
            "within_2x": acc2,
        },
        "series": preds,
    })


OUTCOME_BUCKETS = [
    ("1x",           1.0,  1.0001),
    ("1x - 1.2x",    1.0,  1.2),
    ("1.2x - 1.5x",  1.2,  1.5),
    ("1.5x - 2x",    1.5,  2.0),
    ("2x - 5x",      2.0,  5.0),
    ("5x - 10x",     5.0,  10.0),
    ("10x - up",     10.0, float("inf")),
]


@app.route("/api/db/outcomes")
def api_db_outcomes():
    """All-time probability distribution of crash points across fixed ranges.

    Computed locally from recorded crash points (server seeds/liquidity are
    not trusted). The first bucket '1x' counts exact-1x crashes; the rest are
    half-open ranges. Probabilities sum to 100%.
    """
    with _db_lock:
        rows = _db.execute(
            "SELECT crash_point FROM rounds WHERE crash_point IS NOT NULL"
        ).fetchall()
    pts = [float(r["crash_point"]) for r in rows]
    total = len(pts)
    buckets = []
    for name, lo, hi in OUTCOME_BUCKETS:
        if name == "1x":
            cnt = sum(1 for p in pts if p >= lo and p < hi)
        else:
            cnt = sum(1 for p in pts if p >= lo and p < hi)
        pct = round(100.0 * cnt / total, 2) if total else 0.0
        buckets.append({"range": name, "count": cnt, "pct": pct})
    return jsonify({
        "total": total,
        "buckets": buckets,
    })


@app.route("/api/events")
def api_events():
    with lock:
        return jsonify(list(events))


@app.route("/api/stream")
def api_stream():
    def gen():
        while True:
            with lock:
                snapshot = {
                    "connected": connected,
                    "live_state": live_state,
                    "rounds": list(rounds)[-10:],
                    "ticks": list(ticks)[-50:],
                    "recent_events": list(events)[-20:],
                }
            yield "data: " + json.dumps(snapshot) + "\n\n"
            time.sleep(1)
    return Response(gen(), mimetype="text/event-stream")


def main():
    init_db()
    threading.Thread(target=browser_collector_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080, threaded=True)


if __name__ == "__main__":
    main()
