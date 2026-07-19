# Crash-On Analysis — Project Memory & Build Log

> This file is the canonical memory for the Crash-On Analysis project. It
> captures the goal, architecture, what was built, key decisions, and how to
> run/continue the work. Use it to resume development after context loss.

## Goal
Analyse the betting site **gameonworld.ai** Crash game (Game #42) by recording
its real-time WebSocket data. Build a Python backend + simple HTML dashboard
that mimics a browser, connects to the game's Socket.IO WebSocket, records all
crash events, persists them to a database, and shows live analytics.

## Target endpoints (discovered by reverse-engineering the site)
- Game page: `https://gameonworld.ai/game/42`
- WebSocket (Socket.IO v4 / Engine.IO v4):
  `wss://crash.gameonworld.ai/socket.io/?EIO=4&transport=websocket`
- Site's own client config points the live socket at
  `wss://backend-api.gameonworld.ai` (same server, different Cloudflare zone).

### Socket.IO protocol (spoken manually)
1. Engine.IO handshake: server sends `0{...open with sid, pingInterval, pingTimeout...}`
2. Client sends `40` (join default namespace)
3. Server sends `40{...}`
4. Client sends `42["joinMarket"]` (subscribe to the crash market)
5. Server emits events:
   - `crashState` — full round state (phase: betting/running/crashed/cooldown,
     roundId, multiplier, serverSeedHash, publicSeed, bets[], recent[],
     availableLiquidity)
   - `crashTick` — `{roundId, multiplier}` streaming ticks during a round
   - `roundCrashed` — `{roundId, crashPoint, serverSeed, serverSeedHash,
     publicSeed, houseEdge, maxMultiplier, bets[], houseProfit}`
   - `liquidityUpdate` — `{available}`
   - `playerCashedOut` — `{betId, visitorId, displayName, cashoutMultiplier,
     payout, roundId}`
6. Keepalive: server `2` (ping) → client `3` (pong)

## KEY DECISION: Cloudflare bot protection
A plain WebSocket client (websocket-client / aiohttp) gets **HTTP 400** on the
handshake — no `cf_clearance` cookie + no browser TLS/JA3 fingerprint.
- `crash.gameonworld.ai` → 400 for any non-browser client.
- `backend-api.gameonworld.ai` → 400 too, unless a valid `cf_clearance`
  cookie is supplied.
**Solution:** drive a real headless Chromium (Playwright). It loads the game
page (obtains `cf_clearance`), then opens the Socket.IO WebSocket *from inside
the page context* where the cookie + fingerprint are valid. A JS driver in the
page speaks the Socket.IO framing and forwards decoded events to Python via
`page.expose_function`.

> Note: Playwright Python must be **1.56.0** to match the already-downloaded
> browser build (`chromium-1194`). pip's default 1.61.0 wants build 1228 and
> fails. Python is installed at
> `C:\Users\PM_User\AppData\Local\Programs\Python\Python312\python.exe`.

## Architecture
```
browser_collector_loop()  (Playwright headless Chromium)
   └─ loads game page (cf_clearance) → opens in-page Socket.IO WS →
      forwards events via window.__onCrashEvent → on_event()
on_event() → handle_event() → compute_metrics() (merge across events) →
      store_round() → rounds deque + persist_round() → SQLite (crashon.db)
Flask app serves:
   /                       dashboard (templates/index.html, no-cache)
   /api/status             connection + counts
   /api/rounds             in-memory recent rounds
   /api/db/rounds?limit=N  last N rounds from DB (0 = all)
   /api/db/count           total rows
   /api/db/aggregates      cumulative + per-round series + ratios inputs
   /api/db/predictions     next-crash guess + back-test accuracy
   /api/db/outcomes        all-time crash-point probability buckets
   /api/events             raw recorded events
   /api/stream             Server-Sent Events (live snapshot)
```

## Files
- `backend.py` — Flask + Playwright recorder, SQLite, all API endpoints.
- `templates/index.html` — dashboard (Chart.js via CDN), live polling.
- `crashon.db` — SQLite DB (table `rounds`).
- `run.bat` — launcher (sets PYTHON path, runs backend.py).
- `backend.err` / `backend.log` — runtime logs.

## Dashboard cards (current)
1. **Live Round** — current multiplier + phase + round id (SSE).
2. **Next Crash Prediction** — guess (median+mean of last 30 crashes), method,
   accuracy stats (guesses, avg error, within 1x/2x).
3. **Prediction vs Actual Crashes** — line chart (predicted vs actual).
4. **Outcome Heatmap** — all-time probability per crash-point range
   (1x / 1–1.2 / 1.2–1.5 / 1.5–2 / 2–5 / 5–10 / 10+), heat cells + bar chart.
5. **House vs Players — Per Round** — bar chart bets/won/house-profit per round
   (computed locally; server liquidity NOT trusted).
6. **Bet·Win·House-Profit Ratios** — payout %, real house edge, profit/round,
   player win rate (plain live stats).
7. **Recorded Rounds** — table (last N via dropdown: 20/50/100/500/All) with
   Round ID, Crash, Total Bet, # Bets, Cashouts, House Edge, Liquidity,
   Won by Players, **House Profit = Total Bet − Won by Players**, Time.
8. **Recent Multiplier Ticks** — live tick feed.
9. **Raw Event Log** — raw events.

## Key implementation notes
- `compute_metrics()` MERGES data across `crashState`(crashed) and
  `roundCrashed` per roundId (one has liquidity, other has houseProfit/seed).
  `crashed_at` derived from roundId timestamp `CR-<epoch ms>` when missing.
- **House Profit is computed locally** as `total_bet_value − amount_won`, NOT
  taken from the server's `houseProfit`/`availableLiquidity` (untrusted).
- Liquidity displayed at FULL precision via `fmtLiq()` (e.g. `469.2499999999987`)
  — server sends long floats.
- **Bug fixed:** `on_event` was missing `global connected`, so the SSE
  `connected` flag stayed False while data still flowed ("offline" bug).
- `request` must be imported from flask for the `?limit=` param.
- `/` sends `no-cache` headers so HTML updates aren't served stale.
- Prediction heuristic: for each round i (after a 30-round window), guess =
  (median + mean) of the preceding 30 crashes; back-tested for accuracy.

## How to run
```
run.bat
```
Then open `http://localhost:8080`. The browser-based collector auto-reconnects
and keeps appending to `crashon.db`.

## To continue development
- Restart backend: stop python process, run `run.bat` (or
  `python backend.py`).
- CSV/export, auth, multiple games, better prediction model (e.g. EMA,
  Poisson, or seed-based since `serverSeed`/`publicSeed` are exposed), and a
  production WSGI server are natural next steps.
- The crash is verifiably fair-ish: `serverSeed` + `publicSeed` are published,
  so crash points are reproducible — a future enhancement is to verify/audit
  the crash math client-side.
