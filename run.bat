@echo off
REM Crash-On Analysis - launcher
REM Starts the Flask + Playwright crash-game recorder on http://localhost:8080

setlocal
set PY="C:\Users\PM_User\AppData\Local\Programs\Python\Python312\python.exe"
set DIR=%~dp0

REM Make sure python is on PATH for child processes
set PATH=%PY%;%PY%;%PATH%

cd /d "%DIR%"
echo Starting Crash-On Analysis backend...
echo Open http://localhost:8080 in your browser.
echo Press Ctrl+C to stop.
%PY% "%DIR%backend.py"
endlocal
