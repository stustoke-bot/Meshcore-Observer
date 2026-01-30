@echo off
title Meshrank Bot (local + meshrank.net)
set PORT=COM3
if not "%~1"=="" set PORT=%~1

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed or not in PATH.
  echo Install from https://nodejs.org/ then run this again.
  pause
  exit /b 1
)

if not exist "node_modules" (
  echo Installing dependencies (first run)...
  call npm install
  if errorlevel 1 (
    echo npm install failed.
    pause
    exit /b 1
  )
)

echo Starting bot: port=%PORT%, server=https://meshrank.net
echo.
node bot.js --port %PORT%
pause
