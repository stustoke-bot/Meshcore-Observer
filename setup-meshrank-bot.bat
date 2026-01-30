@echo off
title Setup Meshrank Bot
cd /d "%~dp0"

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed or not in PATH. Install from https://nodejs.org/
  pause
  exit /b 1
)

if not exist "meshrank-bot" mkdir meshrank-bot
if exist "meshcore-bot.js" (
  copy /y meshcore-bot.js meshrank-bot\bot.js >nul
  echo Created meshrank-bot\bot.js from meshcore-bot.js
) else (
  echo meshcore-bot.js not found. Will try to create bot.js in meshrank-bot...
)

cd meshrank-bot
if not exist "bot.js" (
  echo bot.js missing. Trying PowerShell create-bot.ps1...
  if exist "create-bot.ps1" (
    powershell -NoProfile -ExecutionPolicy Bypass -File create-bot.ps1
  ) else (
    echo Run download-bot.bat from meshrank-bot folder, or git pull from Meshcore-Observer then run setup again.
    pause
    exit /b 1
  )
)

if not exist "package.json" (
  call npm init -y
  call npm install @liamcottle/meshcore.js yargs
) else (
  call npm install
)
echo.
echo Setup done. Run the bot with:
echo   cd meshrank-bot
echo   node bot.js --port COM3
echo.
pause
