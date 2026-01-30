@echo off
title Download bot.js
cd /d "%~dp0"

echo Downloading bot.js from GitHub...
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/stustoke-bot/Meshcore-Observer/master/meshrank-bot/bot.js' -OutFile 'bot.js' -UseBasicParsing; Write-Host 'Saved bot.js'; exit 0 } catch { Write-Host 'Download failed. Try: git pull in Meshcore-Observer then run create-bot.ps1 here'; exit 1 }"

if exist bot.js (
  echo Done. Run: node bot.js --port COM5
) else (
  echo.
  echo If download failed, run this in PowerShell from this folder:
  echo   powershell -ExecutionPolicy Bypass -File create-bot.ps1
  echo Or pull latest and copy: cd ..  then  git pull  then  copy meshcore-bot.js meshrank-bot\bot.js
)
pause
