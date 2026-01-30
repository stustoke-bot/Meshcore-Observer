@echo off
title Meshrank Bot - Download and Install
set REPO=https://github.com/stustoke-bot/Meshcore-Observer.git
set FOLDER=Meshcore-Observer
set BOT_DIR=%FOLDER%\meshrank-bot

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed or not in PATH.
  echo Install from https://nodejs.org/ (LTS) then run this again.
  pause
  exit /b 1
)

where git >nul 2>nul
if errorlevel 1 (
  echo Git is not installed or not in PATH.
  echo Install from https://git-scm.com/download/win then run this again.
  pause
  exit /b 1
)

if not exist "%FOLDER%" (
  echo Cloning Meshcore-Observer...
  git clone %REPO% %FOLDER%
  if errorlevel 1 (
    echo Clone failed.
    pause
    exit /b 1
  )
) else (
  echo Folder %FOLDER% already exists. Pulling latest...
  cd %FOLDER%
  git pull
  cd ..
)

if not exist "%BOT_DIR%\bot.js" (
  echo meshrank-bot not found in %BOT_DIR%.
  pause
  exit /b 1
)

echo.
echo Installing meshrank-bot dependencies...
cd %BOT_DIR%
call npm install
if errorlevel 1 (
  echo npm install failed.
  cd ..
  pause
  exit /b 1
)

echo.
echo Done. To run the bot, use one of these:
echo   run.bat           (uses COM3)
echo   run.bat COM4       (use your COM port)
echo   node bot.js --port COM3
echo.
set /p PORT="Enter your COM port (e.g. COM3) or press Enter to use COM3: "
if "%PORT%"=="" set PORT=COM3
echo Starting bot on %PORT%...
node bot.js --port %PORT%
pause
