@echo off
echo Starting Local File Transfer Server...
echo.

REM Check if Node.js is installed
where node >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Node.js is not installed. Please install Node.js first.
    echo Visit: https://nodejs.org/
    pause
    exit /b 1
)

REM Check if dependencies are installed
if not exist "node_modules" (
    echo Installing dependencies...
    call npm install
    echo.
)

REM Start the server
echo Starting server...
echo.
call npm start
pause
