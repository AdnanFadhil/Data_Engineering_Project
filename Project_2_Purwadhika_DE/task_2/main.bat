@echo off
title Project Scheduler Menu
color 0A
setlocal enabledelayedexpansion

net session >nul 2>&1
if %errorlevel% neq 0 (
    echo =========================================
    echo WARNING: This script must be run as Administrator!
    echo Right-click the .bat file and choose "Run as administrator".
    echo =========================================
    pause
    exit /b
)

set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:menu
cls
echo =========================================
echo       Project Scheduler Menu
echo =========================================
echo 1. Setup Task Scheduler (SYSTEMP)
echo 2. Check Task Scheduler (SYSTEMP)
echo 3. Delete Task Scheduler (SYSTEMP)
echo 4. Run main_extract.py manually
echo 5. Run main_analytics.py manually
echo 6. Exit
echo =========================================
set /p choice="Choose an option [1-6]: "

if "%choice%"=="1" goto setup_systemp
if "%choice%"=="2" goto check_systemp
if "%choice%"=="3" goto delete_systemp
if "%choice%"=="4" goto run_extract
if "%choice%"=="5" goto run_analytics
if "%choice%"=="6" exit

goto menu

:setup_systemp
echo Creating SYSTEMP Tasks...

:: Task 1: main_extract.py -> monthly, tanggal 1 jam 00:30
schtasks /create /tn "MyProject_Extract" /tr "\"%~dp0run_main_extract.bat >> %LOG_DIR%\main_extract.log 2>&1\"" /sc monthly /mo 1 /d 1 /st 00:30 /ru SYSTEM /f

:: Task 2: main_analytics.py -> daily, jam 01:00
schtasks /create /tn "MyProject_Analytics" /tr "\"%~dp0run_main_analytics.bat >> %LOG_DIR%\main_analytics.log 2>&1\"" /sc daily /st 01:00 /ru SYSTEM /f

echo SYSTEMP Tasks created.
pause
goto menu

:check_systemp
echo Listing SYSTEMP Tasks...
schtasks /query /tn "MyProject_Extract"
schtasks /query /tn "MyProject_Analytics"
pause
goto menu

:delete_systemp
echo Deleting SYSTEMP Tasks...
schtasks /delete /tn "MyProject_Extract" /f
schtasks /delete /tn "MyProject_Analytics" /f
pause
goto menu

:run_extract
echo Running main_extract.py manually...
chcp 65001
python "%~dp0main_extract.py" >> "%LOG_DIR%\main_extract_manual.log" 2>&1
echo Done. Log saved to %LOG_DIR%\main_extract_manual.log
pause
goto menu

:run_analytics
echo Running main_analytics.py manually...
python "%~dp0main_analytics.py" >> "%LOG_DIR%\main_analytics_manual.log" 2>&1
echo Done. Log saved to %LOG_DIR%\main_analytics_manual.log
pause
goto menu
