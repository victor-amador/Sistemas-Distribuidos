@echo off
setlocal

set "HOST=%~1"
if "%HOST%"=="" set "HOST=192.168.15.50"

set "PORT=%~2"
if "%PORT%"=="" set "PORT=5090"

set "MODE=%~3"
if "%MODE%"=="" set "MODE=TASKS"

set "HEARTBEAT_INTERVAL=1"
set "RECONNECT_DELAY=10"
set "FORCE_STATUS=OK"

where py >nul 2>nul
if %errorlevel%==0 (
    set "PYTHON_CMD=py"
) else (
    where python >nul 2>nul
    if %errorlevel%==0 (
        set "PYTHON_CMD=python"
    ) else (
        echo Python nao encontrado no PATH.
        echo Instale o Python 3 e marque a opcao Add Python to PATH.
        exit /b 1
    )
)

set "MASTER_HOST=%HOST%"
set "MASTER_PORT=%PORT%"
set "WORKER_MODE=%MODE%"
set "HEARTBEAT_INTERVAL=%HEARTBEAT_INTERVAL%"
set "RECONNECT_DELAY=%RECONNECT_DELAY%"
set "FORCE_STATUS=%FORCE_STATUS%"

%PYTHON_CMD% "%~dp0worker1.py"
