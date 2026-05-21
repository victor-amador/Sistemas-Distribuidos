@echo off
setlocal

set "PORT=%~1"
if "%PORT%"=="" set "PORT=5090"

set "TASK_USERS=%~2"
if "%TASK_USERS%"=="" set "TASK_USERS=Michel"

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

set "MASTER_PORT=%PORT%"
set "TASK_USERS=%TASK_USERS%"
%PYTHON_CMD% "%~dp0master.py"
