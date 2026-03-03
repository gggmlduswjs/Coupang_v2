@echo off
chcp 65001 > nul
title Wing Launcher

cd /d "%~dp0"

python --version > nul 2>&1
if errorlevel 1 (
    echo [오류] Python이 없습니다.
    pause
    exit /b
)

pip install flask flask-cors -q

echo Wing Launcher 시작 중...
python wing_launcher.py
pause
