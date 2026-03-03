@echo off
chcp 65001 > nul
title Wing Launcher EXE 빌드

cd /d "%~dp0"

echo PyInstaller 설치 중...
pip install pyinstaller flask flask-cors -q

echo.
echo EXE 빌드 중... (1~2분 소요)
pyinstaller --onefile --noconsole --name "Wing자동로그인" wing_launcher.py

echo.
if exist "dist\Wing자동로그인.exe" (
    echo ✅ 빌드 완료!
    echo 파일 위치: %~dp0dist\Wing자동로그인.exe
    echo.
    echo 이 EXE 파일을 엄마 PC에 복사하고 실행하면 됩니다.
    explorer dist
) else (
    echo ❌ 빌드 실패
)

pause
