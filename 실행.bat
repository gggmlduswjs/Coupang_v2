@echo off
chcp 65001 > nul
title 쿠팡 대시보드

cd /d "%~dp0"

:: Python 확인
python --version > nul 2>&1
if errorlevel 1 (
    echo [오류] Python이 설치되지 않았습니다.
    echo https://www.python.org/downloads/ 에서 설치 후 다시 실행하세요.
    pause
    exit /b
)

:: 패키지 설치 (처음 한 번 or 업데이트)
echo 패키지 확인 중...
pip install -r requirements-local.txt -q

:: Playwright Chrome 설치 확인
python -c "from playwright.sync_api import sync_playwright; p=sync_playwright().start(); p.stop()" > nul 2>&1
if errorlevel 1 (
    echo Playwright Chrome 설치 중... (최초 1회)
    playwright install chrome
)

:: .env 확인
if not exist .env (
    echo [경고] .env 파일이 없습니다. DB 연결이 안 될 수 있어요.
)

:: 대시보드 실행
echo.
echo ✅ 대시보드 시작합니다...
echo    브라우저에서 http://localhost:8501 로 접속하세요
echo.
start http://localhost:8501
streamlit run dashboard/app.py --server.port 8501 --server.headless false

pause
