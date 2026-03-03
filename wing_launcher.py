"""
Wing Launcher — 커스텀 URL 프로토콜 (wing://)
=============================================
wing://open-all     → 5개 계정 Chrome 동시 열기
wing://open/007-ez  → 개별 계정 열기
wing://register     → 프로토콜 등록

설치: Wing자동로그인.exe --register  (최초 1회)
이후: 대시보드 버튼 클릭 → wing:// → 자동 실행
"""
import sys
import os
import subprocess
import time
import threading
from pathlib import Path

# ── 설정 ──────────────────────────────────────────────
ACCOUNTS = [
    "007-ez",
    "007-bm",
    "002-bm",
    "007-book",
    "big6ceo",
]

# 계정별 세션 폴더 (여기에 로그인 쿠키 저장됨)
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

SESSION_DIR = BASE_DIR / "wing_sessions"

# Chrome 경로 자동 탐색
def _find_chrome():
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return "chrome"


# ── 프로토콜 등록 ──────────────────────────────────────

def register_protocol():
    """wing:// 프로토콜을 Windows 레지스트리에 등록"""
    import winreg

    exe = sys.executable if getattr(sys, "frozen", False) else f'python "{Path(__file__).resolve()}"'
    cmd = f'"{exe}" "%1"'

    try:
        key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\wing")
        winreg.SetValue(key, "", winreg.REG_SZ, "URL:Wing Protocol")
        winreg.SetValueEx(key, "URL Protocol", 0, winreg.REG_SZ, "")
        winreg.CloseKey(key)

        key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, r"Software\Classes\wing\shell\open\command")
        winreg.SetValue(key, "", winreg.REG_SZ, cmd)
        winreg.CloseKey(key)

        print("✅ wing:// 프로토콜 등록 완료!")
        print(f"   실행 명령: {cmd}")
        input("엔터를 누르면 닫힙니다...")
    except Exception as e:
        print(f"❌ 등록 실패: {e}")
        input("엔터를 누르면 닫힙니다...")


# ── Chrome 열기 ────────────────────────────────────────

def open_account(account_name: str):
    """계정별 독립 세션으로 Chrome 열기"""
    chrome = _find_chrome()
    user_data = SESSION_DIR / account_name
    user_data.mkdir(parents=True, exist_ok=True)

    subprocess.Popen([
        chrome,
        f"--user-data-dir={user_data}",
        "--no-first-run",
        "--no-default-browser-check",
        "--restore-last-session",
        "https://wing.coupang.com",
    ])


def open_all():
    """5개 계정 동시에 열기 (0.5초 간격)"""
    for name in ACCOUNTS:
        open_account(name)
        time.sleep(0.5)


# ── 메인 ──────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    # 프로토콜 URL 처리: wing://open-all, wing://open/007-ez, wing://register
    url = args[0] if args else ""

    if "--register" in args or "wing://register" in url:
        register_protocol()

    elif not url or url == "wing://open-all":
        open_all()

    elif url.startswith("wing://open/"):
        account = url.replace("wing://open/", "").strip("/")
        if account in ACCOUNTS:
            open_account(account)
        else:
            print(f"알 수 없는 계정: {account}")

    else:
        # 직접 실행 시 → 등록 + 전체 열기
        register_protocol()
        open_all()


if __name__ == "__main__":
    main()
