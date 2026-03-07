"""
Wing Launcher — 커스텀 URL 프로토콜 (wing://)
=============================================
wing://open-all     → 5개 계정 Chrome 동시 열기
wing://open/007-ez  → 개별 계정 열기
wing://register     → 프로토콜 등록

설치: Wing자동로그인.exe --register  (최초 1회)
이후: 대시보드 버튼 클릭 → wing:// → 자동 실행

자동로그인: wing_creds.json에 ID/PW 저장 후 "로컬에 저장" 버튼으로 동기화
"""
import sys
import os
import json
import subprocess
import time
import urllib.request
from pathlib import Path

if sys.stdout:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── 설정 ──────────────────────────────────────────────
ACCOUNTS = [
    "007-ez",
    "007-bm",
    "002-bm",
    "007-book",
    "big6ceo",
]
DEBUG_PORT_BASE = 9230

# 계정별 세션 폴더 (여기에 로그인 쿠키 저장됨)
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).parent

SESSION_DIR = BASE_DIR / "wing_sessions"
CREDS_PATH = BASE_DIR / "wing_creds.json"


def _load_creds() -> dict:
    """wing_creds.json에서 로그인 정보 로드"""
    if not CREDS_PATH.exists():
        return {}
    try:
        raw = json.loads(CREDS_PATH.read_text(encoding="utf-8"))
        return {k: {"id": str(v.get("id", "")), "pw": str(v.get("pw", ""))} for k, v in raw.items()}
    except Exception:
        return {}


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

    if getattr(sys, "frozen", False):
        cmd = f'"{sys.executable}" "%1"'
    else:
        # bat 래퍼 사용 (브라우저 호출 시 cwd/환경 문제 방지)
        bat = BASE_DIR / "wing_protocol.bat"
        cmd = f'"{bat.resolve()}" "%1"'

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


# ── Chrome 열기 + 자동 로그인 ───────────────────────────

def _do_auto_login(account_name: str, user_data: Path, port: int, wing_id: str, wing_pw: str):
    """Chrome을 디버깅 포트로 띄운 뒤 CDP(Chrome DevTools Protocol)로 자동 로그인.
    Playwright 불필요 — websocket-client만 사용하므로 EXE 번들링 가능."""
    chrome = _find_chrome()

    # Chrome 실행 (URL 없이 — CDP로 직접 네비게이션하므로 탭 중복 방지)
    subprocess.Popen([
        chrome,
        f"--user-data-dir={user_data}",
        f"--remote-debugging-port={port}",
        "--remote-allow-origins=*",
        "--no-first-run",
        "--no-default-browser-check",
    ])
    print(f"  [{account_name}] Chrome 시작 (port {port})...")
    time.sleep(3)

    try:
        import websocket

        # CDP 디스커버리: 열린 탭의 WebSocket URL 조회
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=5)
        tabs = json.loads(resp.read().decode())
        if not tabs:
            raise RuntimeError("Chrome 탭 없음")
        ws_url = tabs[0]["webSocketDebuggerUrl"]

        # WebSocket 연결
        ws = websocket.create_connection(ws_url, timeout=15)
        _msg_id = [0]

        def cdp(method: str, **params):
            _msg_id[0] += 1
            ws.send(json.dumps({"id": _msg_id[0], "method": method, "params": params}))
            while True:
                msg = json.loads(ws.recv())
                if msg.get("id") == _msg_id[0]:
                    return msg.get("result", {})

        # Wing 페이지로 이동
        cdp("Page.navigate", url="https://wing.coupang.com")
        time.sleep(4)

        # 로그인 폼 입력 (React input 대응: nativeInputValueSetter 사용)
        login_js = """
        (function(wingId, wingPw) {
            var id_el = document.querySelector('input[placeholder="아이디를 입력해주세요"]');
            var pw_el = document.querySelector('input[placeholder="비밀번호를 입력해주세요"]');
            if (!id_el || !pw_el) {
                if (!document.body || document.body.innerHTML.length < 100)
                    return 'PAGE_LOADING';
                return 'ALREADY_LOGGED_IN';
            }
            var setter = Object.getOwnPropertyDescriptor(
                HTMLInputElement.prototype, 'value').set;
            setter.call(id_el, wingId);
            id_el.dispatchEvent(new Event('input', {bubbles: true}));
            id_el.dispatchEvent(new Event('change', {bubbles: true}));
            setter.call(pw_el, wingPw);
            pw_el.dispatchEvent(new Event('input', {bubbles: true}));
            pw_el.dispatchEvent(new Event('change', {bubbles: true}));
            setTimeout(function() {
                pw_el.dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
                pw_el.dispatchEvent(new KeyboardEvent('keypress', {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
                pw_el.dispatchEvent(new KeyboardEvent('keyup', {key:'Enter',code:'Enter',keyCode:13,bubbles:true}));
                setTimeout(function() {
                    var btn = document.querySelector('input[type="submit"]') || document.querySelector('button[type="submit"]');
                    if (!btn) {
                        var btns = document.querySelectorAll('button');
                        for (var i = 0; i < btns.length; i++) {
                            if (btns[i].textContent.indexOf('로그인') >= 0) { btn = btns[i]; break; }
                        }
                    }
                    if (btn) btn.click();
                }, 300);
            }, 500);
            return 'LOGIN_SUBMITTED';
        })(%s, %s)
        """ % (json.dumps(wing_id), json.dumps(wing_pw))

        # 페이지 로딩 대기 + 로그인 시도 (최대 3회)
        for attempt in range(3):
            result = cdp("Runtime.evaluate", expression=login_js)
            status = result.get("result", {}).get("value", "")
            if status == "LOGIN_SUBMITTED":
                time.sleep(2)
                print(f"  [{account_name}] 로그인 완료")
                break
            elif status == "ALREADY_LOGGED_IN":
                print(f"  [{account_name}] 이미 로그인됨 (쿠키 유효)")
                break
            else:  # PAGE_LOADING
                time.sleep(2)

        ws.close()

    except Exception as e:
        print(f"  [{account_name}] 자동로그인 실패: {e}")
        # Chrome이 이미 열려있으므로 Wing 페이지만 열어줌 (수동 로그인 가능)
        subprocess.Popen([chrome, f"--user-data-dir={user_data}", "https://wing.coupang.com"])


def open_account(account_name: str, port: int = None):
    """계정별 독립 세션으로 Chrome 열기 (wing_creds.json 있으면 자동 로그인)"""
    user_data = SESSION_DIR / account_name
    user_data.mkdir(parents=True, exist_ok=True)

    creds = _load_creds()
    cred = creds.get(account_name) or creds.get(account_name.lower())
    wing_id = (cred or {}).get("id", "").strip()
    wing_pw = (cred or {}).get("pw", "").strip()

    if port is None:
        idx = ACCOUNTS.index(account_name) if account_name in ACCOUNTS else 0
        port = DEBUG_PORT_BASE + idx

    if wing_id and wing_pw:
        _do_auto_login(account_name, user_data, port, wing_id, wing_pw)
    else:
        chrome = _find_chrome()
        subprocess.Popen([
            chrome,
            f"--user-data-dir={user_data}",
            "--no-first-run",
            "--no-default-browser-check",
            "https://wing.coupang.com",
        ])
        print(f"  [{account_name}] 자격증명 없음 — 수동 로그인 필요")


def open_all():
    """5개 계정 순차 열기 + 자동 로그인"""
    print(f"🚀 {len(ACCOUNTS)}개 계정 자동 로그인 시작...")
    for name in ACCOUNTS:
        try:
            open_account(name)
        except Exception as e:
            print(f"  [{name}] 오류: {e}")
    print("✅ 완료")


# ── 메인 ──────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    # 프로토콜 URL 처리: wing://open-all, wing://open/007-ez, wing://register
    url = args[0] if args else ""

    if "--register" in args or "wing://register" in url:
        register_protocol()

    elif not url:
        # 직접 실행 시 → 등록 + 전체 열기
        register_protocol()
        open_all()

    elif "open-all" in url:
        open_all()

    elif url.startswith("wing://open/"):
        account = url.replace("wing://open/", "").strip("/")
        if account in ACCOUNTS:
            open_account(account)
        else:
            print(f"알 수 없는 계정: {account}")


if __name__ == "__main__":
    main()
