"""
Wing Launcher — 로컬 데몬
==========================
Cloud 대시보드 버튼 → 이 서버로 신호 → Chrome 5개 자동 열기

실행: python wing_launcher.py
자동시작: wing_launcher_startup.bat 을 시작프로그램에 등록
"""
import json
import subprocess
import sys
from pathlib import Path
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)   # Cloud(HTTPS)에서 localhost 호출 허용

PORT = 8888

# Chrome 경로 자동 감지
_CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Users\%s\AppData\Local\Google\Chrome\Application\chrome.exe" % __import__('os').getenv('USERNAME', ''),
]
CHROME_PATH = next((p for p in _CHROME_CANDIDATES if Path(p).exists()), "chrome")

# 계정 → Chrome 프로필 매핑
# Chrome 프로필은 크롬 우측상단 계정 아이콘 → 프로필 관리에서 확인
# "Profile 1", "Profile 2" ... 또는 "Default"
CONFIG_FILE = Path(__file__).parent / "wing_launcher_config.json"

DEFAULT_CONFIG = {
    "chrome_path": CHROME_PATH,
    "profiles": {
        "007-ez":   "Profile 1",
        "007-bm":   "Profile 2",
        "002-bm":   "Profile 3",
        "007-book": "Profile 4",
        "big6ceo":  "Profile 5",
    }
}


def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return DEFAULT_CONFIG


def save_config(cfg: dict):
    CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


# ─── API 엔드포인트 ───────────────────────────────────

@app.route("/ping")
def ping():
    """대시보드에서 실행 여부 확인용"""
    return jsonify({"status": "ok", "message": "Wing Launcher 실행 중"})


@app.route("/open-all")
def open_all():
    """5개 계정 전부 열기"""
    cfg = load_config()
    chrome = cfg.get("chrome_path", CHROME_PATH)
    profiles = cfg.get("profiles", {})
    opened, failed = [], []

    for name, profile in profiles.items():
        try:
            subprocess.Popen([chrome, f"--profile-directory={profile}", "https://wing.coupang.com"])
            opened.append(name)
        except Exception as e:
            failed.append(f"{name}: {e}")

    return jsonify({"status": "ok", "opened": opened, "failed": failed})


@app.route("/open/<account>")
def open_one(account):
    """개별 계정 열기"""
    cfg = load_config()
    chrome = cfg.get("chrome_path", CHROME_PATH)
    profile = cfg.get("profiles", {}).get(account)
    if not profile:
        return jsonify({"error": f"{account} 프로필 미설정"}), 404
    try:
        subprocess.Popen([chrome, f"--profile-directory={profile}", "https://wing.coupang.com"])
        return jsonify({"status": "ok", "account": account})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/config")
def get_config():
    return jsonify(load_config())


@app.route("/config", methods=["POST"])
def set_config():
    from flask import request
    cfg = request.get_json()
    save_config(cfg)
    return jsonify({"status": "ok"})


# ─── 실행 ────────────────────────────────────────────

if __name__ == "__main__":
    # 최초 실행 시 기본 설정 파일 생성
    if not CONFIG_FILE.exists():
        save_config(DEFAULT_CONFIG)
        print(f"설정 파일 생성: {CONFIG_FILE}")

    print("=" * 55)
    print("  Wing Launcher 시작!")
    print(f"  http://localhost:{PORT} 에서 실행 중")
    print(f"  Chrome: {CHROME_PATH}")
    print("  이 창을 닫으면 대시보드에서 Wing을 열 수 없어요")
    print("=" * 55)

    app.run(host="127.0.0.1", port=PORT, debug=False, use_reloader=False)
