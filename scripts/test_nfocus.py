"""
한진 N-Focus 로그인 + 페이지 이동 테스트
사용법: python scripts/test_nfocus.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from operations.hanjin_nfocus import HanjinNFocusClient, HanjinNFocusError

creds_path = Path(__file__).resolve().parents[1] / "hanjin_creds.json"
if creds_path.exists():
    creds = json.loads(creds_path.read_text(encoding="utf-8"))
else:
    creds = {
        "user_id": input("N-Focus 아이디: ").strip(),
        "password": input("N-Focus 비밀번호: ").strip(),
    }

print(f"\n=== N-Focus 테스트 (계정: {creds['user_id']}) ===\n")

with HanjinNFocusClient(
    user_id=creds["user_id"],
    password=creds["password"],
    headless=False,
) as client:
    print("[1/2] 로그인...")
    client.login()
    print("  -> 성공!")

    print("[2/2] 출력자료등록 페이지 이동...")
    page = client._page
    page.goto(client.URL_UPLOAD, wait_until="networkidle", timeout=30000)
    client._dismiss_dialogs()
    print(f"  -> URL: {page.url}")

    page.screenshot(path="data/nfocus_test_listup.png")
    print("  -> 스크린샷: data/nfocus_test_listup.png")

    print("\n=== 테스트 완료 ===")
    print("전체 워크플로우 테스트는 Dashboard STEP 3에서 실행하세요.")
    input("\nEnter → 브라우저 닫기...")
