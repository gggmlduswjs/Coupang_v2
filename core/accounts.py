"""계정 설정 및 WING API 클라이언트 팩토리.

ACCOUNTS dict와 get_wing_client()를 단일 소스로 관리.
"""

import os

from core.api.wing_client import CoupangWingClient


ACCOUNTS: dict[str, dict] = {
    "007-ez": {
        "vendor_id": os.getenv("COUPANG_007EZ_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_007EZ_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_007EZ_SECRET_KEY", ""),
    },
    "002-bm": {
        "vendor_id": os.getenv("COUPANG_002BM_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_002BM_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_002BM_SECRET_KEY", ""),
    },
    "007-bm": {
        "vendor_id": os.getenv("COUPANG_007BM_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_007BM_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_007BM_SECRET_KEY", ""),
    },
    "007-book": {
        "vendor_id": os.getenv("COUPANG_007BOOK_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_007BOOK_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_007BOOK_SECRET_KEY", ""),
    },
    "big6ceo": {
        "vendor_id": os.getenv("COUPANG_BIG6CEO_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_BIG6CEO_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_BIG6CEO_SECRET_KEY", ""),
    },
}


def get_wing_client(account: str) -> CoupangWingClient:
    """계정명으로 API 클라이언트 생성."""
    cfg = ACCOUNTS.get(account)
    if not cfg:
        raise ValueError(f"알 수 없는 계정: {account} (등록된 계정: {', '.join(ACCOUNTS.keys())})")
    if not cfg["access_key"]:
        raise ValueError(f"{account}: API 키 미설정 (.env 확인)")
    return CoupangWingClient(
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        vendor_id=cfg["vendor_id"],
    )
