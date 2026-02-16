"""
WING 동기화 공통 베이스 모듈
============================
SQL 인젝션 방지, 공통 계정 조회/클라이언트 생성 로직

모든 sync_*.py 스크립트에서 상속하여 사용
"""
import os
import logging
from pathlib import Path
from typing import List, Dict, Optional, Callable

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# 프로젝트 루트
ROOT = Path(__file__).parent.parent.parent


def get_accounts(engine: Engine, account_name: Optional[str] = None) -> List[Dict]:
    """
    WING API 활성화된 계정 목록 조회 (SQL 인젝션 방지)

    Args:
        engine: SQLAlchemy 엔진
        account_name: 특정 계정명 (None=전체)

    Returns:
        계정 정보 딕셔너리 리스트
    """
    base_sql = """
        SELECT id, account_name, vendor_id, wing_access_key, wing_secret_key
        FROM accounts
        WHERE is_active = true AND wing_api_enabled = true
              AND vendor_id IS NOT NULL
              AND wing_access_key IS NOT NULL
              AND wing_secret_key IS NOT NULL
    """

    params = {}
    if account_name:
        base_sql += " AND account_name = :account_name"
        params["account_name"] = account_name

    base_sql += " ORDER BY account_name"

    with engine.connect() as conn:
        rows = conn.execute(text(base_sql), params).mappings().all()

    return [dict(r) for r in rows]


def create_wing_client(account: Dict, env_map: Optional[Dict[str, str]] = None):
    """
    계정 정보로 WING API 클라이언트 생성

    Args:
        account: 계정 정보 딕셔너리 (id, account_name, vendor_id, wing_access_key, wing_secret_key)
        env_map: 계정명 → 환경변수 접두사 매핑 (None이면 constants에서 가져옴)

    Returns:
        CoupangWingClient 인스턴스
    """
    # 순환 import 방지를 위해 지연 import
    from core.api.wing_client import CoupangWingClient
    from core.constants import WING_ACCOUNT_ENV_MAP

    if env_map is None:
        env_map = WING_ACCOUNT_ENV_MAP

    name = account["account_name"]
    env_prefix = env_map.get(name, "")

    vendor_id = account.get("vendor_id") or ""
    access_key = account.get("wing_access_key") or ""
    secret_key = account.get("wing_secret_key") or ""

    # DB 값이 없으면 환경변수에서 가져오기
    if not access_key and env_prefix:
        vendor_id = os.getenv(f"{env_prefix}_VENDOR_ID", vendor_id)
        access_key = os.getenv(f"{env_prefix}_ACCESS_KEY", "")
        secret_key = os.getenv(f"{env_prefix}_SECRET_KEY", "")

    return CoupangWingClient(vendor_id, access_key, secret_key)


class WingSyncBase:
    """
    WING 동기화 베이스 클래스

    모든 sync_*.py 클래스가 상속하여 공통 기능 사용
    - SQL 인젝션 방지된 계정 조회
    - 공통 클라이언트 생성
    - 트랜잭션 지원
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Args:
            db_path: DB 파일 경로 (None이면 기본 경로)
        """
        from core.database import get_engine_for_db
        self.db_path = db_path
        self.engine = get_engine_for_db(db_path)

    def get_accounts(self, account_name: Optional[str] = None) -> List[Dict]:
        """WING API 활성 계정 조회"""
        return get_accounts(self.engine, account_name)

    def create_client(self, account: Dict):
        """WING API 클라이언트 생성"""
        return create_wing_client(account)

    def execute_sql(self, sql: str, params: Optional[Dict] = None, commit: bool = True):
        """
        SQL 실행 (파라미터 바인딩)

        Args:
            sql: SQL 문 (파라미터는 :name 형식)
            params: 파라미터 딕셔너리
            commit: 자동 커밋 여부

        Returns:
            실행 결과 (SELECT인 경우 행 리스트)
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            if commit:
                conn.commit()
            if result.returns_rows:
                return result.mappings().all()
            return result.rowcount

    def execute_many(self, sql: str, params_list: List[Dict], commit: bool = True):
        """
        배치 SQL 실행 (INSERT/UPDATE 여러 건)

        Args:
            sql: SQL 문
            params_list: 파라미터 딕셔너리 리스트
            commit: 자동 커밋 여부

        Returns:
            처리된 행 수
        """
        if not params_list:
            return 0

        with self.engine.connect() as conn:
            for params in params_list:
                conn.execute(text(sql), params)
            if commit:
                conn.commit()
        return len(params_list)

    def close(self):
        """엔진 닫기"""
        self.engine.dispose()


def match_listing(conn, account_id: int, vendor_item_id=None,
                  coupang_product_id=None, product_name: str = None) -> Optional[int]:
    """
    3-level listing 매칭: vendor_item_id → coupang_product_id → product_name

    Args:
        conn: SQLAlchemy connection
        account_id: 계정 ID
        vendor_item_id: WING vendor item ID (가장 정확)
        coupang_product_id: 쿠팡 상품 ID (seller_product_id)
        product_name: 상품명 (최후 수단)

    Returns:
        listings.id 또는 None
    """
    # 1차: vendor_item_id 매칭 (가장 정확, BigInteger)
    if vendor_item_id:
        row = conn.execute(text(
            "SELECT id FROM listings WHERE account_id = :aid AND vendor_item_id = :vid LIMIT 1"
        ), {"aid": account_id, "vid": int(vendor_item_id)}).fetchone()
        if row:
            return row[0]
    # 2차: coupang_product_id 매칭 (BigInteger)
    if coupang_product_id:
        row = conn.execute(text(
            "SELECT id FROM listings WHERE account_id = :aid AND coupang_product_id = :pid LIMIT 1"
        ), {"aid": account_id, "pid": int(coupang_product_id)}).fetchone()
        if row:
            return row[0]
    # 3차: product_name 정확 매칭
    if product_name:
        row = conn.execute(text(
            "SELECT id FROM listings WHERE account_id = :aid AND product_name = :name LIMIT 1"
        ), {"aid": account_id, "name": product_name}).fetchone()
        if row:
            return row[0]
    return None
