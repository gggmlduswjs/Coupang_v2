"""
발주서(주문) 동기화 스크립트
============================
WING Ordersheet API → orders 테이블

사용법:
    python scripts/sync_orders.py              # 기본 7일
    python scripts/sync_orders.py --days 30    # 최근 30일
    python scripts/sync_orders.py --account 007-book  # 특정 계정만
    python scripts/sync_orders.py --status ACCEPT      # 특정 상태만
"""
import os
import sys
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Callable

from sqlalchemy import text

# 프로젝트 루트
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from core.database import get_engine_for_db

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.services.sync_base import get_accounts, create_wing_client, match_listing

from dashboard.services.order_helpers import (
    parse_dt,
    extract_price,
    extract_order_items,
    build_upsert_params,
    UPSERT_ORDER_SQL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 조회 대상 상태 목록
ORDER_STATUSES = ["ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY", "NONE_TRACKING"]


class OrderSync:
    """발주서(주문) 동기화 엔진"""

    CREATE_INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS ix_order_account_date ON orders(account_id, ordered_at)",
        "CREATE INDEX IF NOT EXISTS ix_order_account_status ON orders(account_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_order_order_id ON orders(order_id)",
    ]

    def __init__(self, db_path: str = None):
        self.engine = get_engine_for_db(db_path)
        self._ensure_table()

    def _ensure_table(self):
        """인덱스 확인 + vendor_item_id NULL 마이그레이션"""
        with self.engine.connect() as conn:
            for idx_sql in self.CREATE_INDEXES_SQL:
                try:
                    conn.execute(text(idx_sql))
                except Exception:
                    pass
            # vendor_item_id NULL → 0 마이그레이션 (UNIQUE 키 NULL 방지)
            try:
                fixed = conn.execute(text(
                    "UPDATE orders SET vendor_item_id = 0 WHERE vendor_item_id IS NULL"
                )).rowcount
                if fixed:
                    logger.info(f"vendor_item_id NULL → 0 마이그레이션: {fixed}건")
            except Exception:
                pass
            conn.commit()
        logger.info("orders 테이블 확인 완료")

    def _get_accounts(self, account_name: str = None) -> list:
        """WING API 활성 계정 목록"""
        return get_accounts(self.engine, account_name)

    def _create_client(self, account: dict) -> CoupangWingClient:
        """계정 정보로 WING 클라이언트 생성"""
        return create_wing_client(account)

    def sync_account(self, account: dict, date_from: date, date_to: date,
                     statuses: List[str] = None,
                     progress_callback: Callable = None) -> dict:
        """계정 1개의 주문 동기화 (API 병렬 + DB 배치)"""
        account_id = account["id"]
        account_name = account["account_name"]
        client = self._create_client(account)

        if statuses is None:
            statuses = ORDER_STATUSES

        logger.info(f"[{account_name}] 주문 동기화 시작: {date_from} ~ {date_to}")

        windows = self._split_date_range(date_from, date_to)

        # 1) API 병렬 호출: 모든 (윈도우, 상태) 조합을 동시에
        all_results = []  # [(status, ordersheets), ...]

        def _fetch(w_from, w_to, status):
            try:
                return status, client.get_all_ordersheets(w_from, w_to, status=status)
            except CoupangWingError as e:
                logger.error(f"  [{account_name}] API 오류 ({status}): {e}")
                return status, []

        with ThreadPoolExecutor(max_workers=min(len(statuses) * len(windows), 15)) as pool:
            futures = []
            for w_from, w_to in windows:
                for status in statuses:
                    futures.append(pool.submit(_fetch, w_from, w_to, status))
            for f in as_completed(futures):
                all_results.append(f.result())

        # 2) 파라미터 일괄 생성
        all_params = []
        for status, ordersheets in all_results:
            if not ordersheets:
                continue
            for os_data in ordersheets:
                for item in extract_order_items(os_data):
                    params = build_upsert_params(account_id, status, os_data, item)
                    if params:
                        all_params.append(params)

        total_fetched = len(all_params)
        total_upserted = 0
        total_matched = 0

        # 3) DB 배치 저장 (트랜잭션 — 개별 오류 시 해당 행만 스킵)
        if all_params:
            try:
                with self.engine.begin() as conn:
                    for params in all_params:
                        try:
                            conn.execute(text(UPSERT_ORDER_SQL), params)
                            total_upserted += 1
                        except (SQLAlchemyError, ValueError, TypeError) as e:
                            logger.warning(f"  DB 오류 (스킵): {e}")
            except SQLAlchemyError as e:
                logger.error(f"  [{account_name}] DB 배치 오류: {e}")

        # 4) 활성 상태 정리: API에서 조회된 활성 주문 외의 DB 활성 주문 → FINAL_DELIVERY
        #    날짜 범위가 충분히 넓을 때만 (60일+) 정리 수행 — quick sync에서 오작동 방지
        active_statuses = {"ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING"}
        date_span = (date.fromisoformat(str(date_to)) - date.fromisoformat(str(date_from))).days if isinstance(date_from, str) else (date_to - date_from).days

        # API 부분 실패 감지: 모든 활성 상태 조회가 성공했을 때만 정리 실행
        fetched_active_statuses = {status for status, ordersheets in all_results if status in active_statuses}
        all_active_fetched = active_statuses.issubset(fetched_active_statuses)

        if date_span >= 60 and all_active_fetched and (statuses is None or active_statuses.issubset(set(statuses or []))):
            # API에서 가져온 활성 주문의 (shipment_box_id, vendor_item_id) 집합
            api_active_keys = set()
            for status, ordersheets in all_results:
                if status not in active_statuses:
                    continue
                for os_data in ordersheets:
                    sb_id = os_data.get("shipmentBoxId")
                    if not sb_id:
                        continue
                    for item in extract_order_items(os_data):
                        v_id = item.get("vendorItemId") or os_data.get("vendorItemId") or 0
                        api_active_keys.add((int(sb_id), int(v_id)))

            if api_active_keys or total_fetched > 0:
                try:
                    with self.engine.connect() as conn:
                        # DB에서 현재 활성 상태인 주문 조회
                        db_active = conn.execute(text(
                            "SELECT shipment_box_id, vendor_item_id FROM orders "
                            "WHERE account_id = :aid AND status IN ('ACCEPT','INSTRUCT','DEPARTURE','DELIVERING')"
                        ), {"aid": account_id}).fetchall()

                        stale_keys = [(r[0], r[1]) for r in db_active if (r[0], r[1]) not in api_active_keys]
                        if stale_keys:
                            for sb_id, v_id in stale_keys:
                                conn.execute(text(
                                    "UPDATE orders SET status = 'FINAL_DELIVERY', updated_at = :now "
                                    "WHERE account_id = :aid AND shipment_box_id = :sb AND vendor_item_id = :vi "
                                    "AND status IN ('ACCEPT','INSTRUCT','DEPARTURE','DELIVERING')"
                                ), {"aid": account_id, "sb": sb_id, "vi": v_id, "now": datetime.now().isoformat()})
                            conn.commit()
                            logger.info(f"  [{account_name}] 상태 정리: {len(stale_keys)}건 → FINAL_DELIVERY")
                except Exception as e:
                    logger.warning(f"  [{account_name}] 상태 정리 실패: {e}")

        result = {
            "account": account_name,
            "fetched": total_fetched,
            "upserted": total_upserted,
            "matched": total_matched,
        }
        logger.info(f"[{account_name}] 완료: 조회 {total_fetched}건, 저장 {total_upserted}건")
        return result

    @staticmethod
    def _split_date_range(date_from: date, date_to: date, window_days: int = 31) -> list:
        """날짜 범위를 window_days 단위 윈도우로 분할"""
        windows = []
        current = date_from
        while current <= date_to:
            end = min(current + timedelta(days=window_days - 1), date_to)
            windows.append((current.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
            current = end + timedelta(days=1)
        return windows

    def sync_all(self, days: int = 7, account_name: str = None,
                 statuses: List[str] = None,
                 progress_callback: Callable = None) -> List[dict]:
        """
        전체 계정 주문 동기화

        Args:
            days: 동기화 기간 (일, 기본 7)
            account_name: 특정 계정만 (None=전체)
            statuses: 조회할 상태 리스트 (None=전체)
            progress_callback: 진행 콜백 (current, total, message)

        Returns:
            계정별 결과 리스트
        """
        accounts = self._get_accounts(account_name)
        if not accounts:
            logger.warning("WING API 활성화된 계정이 없습니다.")
            return []

        date_to = date.today()
        date_from = date_to - timedelta(days=days)

        logger.info(f"주문 동기화: {len(accounts)}개 계정, {date_from} ~ {date_to} (병렬)")

        # 병렬 실행: 모든 계정 동시 처리
        results = []
        with ThreadPoolExecutor(max_workers=len(accounts)) as pool:
            futures = {
                pool.submit(self.sync_account, account, date_from, date_to, statuses, progress_callback): account
                for account in accounts
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    account = futures[future]
                    logger.error(f"[{account['account_name']}] 병렬 동기화 오류: {e}")
                    results.append({"account": account["account_name"], "fetched": 0, "upserted": 0, "matched": 0})

        if progress_callback:
            progress_callback(len(accounts), len(accounts), "동기화 완료!")

        # 결과 요약
        total_f = sum(r["fetched"] for r in results)
        total_u = sum(r["upserted"] for r in results)
        total_m = sum(r["matched"] for r in results)
        logger.info(f"전체 완료: {len(accounts)}개 계정, 조회 {total_f}건, 저장 {total_u}건, 매칭 {total_m}건")

        return results


def main():
    parser = argparse.ArgumentParser(description="발주서(주문) 동기화")
    parser.add_argument("--days", type=int, default=7, help="동기화 기간 (일, 기본 7)")
    parser.add_argument("--account", type=str, default=None, help="특정 계정명 (기본: 전체)")
    parser.add_argument("--status", type=str, default=None,
                        help="특정 상태만 (ACCEPT/INSTRUCT/DEPARTURE/DELIVERING/FINAL_DELIVERY/NONE_TRACKING)")
    parser.add_argument("--quick", action="store_true",
                        help="빠른 동기화: ACCEPT/INSTRUCT만 (1분 스케줄러용)")
    args = parser.parse_args()

    if args.quick:
        statuses = None  # 전체 상태 조회 (1일치라 양 적음)
    elif args.status:
        statuses = [args.status]
    else:
        statuses = None

    syncer = OrderSync()
    results = syncer.sync_all(days=args.days, account_name=args.account, statuses=statuses)

    # 리포트
    print("\n" + "=" * 60)
    print("주문 동기화 결과")
    print("=" * 60)
    for r in results:
        print(f"  {r['account']:12s} | 조회 {r['fetched']:5d} | 저장 {r['upserted']:5d} | 매칭 {r['matched']:5d}")
    print("=" * 60)

    # DB 확인
    eng = get_engine_for_db()
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        print(f"\norders 총 레코드: {cnt:,}건")


if __name__ == "__main__":
    main()
