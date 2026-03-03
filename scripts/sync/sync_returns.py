"""
반품/취소 동기화 스크립트
============================
WING Return Request API → return_requests 테이블

사용법:
    python scripts/sync_returns.py              # 기본 30일
    python scripts/sync_returns.py --days 60    # 최근 60일
    python scripts/sync_returns.py --account 007-book  # 특정 계정만
    python scripts/sync_returns.py --status RU         # 특정 상태만
"""
import os
import sys
import json
import argparse
import logging
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 조회 대상 상태 목록
# RU: 출고중지요청, UC: 반품접수(미확인), CC: 쿠팡확인요청, PR: 반품처리완료
RETURN_STATUSES = ["RU", "UC", "CC", "PR"]


class ReturnSync:
    """반품/취소 동기화 엔진"""

    CREATE_INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS ix_return_account_created ON return_requests(account_id, created_at_api)",
        "CREATE INDEX IF NOT EXISTS ix_return_account_status ON return_requests(account_id, receipt_status)",
        "CREATE INDEX IF NOT EXISTS ix_return_order_id ON return_requests(order_id)",
    ]

    UPSERT_SQL = """
        INSERT INTO return_requests
            (account_id, receipt_id, order_id, payment_id,
             receipt_type, receipt_status,
             created_at_api, modified_at_api,
             requester_name, requester_phone, requester_address,
             requester_address_detail, requester_zip_code,
             cancel_reason_category1, cancel_reason_category2, cancel_reason,
             cancel_count_sum,
             return_delivery_id, return_delivery_type, release_stop_status,
             fault_by_type, pre_refund,
             complete_confirm_type, complete_confirm_date,
             reason_code, reason_code_text,
             return_shipping_charge, enclose_price,
             return_items_json, return_delivery_json, raw_json,
             listing_id, updated_at)
        VALUES
            (:account_id, :receipt_id, :order_id, :payment_id,
             :receipt_type, :receipt_status,
             :created_at_api, :modified_at_api,
             :requester_name, :requester_phone, :requester_address,
             :requester_address_detail, :requester_zip_code,
             :cancel_reason_category1, :cancel_reason_category2, :cancel_reason,
             :cancel_count_sum,
             :return_delivery_id, :return_delivery_type, :release_stop_status,
             :fault_by_type, :pre_refund,
             :complete_confirm_type, :complete_confirm_date,
             :reason_code, :reason_code_text,
             :return_shipping_charge, :enclose_price,
             :return_items_json, :return_delivery_json, :raw_json,
             :listing_id, :updated_at)
        ON CONFLICT (account_id, receipt_id) DO UPDATE SET
            receipt_type=EXCLUDED.receipt_type, receipt_status=EXCLUDED.receipt_status,
            created_at_api=EXCLUDED.created_at_api, modified_at_api=EXCLUDED.modified_at_api,
            requester_name=EXCLUDED.requester_name, requester_phone=EXCLUDED.requester_phone,
            requester_address=EXCLUDED.requester_address, requester_address_detail=EXCLUDED.requester_address_detail,
            requester_zip_code=EXCLUDED.requester_zip_code,
            cancel_reason_category1=EXCLUDED.cancel_reason_category1, cancel_reason_category2=EXCLUDED.cancel_reason_category2,
            cancel_reason=EXCLUDED.cancel_reason, cancel_count_sum=EXCLUDED.cancel_count_sum,
            return_delivery_id=EXCLUDED.return_delivery_id, return_delivery_type=EXCLUDED.return_delivery_type,
            release_stop_status=EXCLUDED.release_stop_status, fault_by_type=EXCLUDED.fault_by_type,
            pre_refund=EXCLUDED.pre_refund, complete_confirm_type=EXCLUDED.complete_confirm_type,
            complete_confirm_date=EXCLUDED.complete_confirm_date,
            reason_code=EXCLUDED.reason_code, reason_code_text=EXCLUDED.reason_code_text,
            return_shipping_charge=EXCLUDED.return_shipping_charge, enclose_price=EXCLUDED.enclose_price,
            return_items_json=EXCLUDED.return_items_json, return_delivery_json=EXCLUDED.return_delivery_json,
            raw_json=EXCLUDED.raw_json, listing_id=EXCLUDED.listing_id, updated_at=EXCLUDED.updated_at
    """

    def __init__(self, db_path: str = None):
        self.engine = get_engine_for_db(db_path)
        self._ensure_table()

    def _ensure_table(self):
        """인덱스 확인"""
        with self.engine.connect() as conn:
            for idx_sql in self.CREATE_INDEXES_SQL:
                try:
                    conn.execute(text(idx_sql))
                except Exception:
                    pass
            conn.commit()
        logger.info("return_requests 테이블 확인 완료")

    def _get_accounts(self, account_name: str = None) -> list:
        """WING API 활성 계정 목록"""
        return get_accounts(self.engine, account_name)

    def _create_client(self, account: dict) -> CoupangWingClient:
        """계정 정보로 WING 클라이언트 생성"""
        return create_wing_client(account)

    def _parse_datetime(self, dt_str) -> Optional[str]:
        """날짜/시간 문자열 파싱"""
        if not dt_str:
            return None
        if isinstance(dt_str, datetime):
            return dt_str.isoformat()
        return str(dt_str)[:19]

    def _extract_shipping_charge(self, data: dict) -> Optional[int]:
        """반품배송비 추출 (returnShippingCharge.units)"""
        charge = data.get("returnShippingCharge")
        if isinstance(charge, dict):
            return int(charge.get("units", 0))
        if isinstance(charge, (int, float)):
            return int(charge)
        return None

    def _extract_enclose_price(self, data: dict) -> Optional[int]:
        """동봉배송비 추출 (enclosePrice.units)"""
        price = data.get("enclosePrice")
        if isinstance(price, dict):
            return int(price.get("units", 0))
        if isinstance(price, (int, float)):
            return int(price)
        return None

    def sync_account(self, account: dict, date_from: date, date_to: date,
                     statuses: List[str] = None,
                     progress_callback: Callable = None) -> dict:
        """
        계정 1개의 반품/취소 동기화

        Returns:
            {"account": str, "fetched": int, "upserted": int, "matched": int}
        """
        account_id = account["id"]
        account_name = account["account_name"]
        client = self._create_client(account)

        if statuses is None:
            statuses = RETURN_STATUSES

        logger.info(f"[{account_name}] 반품 동기화 시작: {date_from} ~ {date_to}")

        # 날짜 범위를 31일 윈도우로 분할
        windows = self._split_date_range(date_from, date_to)
        total_fetched = 0
        total_upserted = 0
        total_matched = 0

        for wi, (w_from, w_to) in enumerate(windows):
            # 상태별 반품 조회 → receipt_id 기준 중복 제거
            seen_ids = set()
            all_returns = []

            for status in statuses:
                logger.info(f"  [{account_name}] 윈도우 {wi+1}/{len(windows)} 상태={status}: {w_from} ~ {w_to}")
                try:
                    items = client.get_all_return_requests(w_from, w_to, status=status)
                    for item in items:
                        rid = item.get("receiptId")
                        if rid and rid not in seen_ids:
                            seen_ids.add(rid)
                            all_returns.append(item)
                except CoupangWingError as e:
                    logger.error(f"  [{account_name}] API 오류 (반품 {status}): {e}")

            # 취소 유형도 별도 조회
            logger.info(f"  [{account_name}] 윈도우 {wi+1}/{len(windows)} cancelType=CANCEL: {w_from} ~ {w_to}")
            try:
                cancel_returns = client.get_all_return_requests(w_from, w_to, cancel_type="CANCEL")
                for cr in cancel_returns:
                    rid = cr.get("receiptId")
                    if rid and rid not in seen_ids:
                        seen_ids.add(rid)
                        all_returns.append(cr)
            except CoupangWingError as e:
                logger.error(f"  [{account_name}] API 오류 (취소): {e}")

            for ret_data in all_returns:
                total_fetched += 1
                receipt_id = ret_data.get("receiptId")
                if not receipt_id:
                    continue

                # returnItems에서 상품 정보 추출
                return_items = ret_data.get("returnItems", [])
                seller_product_id = None
                vendor_item_id = None
                product_name = None
                if return_items:
                    first_item = return_items[0]
                    seller_product_id = first_item.get("sellerProductId")
                    vendor_item_id = first_item.get("vendorItemId")
                    product_name = first_item.get("sellerProductName", "")

                # 수량 합산
                cancel_count_sum = sum(
                    int(item.get("cancelCount", 0) or 0)
                    for item in return_items
                ) if return_items else int(ret_data.get("cancelCountSum", 0) or 0)

                params = {
                    "account_id": account_id,
                    "receipt_id": int(receipt_id),
                    "order_id": int(ret_data.get("orderId", 0) or 0) or None,
                    "payment_id": int(ret_data.get("paymentId", 0) or 0) or None,
                    "receipt_type": ret_data.get("receiptType", ""),
                    "receipt_status": ret_data.get("receiptStatus", ""),
                    "created_at_api": self._parse_datetime(ret_data.get("createdAt")),
                    "modified_at_api": self._parse_datetime(ret_data.get("modifiedAt")),
                    "requester_name": ret_data.get("requesterName", ""),
                    "requester_phone": ret_data.get("requesterPhone", ""),
                    "requester_address": ret_data.get("requesterAddress", ""),
                    "requester_address_detail": ret_data.get("requesterAddressDetail", ""),
                    "requester_zip_code": ret_data.get("requesterZipCode", ""),
                    "cancel_reason_category1": ret_data.get("cancelReasonCategory1", ""),
                    "cancel_reason_category2": ret_data.get("cancelReasonCategory2", ""),
                    "cancel_reason": ret_data.get("cancelReason", ""),
                    "cancel_count_sum": cancel_count_sum,
                    "return_delivery_id": int(ret_data.get("returnDeliveryId", 0) or 0) or None,
                    "return_delivery_type": ret_data.get("returnDeliveryType", ""),
                    "release_stop_status": ret_data.get("releaseStopStatus", ""),
                    "fault_by_type": ret_data.get("faultByType", ""),
                    "pre_refund": bool(ret_data.get("preRefund", False)),
                    "complete_confirm_type": ret_data.get("completeConfirmType", ""),
                    "complete_confirm_date": self._parse_datetime(ret_data.get("completeConfirmDate")),
                    "reason_code": ret_data.get("reasonCode", ""),
                    "reason_code_text": ret_data.get("reasonCodeText", ""),
                    "return_shipping_charge": self._extract_shipping_charge(ret_data),
                    "enclose_price": self._extract_enclose_price(ret_data),
                    "return_items_json": json.dumps(return_items, ensure_ascii=False, default=str)[:5000] if return_items else None,
                    "return_delivery_json": json.dumps(ret_data.get("returnDeliveryDtos", []), ensure_ascii=False, default=str)[:5000] if ret_data.get("returnDeliveryDtos") else None,
                    "raw_json": json.dumps(ret_data, ensure_ascii=False, default=str)[:5000],
                    "listing_id": None,
                    "updated_at": datetime.utcnow().isoformat(),
                }

                try:
                    with self.engine.connect() as conn:
                        # 3-level listing 매칭
                        listing_id = match_listing(
                            conn, account_id,
                            vendor_item_id=vendor_item_id,
                            coupang_product_id=seller_product_id,
                            product_name=product_name
                        )
                        if listing_id:
                            params["listing_id"] = listing_id
                            total_matched += 1

                        conn.execute(text(self.UPSERT_SQL), params)
                        conn.commit()
                    total_upserted += 1
                except SQLAlchemyError as e:
                    logger.warning(f"  DB 오류: {e}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"  데이터 변환 오류: {e}")

            if progress_callback:
                progress_callback(wi + 1, len(windows),
                                  f"[{account_name}] {wi+1}/{len(windows)} 윈도우 완료 ({total_fetched}건)")

        result = {
            "account": account_name,
            "fetched": total_fetched,
            "upserted": total_upserted,
            "matched": total_matched,
        }
        logger.info(f"[{account_name}] 완료: 조회 {total_fetched}건, 저장 {total_upserted}건, 매칭 {total_matched}건")
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

    def sync_all(self, days: int = 30, account_name: str = None,
                 statuses: List[str] = None,
                 progress_callback: Callable = None) -> List[dict]:
        """
        전체 계정 반품/취소 동기화

        Args:
            days: 동기화 기간 (일, 기본 30)
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

        logger.info(f"반품 동기화: {len(accounts)}개 계정, {date_from} ~ {date_to}")

        results = []
        for i, account in enumerate(accounts):
            if progress_callback:
                progress_callback(i, len(accounts),
                                  f"{account['account_name']} 동기화 중...")

            result = self.sync_account(account, date_from, date_to,
                                       statuses=statuses,
                                       progress_callback=progress_callback)
            results.append(result)

        if progress_callback:
            progress_callback(len(accounts), len(accounts), "동기화 완료!")

        # 결과 요약
        total_f = sum(r["fetched"] for r in results)
        total_u = sum(r["upserted"] for r in results)
        total_m = sum(r["matched"] for r in results)
        logger.info(f"전체 완료: {len(accounts)}개 계정, 조회 {total_f}건, 저장 {total_u}건, 매칭 {total_m}건")

        return results


def main():
    parser = argparse.ArgumentParser(description="반품/취소 동기화")
    parser.add_argument("--days", type=int, default=30, help="동기화 기간 (일, 기본 30)")
    parser.add_argument("--account", type=str, default=None, help="특정 계정명 (기본: 전체)")
    parser.add_argument("--status", type=str, default=None,
                        help="특정 상태만 (RU/UC/CC/PR)")
    args = parser.parse_args()

    statuses = [args.status] if args.status else None

    syncer = ReturnSync()
    results = syncer.sync_all(days=args.days, account_name=args.account, statuses=statuses)

    # 리포트
    print("\n" + "=" * 60)
    print("반품/취소 동기화 결과")
    print("=" * 60)
    for r in results:
        print(f"  {r['account']:12s} | 조회 {r['fetched']:5d} | 저장 {r['upserted']:5d} | 매칭 {r['matched']:5d}")
    print("=" * 60)

    # DB 확인
    eng = get_engine_for_db()
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM return_requests")).scalar()
        print(f"\nreturn_requests 총 레코드: {cnt:,}건")


if __name__ == "__main__":
    main()
