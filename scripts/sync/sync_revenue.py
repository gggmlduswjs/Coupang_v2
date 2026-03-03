"""
매출 내역 동기화 스크립트
========================
WING Revenue History API → revenue_history 테이블

사용법:
    python scripts/sync_revenue.py              # 기본 3개월
    python scripts/sync_revenue.py --months 1   # 최근 1개월
    python scripts/sync_revenue.py --account 007-book  # 특정 계정만
"""
import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Callable

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


class RevenueSync:
    """매출 내역 동기화 엔진"""

    CREATE_INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS ix_rev_account_date ON revenue_history(account_id, recognition_date)",
        "CREATE INDEX IF NOT EXISTS ix_rev_recognition ON revenue_history(recognition_date)",
        "CREATE INDEX IF NOT EXISTS ix_rev_listing ON revenue_history(listing_id)",
    ]

    UPSERT_SQL = """
        INSERT INTO revenue_history
            (account_id, order_id, sale_type, sale_date, recognition_date,
             settlement_date, product_id, product_name, vendor_item_id,
             vendor_item_name, sale_price, quantity, coupang_discount,
             sale_amount, seller_discount, service_fee, service_fee_vat,
             service_fee_ratio, settlement_amount, delivery_fee_amount,
             delivery_fee_settlement, listing_id)
        VALUES
            (:account_id, :order_id, :sale_type, :sale_date, :recognition_date,
             :settlement_date, :product_id, :product_name, :vendor_item_id,
             :vendor_item_name, :sale_price, :quantity, :coupang_discount,
             :sale_amount, :seller_discount, :service_fee, :service_fee_vat,
             :service_fee_ratio, :settlement_amount, :delivery_fee_amount,
             :delivery_fee_settlement, :listing_id)
        ON CONFLICT (account_id, order_id, vendor_item_id) DO NOTHING
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
        logger.info("revenue_history 테이블 확인 완료")

    def _get_accounts(self, account_name: str = None) -> list:
        """WING API 활성화된 계정 목록 조회"""
        return get_accounts(self.engine, account_name)

    def _create_client(self, account: dict) -> CoupangWingClient:
        """계정 정보로 WING 클라이언트 생성"""
        return create_wing_client(account)

    @staticmethod
    def _split_date_range(date_from: date, date_to: date, window_days: int = 29) -> List[Tuple[str, str]]:
        """날짜 범위를 window_days 단위 윈도우로 분할"""
        windows = []
        current = date_from
        while current <= date_to:
            end = min(current + timedelta(days=window_days - 1), date_to)
            windows.append((current.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
            current = end + timedelta(days=1)
        return windows

    def _parse_date(self, date_str) -> Optional[str]:
        """날짜 문자열 파싱 (다양한 형식 지원)"""
        if not date_str:
            return None
        if isinstance(date_str, date):
            return date_str.isoformat()
        # "2026-01-15" 또는 "2026-01-15T00:00:00" 형식
        return str(date_str)[:10]

    def sync_account(self, account: dict, date_from: date, date_to: date,
                     progress_callback: Callable = None) -> dict:
        """
        계정 1개의 매출 동기화

        Returns:
            {"account": str, "fetched": int, "inserted": int, "matched": int}
        """
        account_id = account["id"]
        account_name = account["account_name"]
        client = self._create_client(account)

        logger.info(f"[{account_name}] 매출 동기화 시작: {date_from} ~ {date_to}")

        windows = self._split_date_range(date_from, date_to)
        total_fetched = 0
        total_inserted = 0
        total_matched = 0

        for wi, (w_from, w_to) in enumerate(windows):
            logger.info(f"  [{account_name}] 윈도우 {wi+1}/{len(windows)}: {w_from} ~ {w_to}")

            try:
                orders = client.get_all_revenue_history(w_from, w_to)
            except CoupangWingError as e:
                logger.error(f"  [{account_name}] API 오류: {e}")
                continue

            if not orders:
                logger.info(f"  [{account_name}] 데이터 없음")
                continue

            with self.engine.connect() as conn:
                for order in orders:
                    # Revenue API 응답 구조: 주문 단위 또는 아이템 단위
                    items = order.get("items", [order])  # items가 없으면 order 자체가 아이템

                    for item in items:
                        total_fetched += 1
                        order_id = item.get("orderId") or order.get("orderId")
                        v_item_id = item.get("vendorItemId")
                        p_id = item.get("productId") or order.get("productId")

                        if not order_id or not v_item_id:
                            continue

                        # 3-level listing 매칭
                        p_name = item.get("productName") or item.get("vendorItemName", "")
                        listing_id = match_listing(
                            conn, account_id,
                            vendor_item_id=v_item_id,
                            coupang_product_id=p_id,
                            product_name=p_name
                        )
                        if listing_id:
                            total_matched += 1

                        sale_date = self._parse_date(item.get("saleDate") or order.get("saleDate"))
                        recog_date = self._parse_date(item.get("recognitionDate") or order.get("recognitionDate"))
                        settle_date = self._parse_date(item.get("settlementDate") or order.get("settlementDate"))

                        if not sale_date or not recog_date:
                            continue

                        try:
                            conn.execute(text(self.UPSERT_SQL), {
                                "account_id": account_id,
                                "order_id": int(order_id),
                                "sale_type": item.get("saleType", "SALE"),
                                "sale_date": sale_date,
                                "recognition_date": recog_date,
                                "settlement_date": settle_date,
                                "product_id": int(p_id) if p_id else None,
                                "product_name": item.get("productName") or item.get("vendorItemName", ""),
                                "vendor_item_id": int(v_item_id),
                                "vendor_item_name": item.get("vendorItemName", ""),
                                "sale_price": int(item.get("salePrice", 0) or 0),
                                "quantity": int(item.get("quantity", 0) or 0),
                                "coupang_discount": int(item.get("coupangDiscount", 0) or 0),
                                "sale_amount": int(item.get("saleAmount", 0) or 0),
                                "seller_discount": int(item.get("sellerDiscount", 0) or 0),
                                "service_fee": int(item.get("serviceFee", 0) or 0),
                                "service_fee_vat": int(item.get("serviceFeeVat", 0) or 0),
                                "service_fee_ratio": float(item.get("serviceFeeRatio", 0) or 0),
                                "settlement_amount": int(item.get("settlementAmount", 0) or 0),
                                "delivery_fee_amount": int(item.get("deliveryFeeAmount", 0) or 0),
                                "delivery_fee_settlement": int(item.get("deliveryFeeSettlement", 0) or 0),
                                "listing_id": listing_id,
                            })
                            total_inserted += 1
                        except IntegrityError:
                            # 중복 레코드 - 정상적으로 무시
                            logger.debug(f"  중복 스킵: order_id={order_id}")
                        except SQLAlchemyError as e:
                            logger.warning(f"  DB 오류: {e}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"  데이터 변환 오류: {e}")

                conn.commit()

            if progress_callback:
                progress_callback(wi + 1, len(windows),
                                  f"[{account_name}] {wi+1}/{len(windows)} 윈도우 완료 ({total_fetched}건)")

        result = {
            "account": account_name,
            "fetched": total_fetched,
            "inserted": total_inserted,
            "matched": total_matched,
        }
        logger.info(f"[{account_name}] 완료: 조회 {total_fetched}건, 저장 {total_inserted}건, 매칭 {total_matched}건")
        return result

    def sync_all(self, months: int = 3, account_name: str = None,
                 progress_callback: Callable = None) -> List[dict]:
        """
        전체 계정 매출 동기화

        Args:
            months: 동기화 기간 (개월)
            account_name: 특정 계정만 (None=전체)
            progress_callback: 진행 콜백 (current, total, message)

        Returns:
            계정별 결과 리스트
        """
        accounts = self._get_accounts(account_name)
        if not accounts:
            logger.warning("WING API 활성화된 계정이 없습니다.")
            return []

        date_to = date.today() - timedelta(days=1)  # API는 어제까지만 조회 가능
        date_from = date_to - timedelta(days=months * 30)

        logger.info(f"매출 동기화: {len(accounts)}개 계정, {date_from} ~ {date_to}")

        results = []
        for i, account in enumerate(accounts):
            if progress_callback:
                progress_callback(i, len(accounts),
                                  f"{account['account_name']} 동기화 중...")

            result = self.sync_account(account, date_from, date_to, progress_callback)
            results.append(result)

        if progress_callback:
            progress_callback(len(accounts), len(accounts), "동기화 완료!")

        # 결과 요약
        total_f = sum(r["fetched"] for r in results)
        total_i = sum(r["inserted"] for r in results)
        total_m = sum(r["matched"] for r in results)
        logger.info(f"전체 완료: {len(accounts)}개 계정, 조회 {total_f}건, 저장 {total_i}건, 매칭 {total_m}건")

        return results


def main():
    parser = argparse.ArgumentParser(description="매출 내역 동기화")
    parser.add_argument("--months", type=int, default=3, help="동기화 기간 (개월, 기본 3)")
    parser.add_argument("--account", type=str, default=None, help="특정 계정명 (기본: 전체)")
    args = parser.parse_args()

    syncer = RevenueSync()
    results = syncer.sync_all(months=args.months, account_name=args.account)

    # 리포트
    print("\n" + "=" * 60)
    print("매출 동기화 결과")
    print("=" * 60)
    for r in results:
        print(f"  {r['account']:12s} | 조회 {r['fetched']:5d} | 저장 {r['inserted']:5d} | 매칭 {r['matched']:5d}")
    print("=" * 60)

    # DB 확인
    eng = get_engine_for_db()
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM revenue_history")).scalar()
        print(f"\nrevenue_history 총 레코드: {cnt:,}건")


if __name__ == "__main__":
    main()
