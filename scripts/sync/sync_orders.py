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

    def _parse_datetime(self, dt_str) -> Optional[str]:
        """날짜/시간 문자열 파싱"""
        if not dt_str:
            return None
        if isinstance(dt_str, datetime):
            return dt_str.isoformat()
        # "2026-01-15T10:30:00" 형식
        return str(dt_str)[:19]

    @staticmethod
    def _extract_price(price_val) -> int:
        """v5 가격 Object {currencyCode, units, nanos} 또는 v4 plain int 파싱"""
        if price_val is None:
            return 0
        if isinstance(price_val, dict):
            return int(price_val.get("units", 0) or 0)
        return int(price_val or 0)

    def _extract_order_items(self, ordersheet: dict) -> List[dict]:
        """
        발주서 응답에서 주문 아이템 추출

        API 응답 구조:
        - shipmentBoxId, orderId, ordererName, ...
        - orderItems: [{vendorItemId, vendorItemName, shippingCount, ...}]
        """
        items = ordersheet.get("orderItems", [])
        if not items:
            # orderItems가 없으면 ordersheet 자체를 아이템으로 처리
            return [ordersheet]
        return items

    UPSERT_SQL = """
        INSERT INTO orders
            (account_id, shipment_box_id, order_id, vendor_item_id,
             status, ordered_at, paid_at,
             orderer_name, receiver_name, receiver_addr, receiver_post_code,
             product_id, seller_product_id, seller_product_name, vendor_item_name,
             shipping_count, cancel_count, hold_count_for_cancel,
             sales_price, order_price, discount_price, shipping_price,
             delivery_company_name, invoice_number, shipment_type,
             delivered_date, confirm_date,
             refer, canceled, listing_id, raw_json, updated_at)
        VALUES
            (:account_id, :shipment_box_id, :order_id, :vendor_item_id,
             :status, :ordered_at, :paid_at,
             :orderer_name, :receiver_name, :receiver_addr, :receiver_post_code,
             :product_id, :seller_product_id, :seller_product_name, :vendor_item_name,
             :shipping_count, :cancel_count, :hold_count_for_cancel,
             :sales_price, :order_price, :discount_price, :shipping_price,
             :delivery_company_name, :invoice_number, :shipment_type,
             :delivered_date, :confirm_date,
             :refer, :canceled, :listing_id, :raw_json, :updated_at)
        ON CONFLICT (account_id, shipment_box_id, vendor_item_id) DO UPDATE SET
            status=EXCLUDED.status, ordered_at=EXCLUDED.ordered_at, paid_at=EXCLUDED.paid_at,
            orderer_name=EXCLUDED.orderer_name, receiver_name=EXCLUDED.receiver_name,
            receiver_addr=EXCLUDED.receiver_addr, receiver_post_code=EXCLUDED.receiver_post_code,
            product_id=EXCLUDED.product_id, seller_product_id=EXCLUDED.seller_product_id,
            seller_product_name=EXCLUDED.seller_product_name, vendor_item_name=EXCLUDED.vendor_item_name,
            shipping_count=EXCLUDED.shipping_count, cancel_count=EXCLUDED.cancel_count,
            hold_count_for_cancel=EXCLUDED.hold_count_for_cancel,
            sales_price=EXCLUDED.sales_price, order_price=EXCLUDED.order_price,
            discount_price=EXCLUDED.discount_price, shipping_price=EXCLUDED.shipping_price,
            delivery_company_name=EXCLUDED.delivery_company_name, invoice_number=EXCLUDED.invoice_number,
            shipment_type=EXCLUDED.shipment_type, delivered_date=EXCLUDED.delivered_date,
            confirm_date=EXCLUDED.confirm_date, refer=EXCLUDED.refer, canceled=EXCLUDED.canceled,
            listing_id=EXCLUDED.listing_id, raw_json=EXCLUDED.raw_json, updated_at=EXCLUDED.updated_at
    """

    def sync_account(self, account: dict, date_from: date, date_to: date,
                     statuses: List[str] = None,
                     progress_callback: Callable = None) -> dict:
        """
        계정 1개의 주문 동기화

        Returns:
            {"account": str, "fetched": int, "upserted": int, "matched": int}
        """
        account_id = account["id"]
        account_name = account["account_name"]
        client = self._create_client(account)

        if statuses is None:
            statuses = ORDER_STATUSES

        logger.info(f"[{account_name}] 주문 동기화 시작: {date_from} ~ {date_to}")

        # 날짜 범위를 31일 윈도우로 분할
        windows = self._split_date_range(date_from, date_to)
        total_fetched = 0
        total_upserted = 0
        total_matched = 0

        for wi, (w_from, w_to) in enumerate(windows):
            for status in statuses:
                logger.info(f"  [{account_name}] 윈도우 {wi+1}/{len(windows)} 상태={status}: {w_from} ~ {w_to}")

                try:
                    ordersheets = client.get_all_ordersheets(w_from, w_to, status=status)
                except CoupangWingError as e:
                    logger.error(f"  [{account_name}] API 오류 ({status}): {e}")
                    continue

                if not ordersheets:
                    continue

                for os_data in ordersheets:
                    shipment_box_id = os_data.get("shipmentBoxId")
                    order_id = os_data.get("orderId")
                    if not shipment_box_id or not order_id:
                        continue

                    order_items = self._extract_order_items(os_data)

                    for item in order_items:
                        total_fetched += 1
                        v_item_id = item.get("vendorItemId") or os_data.get("vendorItemId")

                        # listing 매칭 (3-level)
                        sp_id = item.get("sellerProductId") or os_data.get("sellerProductId")
                        sp_name = item.get("sellerProductName") or os_data.get("sellerProductName", "")

                        # v5 응답: orderer/receiver가 중첩 객체, 가격은 {units, nanos} Object
                        orderer = os_data.get("orderer") or {}
                        receiver = os_data.get("receiver") or {}
                        addr1 = receiver.get("addr1", "") or ""
                        addr2 = receiver.get("addr2", "") or ""
                        receiver_addr = f"{addr1} {addr2}".strip()

                        params = {
                            "account_id": account_id,
                            "shipment_box_id": int(shipment_box_id),
                            "order_id": int(order_id),
                            "vendor_item_id": int(v_item_id) if v_item_id else 0,  # NULL → 0
                            "status": status,
                            "ordered_at": self._parse_datetime(os_data.get("orderedAt")),
                            "paid_at": self._parse_datetime(os_data.get("paidAt")),
                            "orderer_name": orderer.get("name", ""),
                            "receiver_name": receiver.get("name", ""),
                            "receiver_addr": receiver_addr,
                            "receiver_post_code": receiver.get("postCode", ""),
                            "product_id": int(item.get("productId") or 0) or None,
                            "seller_product_id": int(sp_id) if sp_id else None,
                            "seller_product_name": sp_name,
                            "vendor_item_name": item.get("vendorItemName") or "",
                            "shipping_count": int(item.get("shippingCount", 0) or 0),
                            "cancel_count": int(item.get("cancelCount", 0) or 0),
                            "hold_count_for_cancel": int(item.get("holdCountForCancel", 0) or 0),
                            "sales_price": self._extract_price(item.get("salesPrice")),
                            "order_price": self._extract_price(item.get("orderPrice")),
                            "discount_price": self._extract_price(item.get("discountPrice")),
                            "shipping_price": self._extract_price(os_data.get("shippingPrice")),
                            "delivery_company_name": os_data.get("deliveryCompanyName", ""),
                            "invoice_number": os_data.get("invoiceNumber", ""),
                            "shipment_type": os_data.get("shipmentType", ""),
                            "delivered_date": self._parse_datetime(os_data.get("deliveredDate")),
                            "confirm_date": self._parse_datetime(item.get("confirmDate")),
                            "refer": os_data.get("refer", ""),
                            "canceled": bool(item.get("canceled", False)),
                            "listing_id": None,
                            "raw_json": json.dumps(os_data, ensure_ascii=False, default=str)[:5000],
                            "updated_at": datetime.now().isoformat(),
                        }

                        try:
                            with self.engine.connect() as conn:
                                # 3-level listing 매칭
                                listing_id = match_listing(
                                    conn, account_id,
                                    vendor_item_id=v_item_id,
                                    coupang_product_id=sp_id,
                                    product_name=sp_name
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

        logger.info(f"주문 동기화: {len(accounts)}개 계정, {date_from} ~ {date_to}")

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
    parser = argparse.ArgumentParser(description="발주서(주문) 동기화")
    parser.add_argument("--days", type=int, default=7, help="동기화 기간 (일, 기본 7)")
    parser.add_argument("--account", type=str, default=None, help="특정 계정명 (기본: 전체)")
    parser.add_argument("--status", type=str, default=None,
                        help="특정 상태만 (ACCEPT/INSTRUCT/DEPARTURE/DELIVERING/FINAL_DELIVERY/NONE_TRACKING)")
    args = parser.parse_args()

    statuses = [args.status] if args.status else None

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
