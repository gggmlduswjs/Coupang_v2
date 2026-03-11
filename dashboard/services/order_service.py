"""주문 비즈니스 로직 — UI 독립 함수들.

orders.py에서 추출된 순수 데이터 처리 함수.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text as sa_text

from dashboard.utils import engine

logger = logging.getLogger(__name__)

_IS_LOCAL = sys.platform == "win32"
_HANJIN_CREDS_PATH = Path(__file__).resolve().parents[2] / "hanjin_creds.json"


def load_hanjin_creds() -> dict:
    if _HANJIN_CREDS_PATH.exists():
        try:
            return json.loads(_HANJIN_CREDS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_hanjin_creds(user_id: str, password: str):
    data = {"user_id": user_id, "password": password}
    _HANJIN_CREDS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_dt(val):
    """날짜/시간 문자열 파싱"""
    if not val:
        return None
    return str(val)[:19]


def extract_price(val):
    """v4 plain int / v5 {units, nanos} 파싱"""
    if val is None:
        return 0
    if isinstance(val, dict):
        return int(val.get("units", 0) or 0)
    return int(val or 0)


# ── 주문 DB 저장용 UPSERT SQL ──
_UPSERT_ORDER_SQL = """
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


def save_ordersheets_to_db(acct, ordersheets, status):
    """WING API 응답 → orders 테이블 UPSERT (백그라운드 스레드, match_listing 생략)"""
    if not ordersheets:
        return

    def _do_save():
        account_id = int(acct["id"])
        try:
            with engine.connect() as conn:
                for os_data in ordersheets:
                    shipment_box_id = os_data.get("shipmentBoxId")
                    order_id = os_data.get("orderId")
                    if not shipment_box_id or not order_id:
                        continue
                    order_items = os_data.get("orderItems", [])
                    if not order_items:
                        order_items = [os_data]
                    orderer = os_data.get("orderer") or {}
                    receiver = os_data.get("receiver") or {}
                    addr1 = receiver.get("addr1", "") or ""
                    addr2 = receiver.get("addr2", "") or ""
                    receiver_addr = f"{addr1} {addr2}".strip()
                    for item in order_items:
                        v_item_id = item.get("vendorItemId") or os_data.get("vendorItemId")
                        sp_id = item.get("sellerProductId") or os_data.get("sellerProductId")
                        sp_name = item.get("sellerProductName") or os_data.get("sellerProductName", "")
                        params = {
                            "account_id": account_id,
                            "shipment_box_id": int(shipment_box_id),
                            "order_id": int(order_id),
                            "vendor_item_id": int(v_item_id) if v_item_id else 0,
                            "status": status,
                            "ordered_at": parse_dt(os_data.get("orderedAt")),
                            "paid_at": parse_dt(os_data.get("paidAt")),
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
                            "sales_price": extract_price(item.get("salesPrice")),
                            "order_price": extract_price(item.get("orderPrice")),
                            "discount_price": extract_price(item.get("discountPrice")),
                            "shipping_price": extract_price(os_data.get("shippingPrice")),
                            "delivery_company_name": os_data.get("deliveryCompanyName", ""),
                            "invoice_number": os_data.get("invoiceNumber", ""),
                            "shipment_type": os_data.get("shipmentType", ""),
                            "delivered_date": parse_dt(os_data.get("deliveredDate")),
                            "confirm_date": parse_dt(item.get("confirmDate")),
                            "refer": os_data.get("refer", ""),
                            "canceled": bool(item.get("canceled", False)),
                            "listing_id": None,
                            "raw_json": json.dumps(os_data, ensure_ascii=False, default=str)[:5000],
                            "updated_at": datetime.now().isoformat(),
                        }
                        try:
                            conn.execute(sa_text(_UPSERT_ORDER_SQL), params)
                        except Exception as e:
                            logger.warning(
                                f"주문 UPSERT 실패 (box={params.get('shipment_box_id')}, "
                                f"vid={params.get('vendor_item_id')}): {e}"
                            )
                conn.commit()
        except Exception as e:
            logger.warning(f"주문 DB 저장 오류: {e}")

    import threading
    threading.Thread(target=_do_save, daemon=True).start()


def update_orders_status_after_invoice(success_items):
    """송장 등록 성공 후 DB 주문 상태를 DEPARTURE로 업데이트.

    Args:
        success_items: list of dict with keys:
            - shipmentBoxId (int)
            - invoiceNumber (str)
            - deliveryCompanyCode (str, e.g. "HANJIN")
    """
    if not success_items:
        return

    def _do_update():
        try:
            with engine.connect() as conn:
                for item in success_items:
                    box_id = item.get("shipmentBoxId")
                    invoice = item.get("invoiceNumber", "")
                    company = item.get("deliveryCompanyCode", "HANJIN")
                    company_name = {"HANJIN": "한진택배"}.get(company, company)
                    if not box_id:
                        continue
                    conn.execute(sa_text("""
                        UPDATE orders
                        SET status = 'DEPARTURE',
                            invoice_number = :invoice,
                            delivery_company_name = :company_name,
                            updated_at = :updated_at
                        WHERE shipment_box_id = :box_id
                          AND status = 'INSTRUCT'
                    """), {
                        "box_id": int(box_id),
                        "invoice": str(invoice).strip(),
                        "company_name": company_name,
                        "updated_at": datetime.now().isoformat(),
                    })
                conn.commit()
        except Exception as e:
            logger.warning(f"송장 등록 후 DB 상태 업데이트 오류: {e}")

    import threading
    threading.Thread(target=_do_update, daemon=True).start()
