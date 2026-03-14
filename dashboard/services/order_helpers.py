"""주문 공용 함수/SQL — order_service.py, sync_orders.py 공통 코드 추출.

parse_dt, extract_price, UPSERT_ORDER_SQL, build_upsert_params, extract_order_items
"""

import json
from datetime import datetime
from typing import List, Optional


def parse_dt(val) -> Optional[str]:
    """날짜/시간 문자열 파싱 (datetime 객체도 지원)."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)[:19]


def extract_price(val) -> int:
    """v4 plain int / v5 {units, nanos} 파싱."""
    if val is None:
        return 0
    if isinstance(val, dict):
        return int(val.get("units", 0) or 0)
    return int(val or 0)


def extract_order_items(ordersheet: dict) -> List[dict]:
    """발주서 응답에서 주문 아이템 추출.

    orderItems가 없으면 ordersheet 자체를 아이템으로 처리.
    """
    items = ordersheet.get("orderItems", [])
    if not items:
        return [ordersheet]
    return items


UPSERT_ORDER_SQL = """
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
        status = CASE
            WHEN ARRAY_POSITION(
                ARRAY['ACCEPT','INSTRUCT','DEPARTURE','NONE_TRACKING','DELIVERING','FINAL_DELIVERY'],
                EXCLUDED.status
            ) >= COALESCE(ARRAY_POSITION(
                ARRAY['ACCEPT','INSTRUCT','DEPARTURE','NONE_TRACKING','DELIVERING','FINAL_DELIVERY'],
                orders.status
            ), 0)
            THEN EXCLUDED.status
            ELSE orders.status
        END,
        ordered_at=EXCLUDED.ordered_at, paid_at=EXCLUDED.paid_at,
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


def build_upsert_params(account_id: int, status: str, os_data: dict, item: dict) -> Optional[dict]:
    """단일 주문 아이템 → DB UPSERT 파라미터 변환.

    Returns:
        dict of params or None if shipmentBoxId/orderId missing.
    """
    shipment_box_id = os_data.get("shipmentBoxId")
    order_id = os_data.get("orderId")
    if not shipment_box_id or not order_id:
        return None

    v_item_id = item.get("vendorItemId") or os_data.get("vendorItemId")
    sp_id = item.get("sellerProductId") or os_data.get("sellerProductId")
    sp_name = item.get("sellerProductName") or os_data.get("sellerProductName", "")

    orderer = os_data.get("orderer") or {}
    receiver = os_data.get("receiver") or {}
    addr1 = receiver.get("addr1", "") or ""
    addr2 = receiver.get("addr2", "") or ""

    return {
        "account_id": account_id,
        "shipment_box_id": int(shipment_box_id),
        "order_id": int(order_id),
        "vendor_item_id": int(v_item_id) if v_item_id else int(os_data.get("vendorItemId", 0) or 0),
        "status": status,
        "ordered_at": parse_dt(os_data.get("orderedAt")),
        "paid_at": parse_dt(os_data.get("paidAt")),
        "orderer_name": orderer.get("name", ""),
        "receiver_name": receiver.get("name", ""),
        "receiver_addr": f"{addr1} {addr2}".strip(),
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
        "raw_json": json.dumps(os_data, ensure_ascii=False, default=str),
        "updated_at": datetime.now().isoformat(),
    }
