"""주문 공유 데이터 로직 — orders.py / shipping.py 양쪽에서 사용.

WING API 실시간 조회 기반. DB는 백그라운드 동기화용으로만 사용.
"""

import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from dashboard.utils import create_wing_client, query_df
from dashboard.services.order_service import (
    extract_price as _extract_price,
    parse_dt as _parse_dt,
    save_ordersheets_to_db as _save_ordersheets_to_db,
)

logger = logging.getLogger(__name__)

# 조회 대상 상태
LIVE_STATUSES = ["ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY", "NONE_TRACKING"]

STATUS_MAP = {
    "ACCEPT": "결제완료", "INSTRUCT": "상품준비중", "DEPARTURE": "출고완료",
    "DELIVERING": "배송중", "FINAL_DELIVERY": "배송완료", "NONE_TRACKING": "추적불가",
}


def load_all_orders_live(accounts_df):
    """WING API 실시간 주문 조회 (세션 캐시 30초)"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 세션 캐시: 30초 이내 재호출 시 캐시 반환
    _cache_key = "_orders_live_cache"
    _ts_key = "_orders_live_ts"
    now = time.time()
    if now - st.session_state.get(_ts_key, 0) < 30 and _cache_key in st.session_state:
        return st.session_state[_cache_key]

    _today = date.today()
    _from_active = (_today - timedelta(days=7)).isoformat()
    _from_delivered = (_today - timedelta(days=30)).isoformat()
    _to = _today.isoformat()

    active_statuses = ["ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "NONE_TRACKING"]

    acct_clients = []
    for _, acct in accounts_df.iterrows():
        client = create_wing_client(acct)
        if client:
            acct_clients.append((acct, client))

    if not acct_clients:
        # API 클라이언트 없으면 DB fallback
        return _load_all_orders_from_db()

    def _fetch(acct, client, status, date_from):
        try:
            return acct, status, client.get_all_ordersheets(date_from, _to, status=status)
        except Exception as e:
            logger.warning(f"[{acct['account_name']}] {status} 조회 실패: {e}")
            return acct, status, []

    rows = []
    max_workers = min(len(acct_clients) * 7, 20) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for acct, client in acct_clients:
            for status in active_statuses:
                futures.append(pool.submit(_fetch, acct, client, status, _from_active))
            # FINAL_DELIVERY: 30일
            futures.append(pool.submit(_fetch, acct, client, "FINAL_DELIVERY", _from_delivered))

        for f in as_completed(futures):
            acct, status, ordersheets = f.result()
            acct_name = acct["account_name"]
            acct_id = int(acct["id"])

            # DB에도 저장 (백그라운드)
            if ordersheets:
                _save_ordersheets_to_db(acct, ordersheets, status)

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

                ordered_at = _parse_dt(os_data.get("orderedAt"))
                ordered_date = ordered_at[:10] if ordered_at else ""
                delivered_date = _parse_dt(os_data.get("deliveredDate"))
                delivered_date_str = delivered_date[:10] if delivered_date else ""

                for item in order_items:
                    vid = item.get("vendorItemId") or os_data.get("vendorItemId") or 0
                    spid = item.get("sellerProductId") or os_data.get("sellerProductId")
                    sp_name = item.get("sellerProductName") or os_data.get("sellerProductName", "")
                    order_price = _extract_price(item.get("orderPrice"))
                    sales_price = _extract_price(item.get("salesPrice"))
                    shipping_price = _extract_price(os_data.get("shippingPrice"))

                    rows.append({
                        "계정": acct_name,
                        "묶음배송번호": shipment_box_id,
                        "주문번호": order_id,
                        "상품명": sp_name,
                        "옵션명": item.get("vendorItemName") or "",
                        "수량": int(item.get("shippingCount", 0) or 0),
                        "결제금액": order_price,
                        "주문일": ordered_date,
                        "수취인": receiver.get("name", ""),
                        "상태": status,
                        "택배사": os_data.get("deliveryCompanyName", ""),
                        "운송장번호": os_data.get("invoiceNumber", ""),
                        "배송완료일": delivered_date_str,
                        "취소": bool(item.get("canceled", False)),
                        "_account_id": acct_id,
                        "_vendor_item_id": int(vid) if vid else 0,
                        "_seller_product_id": int(spid) if spid else None,
                        "_order_price_raw": order_price,
                        "주문일시": ordered_at or "",
                        "구매자": orderer.get("name", ""),
                        "구매자전화번호": "",
                        "수취인전화번호": "",
                        "우편번호": receiver.get("postCode", ""),
                        "수취인주소": receiver_addr,
                        "배송메세지": "",
                        "배송비": shipping_price,
                        "도서산간추가배송비": 0,
                        "결제위치": os_data.get("refer", ""),
                        "분리배송가능": False,
                        "주문시출고예정일": "",
                        "배송비구분": "",
                        "판매단가": sales_price,
                        "최초등록상품옵션명": "",
                        "업체상품코드": "",
                        "개인통관번호": "",
                        "통관용전화번호": "",
                    })

    if not rows:
        # API 결과 없으면 DB fallback
        return _load_all_orders_from_db()

    result = pd.DataFrame(rows)
    result = result.sort_values("주문일시", ascending=False).reset_index(drop=True)

    # 마지막 동기화 시각 기록
    st.session_state["order_last_synced"] = (datetime.utcnow() + timedelta(hours=9)).strftime("%H:%M:%S")

    # 세션 캐시 저장
    st.session_state[_cache_key] = result
    st.session_state[_ts_key] = now
    return result


def _load_all_orders_from_db():
    """DB fallback — API 호출 불가 시 사용"""
    _from = (date.today() - timedelta(days=30)).isoformat()
    return query_df("""
        SELECT a.account_name AS "계정",
               o.shipment_box_id AS "묶음배송번호",
               o.order_id AS "주문번호",
               o.seller_product_name AS "상품명",
               o.vendor_item_name AS "옵션명",
               o.shipping_count AS "수량",
               o.order_price AS "결제금액",
               to_char(o.ordered_at, 'YYYY-MM-DD') AS "주문일",
               o.receiver_name AS "수취인",
               o.status AS "상태",
               o.delivery_company_name AS "택배사",
               o.invoice_number AS "운송장번호",
               to_char(o.delivered_date, 'YYYY-MM-DD') AS "배송완료일",
               COALESCE(o.canceled, false) AS "취소",
               o.account_id AS "_account_id",
               o.vendor_item_id AS "_vendor_item_id",
               o.seller_product_id AS "_seller_product_id",
               o.order_price AS "_order_price_raw",
               to_char(o.ordered_at, 'YYYY-MM-DD HH24:MI:SS') AS "주문일시",
               o.orderer_name AS "구매자",
               '' AS "구매자전화번호",
               '' AS "수취인전화번호",
               o.receiver_post_code AS "우편번호",
               o.receiver_addr AS "수취인주소",
               '' AS "배송메세지",
               COALESCE(o.shipping_price, 0) AS "배송비",
               0 AS "도서산간추가배송비",
               COALESCE(o.refer, '') AS "결제위치",
               false AS "분리배송가능",
               '' AS "주문시출고예정일",
               '' AS "배송비구분",
               COALESCE(o.sales_price, 0) AS "판매단가",
               '' AS "최초등록상품옵션명",
               '' AS "업체상품코드",
               '' AS "개인통관번호",
               '' AS "통관용전화번호"
        FROM orders o
        JOIN accounts a ON o.account_id = a.id
        WHERE o.status IN ('ACCEPT','INSTRUCT','DEPARTURE','DELIVERING','NONE_TRACKING')
        UNION ALL
        SELECT a.account_name AS "계정",
               o.shipment_box_id AS "묶음배송번호",
               o.order_id AS "주문번호",
               o.seller_product_name AS "상품명",
               o.vendor_item_name AS "옵션명",
               o.shipping_count AS "수량",
               o.order_price AS "결제금액",
               to_char(o.ordered_at, 'YYYY-MM-DD') AS "주문일",
               o.receiver_name AS "수취인",
               o.status AS "상태",
               o.delivery_company_name AS "택배사",
               o.invoice_number AS "운송장번호",
               to_char(o.delivered_date, 'YYYY-MM-DD') AS "배송완료일",
               COALESCE(o.canceled, false) AS "취소",
               o.account_id AS "_account_id",
               o.vendor_item_id AS "_vendor_item_id",
               o.seller_product_id AS "_seller_product_id",
               o.order_price AS "_order_price_raw",
               to_char(o.ordered_at, 'YYYY-MM-DD HH24:MI:SS') AS "주문일시",
               o.orderer_name AS "구매자",
               '' AS "구매자전화번호",
               '' AS "수취인전화번호",
               o.receiver_post_code AS "우편번호",
               o.receiver_addr AS "수취인주소",
               '' AS "배송메세지",
               COALESCE(o.shipping_price, 0) AS "배송비",
               0 AS "도서산간추가배송비",
               COALESCE(o.refer, '') AS "결제위치",
               false AS "분리배송가능",
               '' AS "주문시출고예정일",
               '' AS "배송비구분",
               COALESCE(o.sales_price, 0) AS "판매단가",
               '' AS "최초등록상품옵션명",
               '' AS "업체상품코드",
               '' AS "개인통관번호",
               '' AS "통관용전화번호"
        FROM orders o
        JOIN accounts a ON o.account_id = a.id
        WHERE o.status = 'FINAL_DELIVERY'
          AND o.ordered_at >= :date_from
        ORDER BY "주문일시" DESC
    """, {"date_from": _from})


def get_instruct_orders(all_orders):
    """INSTRUCT 상태 필터링 (취소 제외)"""
    if all_orders.empty:
        return pd.DataFrame()
    instruct = all_orders[all_orders["상태"] == "INSTRUCT"].copy()
    if instruct.empty:
        return pd.DataFrame()
    return instruct[~instruct["취소"]].copy()


def get_instruct_by_box(instruct_all):
    """묶음배송 단위 그룹핑"""
    if instruct_all.empty:
        return pd.DataFrame()
    return instruct_all.groupby(["계정", "묶음배송번호", "주문번호", "주문일", "수취인"]).agg(
        상품명=("상품명", lambda x: " / ".join(x.unique())),
        수량=("수량", "sum"),
        결제금액=("_order_price_raw", "sum"),
    ).reset_index()


def clear_order_caches():
    """세션 캐시 초기화 → 다음 호출 시 API 재조회"""
    st.session_state.pop("_orders_live_cache", None)
    st.session_state.pop("_orders_live_ts", None)
    st.cache_data.clear()


def fmt_krw_short(val):
    """금액 축약 포맷"""
    val = int(val)
    if abs(val) >= 100_000_000:
        return f"{val / 100_000_000:.1f}억"
    elif abs(val) >= 10_000:
        return f"{val / 10_000:.0f}만"
    else:
        return f"{val:,}"


def sync_live_orders(accounts_df):
    """WING API → DB 저장 (수동 동기화 버튼용)"""
    # load_all_orders_live가 이미 API 호출 + DB 저장을 하므로,
    # 캐시만 클리어하면 다음 호출 시 자동으로 최신 데이터 로드
    clear_order_caches()
    result = load_all_orders_live(accounts_df)
    return len(result) if not result.empty else 0
