"""주문 공유 데이터 로직 — orders.py / shipping.py 양쪽에서 사용.

UI 코드 없이 데이터 로드/필터/동기화만 담당.
@st.cache_data 는 함수 identity 기반이므로, 같은 함수를 호출하면 캐시 자동 공유.
"""

import os
import sys
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from dashboard.utils import create_wing_client, query_df
from dashboard.services.order_service import save_ordersheets_to_db as _save_ordersheets_to_db


# 동기화 대상 상태
LIVE_STATUSES = ["ACCEPT", "INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY", "NONE_TRACKING"]

STATUS_MAP = {
    "ACCEPT": "결제완료", "INSTRUCT": "상품준비중", "DEPARTURE": "출고완료",
    "DELIVERING": "배송중", "FINAL_DELIVERY": "배송완료", "NONE_TRACKING": "추적불가",
}


@st.cache_data(ttl=30)
def load_all_orders_from_db():
    """DB에서 활성 주문(전체) + 배송완료(30일) 조회"""
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
    """캐시 초기화"""
    load_all_orders_from_db.clear()
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
    """WING API 병렬 호출 → DB 저장 (전체 상태, 최근 7일)"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _today = date.today()
    _from = (_today - timedelta(days=7)).isoformat()
    _to = _today.isoformat()

    acct_clients = []
    for _, acct in accounts_df.iterrows():
        client = create_wing_client(acct)
        if client:
            acct_clients.append((acct, client))

    def _fetch_one(acct, client, status):
        try:
            return acct, status, client.get_all_ordersheets(_from, _to, status=status)
        except Exception:
            return acct, status, []

    total = 0
    with ThreadPoolExecutor(max_workers=len(LIVE_STATUSES) * len(acct_clients) or 10) as pool:
        futures = []
        for acct, client in acct_clients:
            for status in LIVE_STATUSES:
                futures.append(pool.submit(_fetch_one, acct, client, status))
        for f in as_completed(futures):
            acct, status, ordersheets = f.result()
            if ordersheets:
                _save_ordersheets_to_db(acct, ordersheets, status)
                total += len(ordersheets)

    # 마지막 동기화 시각 기록
    st.session_state["order_last_synced"] = (datetime.utcnow() + timedelta(hours=9)).strftime("%H:%M:%S")
    return total


def can_call_api():
    """API 호출 가능 여부"""
    return sys.platform == "win32" or os.environ.get("RAILWAY_ENVIRONMENT")
