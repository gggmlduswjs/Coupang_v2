"""상품 관리 페이지 — V4 (탭별 모듈 분할)"""
import streamlit as st
import pandas as pd

from dashboard.utils import (
    query_df, query_df_cached, create_wing_client, fmt_money_df,
    CoupangWingError,
)
from dashboard.pages.products_list import render_tab_list
from dashboard.pages.products_inventory import render_tab_inventory
from dashboard.pages.products_register import render_tab_register
from dashboard.pages.products_manual import render_tab_manual


def render(selected_account, accounts_df, account_names):
    """상품 관리 페이지 렌더링"""
    st.title("상품 관리")

    # ── 전체 요약 KPI ──
    _kpi = query_df("""
        SELECT
            COUNT(*) FILTER (WHERE coupang_status = 'active') as active_cnt,
            COUNT(*) FILTER (WHERE coupang_status != 'active') as other_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'active' AND stock_quantity <= 3) as low_stock_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'active'
                              AND sale_price > 0 AND original_price > 0
                              AND sale_price > original_price) as price_over_cnt
        FROM listings
    """)
    _pub_df = query_df_cached("SELECT COUNT(*) as c FROM publishers WHERE is_active = true")
    _pub_cnt = int(_pub_df.iloc[0]['c']) if not _pub_df.empty else 0
    _all_active = int(_kpi.iloc[0]['active_cnt']) if not _kpi.empty else 0
    _all_other = int(_kpi.iloc[0]['other_cnt']) if not _kpi.empty else 0
    _low_stock_cnt = int(_kpi.iloc[0]['low_stock_cnt']) if not _kpi.empty else 0
    _price_over_cnt = int(_kpi.iloc[0]['price_over_cnt']) if not _kpi.empty else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("전체 판매중", f"{_all_active:,}개")
    c2.metric("전체 기타", f"{_all_other:,}개")
    c3.metric("출판사", f"{_pub_cnt}개")
    c4.metric("재고 부족", f"{_low_stock_cnt}건",
              delta=f"-{_low_stock_cnt}" if _low_stock_cnt > 0 else None,
              delta_color="inverse")
    c5.metric("정가초과", f"{_price_over_cnt}건",
              delta=f"⚠ {_price_over_cnt}" if _price_over_cnt > 0 else "정상",
              delta_color="inverse" if _price_over_cnt > 0 else "normal")

    # ── 계정별 요약 테이블 ──
    acct_sum = query_df("""
        SELECT a.account_name as 계정,
               COUNT(l.id) as 전체,
               SUM(CASE WHEN l.coupang_status = 'active' THEN 1 ELSE 0 END) as 판매중,
               SUM(CASE WHEN l.coupang_status != 'active' THEN 1 ELSE 0 END) as 기타
        FROM accounts a
        LEFT JOIN listings l ON a.id = l.account_id
        WHERE a.is_active = true
        GROUP BY a.id, a.account_name ORDER BY a.account_name
    """)
    if not acct_sum.empty:
        st.dataframe(acct_sum, use_container_width=True, hide_index=True)

    with st.expander("출판사별 도서 수 (TOP 10)"):
        pub_df = query_df("""
            SELECT p.name as 출판사, p.margin_rate as "매입율(%)",
                   COUNT(b.id) as 도서수,
                   COALESCE(ROUND(AVG(pr.net_margin)), 0) as "평균마진(원)"
            FROM publishers p
            LEFT JOIN books b ON p.id = b.publisher_id
            LEFT JOIN products pr ON b.id = pr.book_id
            WHERE p.is_active = true GROUP BY p.id HAVING COUNT(b.id) > 0
            ORDER BY COUNT(b.id) DESC LIMIT 10
        """)
        if not pub_df.empty:
            st.dataframe(fmt_money_df(pub_df), use_container_width=True, hide_index=True)

    st.divider()

    # ── 계정 선택 (전체 요약 아래) ──
    _prod_c1, _prod_c2 = st.columns([3, 7])
    with _prod_c1:
        _prod_acct_name = st.selectbox(
            "계정 선택", account_names,
            index=0 if account_names else None,
            key="prod_account",
        )
    selected_account = None
    if _prod_acct_name and not accounts_df.empty:
        mask = accounts_df["account_name"] == _prod_acct_name
        if mask.any():
            selected_account = accounts_df[mask].iloc[0]
    selected_account_name = selected_account["account_name"] if selected_account is not None else None

    # ── 계정 필요 ──
    if selected_account is None:
        st.info("계정을 선택하면 상세 조회할 수 있습니다.")
        st.stop()

    account_id = int(selected_account["id"])

    # ── 선택 계정 KPI + WING 현황 ──
    _wing_client = create_wing_client(selected_account)

    _acct_kpi = query_df("""
        SELECT
            COUNT(*) FILTER (WHERE coupang_status = 'active') as active_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'paused') as paused_cnt,
            COUNT(*) FILTER (WHERE coupang_status NOT IN ('active','paused')) as other_cnt,
            COUNT(*) as total_cnt
        FROM listings WHERE account_id = :aid
    """, {"aid": account_id})

    _a_active = int(_acct_kpi.iloc[0]['active_cnt']) if not _acct_kpi.empty else 0
    _a_paused = int(_acct_kpi.iloc[0]['paused_cnt']) if not _acct_kpi.empty else 0
    _a_other  = int(_acct_kpi.iloc[0]['other_cnt'])  if not _acct_kpi.empty else 0
    _a_total  = int(_acct_kpi.iloc[0]['total_cnt'])  if not _acct_kpi.empty else 0

    ka1, ka2, ka3, ka4 = st.columns(4)
    ka1.metric(f"{selected_account_name} 판매중", f"{_a_active:,}개")
    ka2.metric("판매중지", f"{_a_paused:,}개")
    ka3.metric("기타", f"{_a_other:,}개")
    ka4.metric("전체", f"{_a_total:,}개")

    if _wing_client is not None:
        try:
            @st.cache_data(ttl=60)
            def _fetch_inflow_status(_vendor_id):
                _c = create_wing_client(selected_account)
                if _c is None:
                    return None
                return _c.get_inflow_status()

            _inflow = _fetch_inflow_status(selected_account.get("vendor_id", ""))
            if _inflow and isinstance(_inflow, dict):
                _inflow_data = _inflow.get("data", _inflow)
                _registered = _inflow_data.get("registeredCount", "-")
                _permitted = _inflow_data.get("permittedCount", "-")
                _restricted = _inflow_data.get("restricted", False)
                _iw1, _iw2, _iw3 = st.columns(3)
                _iw1.metric("WING 등록 상품", f"{_registered:,}건" if isinstance(_registered, int) else f"{_registered}건")
                _iw2.metric("등록 한도", f"{_permitted:,}건" if isinstance(_permitted, int) and _permitted < 2_000_000_000 else "무제한")
                _iw3.metric("판매 제한", "제한됨" if _restricted else "정상")
        except CoupangWingError as e:
            st.caption(f"WING 등록현황 조회 실패: {e.message}")
        except Exception:
            pass

    st.divider()

    # ═══ 4개 탭 ═══
    pm_tab1, pm_tab2, pm_tab3, pm_tab4 = st.tabs(["상품 목록", "가격/재고", "신규 등록", "수동 등록"])

    with pm_tab1:
        render_tab_list(account_id, selected_account, accounts_df, _wing_client)
    with pm_tab2:
        render_tab_inventory(account_id, selected_account, accounts_df, _wing_client)
    with pm_tab3:
        render_tab_register(account_id, selected_account, accounts_df, _wing_client)
    with pm_tab4:
        render_tab_manual(account_id, selected_account, accounts_df, _wing_client)
