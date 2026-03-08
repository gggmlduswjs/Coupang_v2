"""상품등록 페이지 — 신규 등록 + 수동 등록 탭"""
import streamlit as st

from dashboard.utils import (
    query_df, query_df_cached, create_wing_client, fmt_money_df,
    CoupangWingError,
)
from dashboard.pages.products_register import render_tab_register
from dashboard.pages.products_manual import render_tab_manual


def render(selected_account, accounts_df, account_names):
    """상품등록 페이지 렌더링"""
    st.title("상품등록")

    # ── 계정 선택 ──
    _c1, _c2 = st.columns([3, 7])
    with _c1:
        _acct_name = st.selectbox(
            "계정 선택", account_names,
            index=0 if account_names else None,
            key="reg_account",
        )
    selected_account = None
    if _acct_name and not accounts_df.empty:
        mask = accounts_df["account_name"] == _acct_name
        if mask.any():
            selected_account = accounts_df[mask].iloc[0]

    if selected_account is None:
        st.info("계정을 선택하면 상품을 등록할 수 있습니다.")
        st.stop()

    account_id = int(selected_account["id"])
    _wing_client = create_wing_client(selected_account)

    # ── 선택 계정 KPI ──
    _acct_kpi = query_df("""
        SELECT
            COUNT(*) FILTER (WHERE coupang_status = 'active') as active_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'paused') as paused_cnt,
            COUNT(*) FILTER (WHERE coupang_status NOT IN ('active','paused')) as other_cnt,
            COUNT(*) as total_cnt
        FROM listings WHERE account_id = :aid
    """, {"aid": account_id})

    if not _acct_kpi.empty:
        r = _acct_kpi.iloc[0]
        ka1, ka2, ka3, ka4 = st.columns(4)
        ka1.metric(f"{_acct_name} 판매중", f"{int(r['active_cnt']):,}개")
        ka2.metric("판매중지", f"{int(r['paused_cnt']):,}개")
        ka3.metric("기타", f"{int(r['other_cnt']):,}개")
        ka4.metric("전체", f"{int(r['total_cnt']):,}개")

    if _wing_client is not None:
        try:
            @st.cache_data(ttl=60)
            def _fetch_inflow(_vendor_id, _acct_name):
                _mask = accounts_df["account_name"] == _acct_name
                if not _mask.any():
                    return None
                _c = create_wing_client(accounts_df[_mask].iloc[0])
                if _c is None:
                    return None
                return _c.get_inflow_status()

            _inflow = _fetch_inflow(selected_account.get("vendor_id", ""), _acct_name)
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

    # ── 2개 탭: 신규 등록 / 수동 등록 ──
    reg_tab1, reg_tab2 = st.tabs(["신규 등록", "수동 등록"])

    with reg_tab1:
        render_tab_register(account_id, selected_account, accounts_df, _wing_client)
    with reg_tab2:
        render_tab_manual(account_id, selected_account, accounts_df, _wing_client)
