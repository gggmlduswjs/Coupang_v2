"""상품 관리 — Tab 2: 가격/재고 관리"""
import logging
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from dashboard.utils import (
    query_df, run_sql, create_wing_client, CoupangWingError,
)

logger = logging.getLogger(__name__)


def render_tab_inventory(account_id, selected_account, accounts_df, _wing_client):
    """Tab 2: 가격/재고 관리 렌더링"""
    account_names = accounts_df["account_name"].tolist()
    st.subheader("가격/재고 관리")

    _inv_tab1, _inv_tab2, _inv_tab3 = st.tabs(["가격 불일치", "재고 부족", "동기화"])

    # ── 탭1: 가격 불일치 ──
    with _inv_tab1:
        _price_diff_df = query_df("""
            SELECT l.id, COALESCE(l.product_name, '(미등록)') as 상품명,
                   p.sale_price as 기준가, l.sale_price as 쿠팡가,
                   (p.sale_price - l.sale_price) as 차이,
                   COALESCE(CAST(l.vendor_item_id AS TEXT), '') as "VID",
                   l.isbn as "ISBN"
            FROM listings l
            JOIN products p ON l.product_id = p.id
            WHERE l.account_id = :acct_id
              AND l.coupang_status = 'active'
              AND l.sale_price > 0 AND p.sale_price > 0
              AND l.sale_price != p.sale_price
            ORDER BY ABS(p.sale_price - l.sale_price) DESC
        """, {"acct_id": account_id})

        if not _price_diff_df.empty:
            st.caption(f"{len(_price_diff_df)}건의 가격 불일치 발견")
            _pd_gb = GridOptionsBuilder.from_dataframe(_price_diff_df[["상품명", "기준가", "쿠팡가", "차이", "VID"]])
            _pd_gb.configure_selection(selection_mode="multiple", use_checkbox=True)
            _pd_gb.configure_column("상품명", headerCheckboxSelection=True)
            _pd_gb.configure_grid_options(domLayout="normal")
            _pd_grid = AgGrid(
                _price_diff_df[["상품명", "기준가", "쿠팡가", "차이", "VID"]],
                gridOptions=_pd_gb.build(),
                update_on=["selectionChanged"],
                height=300,
                theme="streamlit",
                key="pd_aggrid",
            )
            _pd_selected = _pd_grid["selected_rows"]
            _pd_sel_list = []
            if _pd_selected is not None and len(_pd_selected) > 0:
                _pd_sel_df = _pd_selected if isinstance(_pd_selected, pd.DataFrame) else pd.DataFrame(_pd_selected)
                _pd_sel_list = _pd_sel_df.to_dict("records")

            if _pd_sel_list:
                _pd_confirm = st.checkbox("가격 일괄 수정을 확인합니다", key="pd_confirm")
                if st.button(f"선택 {len(_pd_sel_list)}건 가격 수정 (기준가로)", type="primary", disabled=not _pd_confirm, key="btn_fix_price", use_container_width=True):
                    if _wing_client:
                        _pd_prog = st.progress(0, text="가격 수정 중...")
                        _pd_ok, _pd_fail = 0, 0
                        for _pi, _pr in enumerate(_pd_sel_list):
                            _pd_prog.progress((_pi + 1) / len(_pd_sel_list), text=f"[{_pi+1}/{len(_pd_sel_list)}] {str(_pr.get('상품명', ''))[:30]}...")
                            _pr_vid = str(_pr.get("VID", ""))
                            if not _pr_vid:
                                _pd_fail += 1
                                continue
                            _pr_match = _price_diff_df[_price_diff_df["VID"] == _pr_vid]
                            _pr_target = int(_pr_match.iloc[0]["기준가"]) if not _pr_match.empty else int(_pr.get("기준가", 0))
                            try:
                                _wing_client.update_price(int(_pr_vid), _pr_target, dashboard_override=True)
                                run_sql("UPDATE listings SET sale_price=:sp WHERE account_id=:aid AND vendor_item_id=:vid",
                                        {"sp": _pr_target, "aid": account_id, "vid": _pr_vid})
                                _pd_ok += 1
                            except CoupangWingError as e:
                                _pd_fail += 1
                                logger.warning(f"가격 수정 실패 VID={_pr_vid}: {e.message}")
                        _pd_prog.progress(1.0, text="완료!")
                        st.success(f"가격 수정 완료: 성공 {_pd_ok}건, 실패 {_pd_fail}건")
                        query_df.clear()
                        st.rerun()
                    else:
                        st.error("API 키가 설정되지 않았습니다.")
        else:
            st.success("가격 불일치 없음")

    # ── 탭2: 재고 부족 ──
    with _inv_tab2:
        _low_stock_df = query_df("""
            SELECT l.id, COALESCE(l.product_name, '(미등록)') as 상품명,
                   COALESCE(l.stock_quantity, 0) as 현재재고,
                   COALESCE(CAST(l.vendor_item_id AS TEXT), '') as "VID",
                   l.isbn as "ISBN"
            FROM listings l
            WHERE l.account_id = :acct_id
              AND l.coupang_status = 'active'
              AND COALESCE(l.stock_quantity, 0) <= 3
            ORDER BY l.stock_quantity ASC
        """, {"acct_id": account_id})

        if not _low_stock_df.empty:
            st.caption(f"{len(_low_stock_df)}건의 재고 부족 (3개 이하)")
            _ls_gb = GridOptionsBuilder.from_dataframe(_low_stock_df[["상품명", "현재재고", "VID"]])
            _ls_gb.configure_selection(selection_mode="multiple", use_checkbox=True)
            _ls_gb.configure_column("상품명", headerCheckboxSelection=True)
            _ls_gb.configure_grid_options(domLayout="normal")
            _ls_grid = AgGrid(
                _low_stock_df[["상품명", "현재재고", "VID"]],
                gridOptions=_ls_gb.build(),
                update_on=["selectionChanged"],
                height=300,
                theme="streamlit",
                key="ls_aggrid",
            )
            _ls_selected = _ls_grid["selected_rows"]
            _ls_sel_list = []
            if _ls_selected is not None and len(_ls_selected) > 0:
                _ls_sel_df = _ls_selected if isinstance(_ls_selected, pd.DataFrame) else pd.DataFrame(_ls_selected)
                _ls_sel_list = _ls_sel_df.to_dict("records")

            _refill_qty_col, _ = st.columns([1, 3])
            with _refill_qty_col:
                _refill_qty = st.number_input("리필 수량", value=10, min_value=1, max_value=999, key="refill_qty")

            if _ls_sel_list:
                _ls_confirm = st.checkbox("재고 일괄 리필을 확인합니다", key="ls_confirm")
                if st.button(f"선택 {len(_ls_sel_list)}건 재고 리필 ({_refill_qty}개)", type="primary", disabled=not _ls_confirm, key="btn_refill", use_container_width=True):
                    if _wing_client:
                        _ls_prog = st.progress(0, text="재고 리필 중...")
                        _ls_ok, _ls_fail = 0, 0
                        for _li, _lr in enumerate(_ls_sel_list):
                            _ls_prog.progress((_li + 1) / len(_ls_sel_list), text=f"[{_li+1}/{len(_ls_sel_list)}] {str(_lr.get('상품명', ''))[:30]}...")
                            _lr_vid = str(_lr.get("VID", ""))
                            if not _lr_vid:
                                _ls_fail += 1
                                continue
                            try:
                                _wing_client.update_quantity(int(_lr_vid), _refill_qty)
                                run_sql("UPDATE listings SET stock_quantity=:qty WHERE account_id=:aid AND vendor_item_id=:vid",
                                        {"qty": _refill_qty, "aid": account_id, "vid": _lr_vid})
                                _ls_ok += 1
                            except CoupangWingError as e:
                                _ls_fail += 1
                                logger.warning(f"재고 리필 실패 VID={_lr_vid}: {e.message}")
                        _ls_prog.progress(1.0, text="완료!")
                        st.success(f"재고 리필 완료: 성공 {_ls_ok}건, 실패 {_ls_fail}건")
                        query_df.clear()
                        st.rerun()
                    else:
                        st.error("API 키가 설정되지 않았습니다.")
        else:
            st.success("재고 부족 상품 없음")

    # ── 탭3: 동기화 ──
    with _inv_tab3:
        _sync_col1, _sync_col2 = st.columns([2, 1])
        with _sync_col1:
            _inv_acct = st.selectbox("동기화 계정", ["전체"] + account_names, key="inv_acct")
        with _sync_col2:
            _inv_dry = st.checkbox("Dry Run", value=True, key="inv_dry", help="실제 변경 없이 확인만")
        _btn_inv_sync = st.button("가격/재고 동기화 실행", type="primary", key="btn_inv_sync", use_container_width=True)

        if _btn_inv_sync:
            st.warning("가격/재고 동기화 기능 준비 중")
