"""
반품 관리 페이지
================
반품 목록 조회, 반품 동기화, 입고 확인 및 반품 승인 처리.
"""

from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import text, inspect as sa_inspect
from st_aggrid import AgGrid, GridOptionsBuilder

from core.api.wing_client import CoupangWingError
from dashboard.utils import (
    query_df,
    run_sql,
    create_wing_client,
    fmt_krw,
    fmt_money_df,
    render_grid,
    engine,
)


def render(selected_account, accounts_df, account_names):
    st.title("반품 관리")

    # ── 상단 컨트롤 ──
    _ret_ctrl1, _ret_ctrl2, _ret_ctrl3, _ret_ctrl4 = st.columns([2, 2, 2, 2])
    with _ret_ctrl1:
        _ret_acct = st.selectbox("계정", ["전체"] + account_names, key="ret_acct")
    with _ret_ctrl2:
        _ret_period = st.selectbox("기간", ["7일", "14일", "30일", "60일", "90일"], index=2, key="ret_period")
    with _ret_ctrl3:
        _ret_status_filter = st.selectbox("상태", [
            "전체", "RELEASE_STOP_UNCHECKED", "RETURNS_UNCHECKED",
            "VENDOR_WAREHOUSE_CONFIRM", "REQUEST_COUPANG_CHECK", "RETURNS_COMPLETED"
        ], key="ret_status")
    with _ret_ctrl4:
        st.markdown("<br>", unsafe_allow_html=True)
        _btn_ret_sync = st.button("반품 동기화", type="primary", key="btn_ret_sync", use_container_width=True)

    # 기간 계산
    _ret_days = int(_ret_period.replace("일", ""))
    _ret_date_to = date.today()
    _ret_date_from = _ret_date_to - timedelta(days=_ret_days)
    _ret_date_from_str = _ret_date_from.isoformat()
    _ret_date_to_str = _ret_date_to.isoformat()

    # WHERE 절 구성
    _ret_acct_where = ""
    _ret_acct_params = {}
    if _ret_acct != "전체":
        _ret_acct_where = "AND r.account_id = (SELECT id FROM accounts WHERE account_name = :acct_name)"
        _ret_acct_params["acct_name"] = _ret_acct

    _ret_status_where = ""
    if _ret_status_filter != "전체":
        _ret_status_where = f"AND r.receipt_status = '{_ret_status_filter}'"

    _ret_date_where = f"AND r.created_at_api >= '{_ret_date_from_str}' AND r.created_at_api <= '{_ret_date_to_str} 23:59:59'"

    # 동기화 실행
    if _btn_ret_sync:
        with st.spinner("반품 데이터 동기화 중..."):
            try:
                from scripts.sync.sync_returns import ReturnSync
                _ret_syncer = ReturnSync()
                _sync_acct = _ret_acct if _ret_acct != "전체" else None
                _ret_progress = st.progress(0, text="동기화 시작...")
                def _ret_progress_cb(current, total, msg):
                    if total > 0:
                        _ret_progress.progress(min(current / total, 1.0), text=msg)
                _ret_results = _ret_syncer.sync_all(
                    days=_ret_days,
                    account_name=_sync_acct,
                    progress_callback=_ret_progress_cb,
                )
                _total_f = sum(r["fetched"] for r in _ret_results)
                _total_u = sum(r["upserted"] for r in _ret_results)
                st.success(f"동기화 완료! 조회 {_total_f:,}건, 저장 {_total_u:,}건")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"동기화 오류: {e}")

    # ── 테이블 존재 확인 ──
    _ret_table_exists = False
    try:
        _ret_table_exists = sa_inspect(engine).has_table("return_requests")
    except Exception:
        pass

    if not _ret_table_exists:
        st.info("return_requests 테이블이 없습니다. '반품 동기화' 버튼을 눌러 데이터를 가져오세요.")
    else:
        # ── KPI 카드 ──
        _ret_kpi_base = f"""
            FROM return_requests r
            WHERE 1=1 {_ret_acct_where} {_ret_date_where}
        """

        def _cnt(df): return int(df.iloc[0]["c"]) if not df.empty else 0
        _ret_total     = _cnt(query_df(f"SELECT COUNT(*) as c {_ret_kpi_base}", _ret_acct_params))
        _ret_pending   = _cnt(query_df(f"SELECT COUNT(*) as c {_ret_kpi_base} AND r.receipt_status IN ('RELEASE_STOP_UNCHECKED', 'RETURNS_UNCHECKED')", _ret_acct_params))
        _ret_completed = _cnt(query_df(f"SELECT COUNT(*) as c {_ret_kpi_base} AND r.receipt_status = 'RETURNS_COMPLETED'", _ret_acct_params))

        # 귀책 비율
        _ret_fault = query_df(f"""
            SELECT
                SUM(CASE WHEN r.fault_by_type IN ('CUSTOMER') THEN 1 ELSE 0 END) as 고객귀책,
                SUM(CASE WHEN r.fault_by_type IN ('VENDOR') THEN 1 ELSE 0 END) as 셀러귀책,
                COUNT(*) as 전체
            {_ret_kpi_base}
        """, _ret_acct_params)
        _ret_customer_fault = int(_ret_fault.iloc[0]["고객귀책"] or 0) if not _ret_fault.empty else 0
        _ret_vendor_fault = int(_ret_fault.iloc[0]["셀러귀책"] or 0) if not _ret_fault.empty else 0
        _ret_fault_total = _ret_customer_fault + _ret_vendor_fault
        _ret_fault_text = f"고객 {_ret_customer_fault} / 셀러 {_ret_vendor_fault}" if _ret_fault_total > 0 else "-"

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("총 반품/취소", f"{_ret_total:,}건")
        k2.metric("미처리 건수", f"{_ret_pending:,}건")
        k3.metric("처리완료", f"{_ret_completed:,}건")
        k4.metric("귀책 (고객/셀러)", _ret_fault_text)

        st.divider()

        # ── 일별 추이 차트 ──
        _ret_daily = query_df(f"""
            SELECT DATE(r.created_at_api) as 날짜,
                   COUNT(*) as 건수,
                   COALESCE(SUM(r.return_shipping_charge), 0) as 배송비부담
            {_ret_kpi_base}
            GROUP BY DATE(r.created_at_api)
            ORDER BY 날짜
        """, _ret_acct_params)

        if not _ret_daily.empty:
            _ret_fig = make_subplots(specs=[[{"secondary_y": True}]])
            _ret_fig.add_trace(
                go.Bar(x=_ret_daily["날짜"], y=_ret_daily["건수"], name="반품 건수", marker_color="#EF553B"),
                secondary_y=False,
            )
            _ret_fig.add_trace(
                go.Scatter(x=_ret_daily["날짜"], y=_ret_daily["배송비부담"], name="배송비 부담액",
                           line=dict(color="#636EFA", width=2)),
                secondary_y=True,
            )
            _ret_fig.update_layout(
                title="일별 반품 추이",
                height=350,
                margin=dict(l=20, r=20, t=40, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            _ret_fig.update_yaxes(title_text="건수", secondary_y=False)
            _ret_fig.update_yaxes(title_text="배송비 (원)", secondary_y=True)
            st.plotly_chart(_ret_fig, use_container_width=True)

        st.divider()

        # ── 탭 ──
        _ret_tab1, _ret_tab2 = st.tabs(["반품 목록", "반품 처리"])

        # ── 탭1: 반품 목록 ──
        with _ret_tab1:
            _ret_list = query_df(f"""
                SELECT
                    a.account_name as 계정,
                    r.receipt_id as 접수번호,
                    r.order_id as 주문번호,
                    r.receipt_type as 유형,
                    r.receipt_status as 상태,
                    DATE(r.created_at_api) as 접수일,
                    r.cancel_reason_category1 as 사유분류,
                    r.cancel_reason as 사유,
                    r.cancel_count_sum as 수량,
                    COALESCE(r.return_shipping_charge, 0) as 배송비,
                    r.fault_by_type as 귀책,
                    r.requester_name as 요청자
                FROM return_requests r
                JOIN accounts a ON r.account_id = a.id
                WHERE 1=1 {_ret_acct_where} {_ret_status_where} {_ret_date_where}
                ORDER BY r.created_at_api DESC
                LIMIT 500
            """, _ret_acct_params)

            if _ret_list.empty:
                st.info("해당 조건의 반품/취소 건이 없습니다.")
            else:
                # 상태 한글 매핑
                _ret_status_map = {
                    "RELEASE_STOP_UNCHECKED": "출고중지요청",
                    "RETURNS_UNCHECKED": "반품접수(미확인)",
                    "VENDOR_WAREHOUSE_CONFIRM": "입고확인",
                    "REQUEST_COUPANG_CHECK": "쿠팡확인요청",
                    "RETURNS_COMPLETED": "반품완료",
                }
                _ret_list["상태"] = _ret_list["상태"].map(lambda x: _ret_status_map.get(x, x))

                _ret_type_map = {"RETURN": "반품", "CANCEL": "취소"}
                _ret_list["유형"] = _ret_list["유형"].map(lambda x: _ret_type_map.get(x, x))

                _ret_fault_map = {
                    "CUSTOMER": "고객", "VENDOR": "셀러", "COUPANG": "쿠팡",
                    "WMS": "WMS", "GENERAL": "일반",
                }
                _ret_list["귀책"] = _ret_list["귀책"].map(lambda x: _ret_fault_map.get(x, x) if x else "-")

                if "배송비" in _ret_list.columns:
                    _ret_list["배송비"] = _ret_list["배송비"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

                gb = GridOptionsBuilder.from_dataframe(_ret_list)
                gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
                gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
                gb.configure_column("사유", width=250)
                grid_opts = gb.build()
                AgGrid(_ret_list, gridOptions=grid_opts, height=500, theme="streamlit")

                st.download_button(
                    "CSV 다운로드",
                    _ret_list.to_csv(index=False, encoding="utf-8-sig"),
                    file_name=f"returns_{_ret_date_from_str}_{_ret_date_to_str}.csv",
                    mime="text/csv",
                    key="ret_csv_dl",
                )

        # ── 탭2: 반품 처리 ──
        with _ret_tab2:
            if selected_account is None:
                st.warning("사이드바에서 계정을 선택하세요.")
            else:
                _ret_mgmt_aid = int(selected_account["id"])
                _ret_mgmt_client = create_wing_client(selected_account)

                # 미처리 반품 목록
                st.subheader("입고 확인 대기")
                st.caption("RETURNS_UNCHECKED 상태의 반품에 대해 입고 확인 처리합니다.")

                _ret_unchecked = query_df("""
                    SELECT r.receipt_id as 접수번호,
                           r.order_id as 주문번호,
                           r.receipt_type as 유형,
                           r.cancel_reason_category1 as 사유,
                           r.cancel_count_sum as 수량,
                           r.fault_by_type as 귀책,
                           DATE(r.created_at_api) as 접수일
                    FROM return_requests r
                    WHERE r.account_id = :aid
                          AND r.receipt_status = 'RETURNS_UNCHECKED'
                    ORDER BY r.created_at_api
                """, {"aid": _ret_mgmt_aid})

                if _ret_unchecked.empty:
                    st.info("입고 확인 대기 중인 반품이 없습니다.")
                else:
                    st.dataframe(_ret_unchecked, use_container_width=True, hide_index=True)

                    _ret_confirm_col1, _ret_confirm_col2 = st.columns(2)
                    with _ret_confirm_col1:
                        _sel_receipt_confirm = st.selectbox(
                            "접수번호 선택 (입고확인)",
                            _ret_unchecked["접수번호"].tolist(),
                            key="sel_receipt_confirm"
                        )
                    with _ret_confirm_col2:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button("입고 확인", type="primary", key="btn_confirm_receipt"):
                            if _ret_mgmt_client:
                                try:
                                    _ret_mgmt_client.confirm_return_receipt(int(_sel_receipt_confirm))
                                    # DB 상태 업데이트
                                    with engine.connect() as conn:
                                        conn.execute(text(
                                            "UPDATE return_requests SET receipt_status = 'VENDOR_WAREHOUSE_CONFIRM', updated_at = :now WHERE account_id = :aid AND receipt_id = :rid"
                                        ), {"now": datetime.utcnow().isoformat(), "aid": _ret_mgmt_aid, "rid": int(_sel_receipt_confirm)})
                                        conn.commit()
                                    st.success(f"입고 확인 완료: 접수번호 {_sel_receipt_confirm}")
                                    st.cache_data.clear()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("WING API 클라이언트를 생성할 수 없습니다.")

                st.divider()

                # 반품 승인 대기
                st.subheader("반품 승인 대기")
                st.caption("VENDOR_WAREHOUSE_CONFIRM 상태의 반품을 승인 처리합니다.")

                _ret_confirm_list = query_df("""
                    SELECT r.receipt_id as 접수번호,
                           r.order_id as 주문번호,
                           r.receipt_type as 유형,
                           r.cancel_reason_category1 as 사유,
                           r.cancel_count_sum as 수량,
                           r.fault_by_type as 귀책,
                           DATE(r.created_at_api) as 접수일
                    FROM return_requests r
                    WHERE r.account_id = :aid
                          AND r.receipt_status = 'VENDOR_WAREHOUSE_CONFIRM'
                    ORDER BY r.created_at_api
                """, {"aid": _ret_mgmt_aid})

                if _ret_confirm_list.empty:
                    st.info("승인 대기 중인 반품이 없습니다.")
                else:
                    st.dataframe(_ret_confirm_list, use_container_width=True, hide_index=True)

                    _ret_approve_col1, _ret_approve_col2 = st.columns(2)
                    with _ret_approve_col1:
                        _sel_receipt_approve = st.selectbox(
                            "접수번호 선택 (승인)",
                            _ret_confirm_list["접수번호"].tolist(),
                            key="sel_receipt_approve"
                        )
                    with _ret_approve_col2:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button("반품 승인", type="primary", key="btn_approve_return"):
                            if _ret_mgmt_client:
                                try:
                                    _ret_mgmt_client.approve_return_request(int(_sel_receipt_approve))
                                    with engine.connect() as conn:
                                        conn.execute(text(
                                            "UPDATE return_requests SET receipt_status = 'RETURNS_COMPLETED', updated_at = :now WHERE account_id = :aid AND receipt_id = :rid"
                                        ), {"now": datetime.utcnow().isoformat(), "aid": _ret_mgmt_aid, "rid": int(_sel_receipt_approve)})
                                        conn.commit()
                                    st.success(f"반품 승인 완료: 접수번호 {_sel_receipt_approve}")
                                    st.cache_data.clear()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("WING API 클라이언트를 생성할 수 없습니다.")
