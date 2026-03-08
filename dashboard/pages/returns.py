"""
반품 관리 페이지
================
반품/취소 목록 조회, 동기화, 상세 조회, 입고 확인, 반품 승인, 회수 송장 등록.
"""

import json
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

# ── 상수 ──
_STATUS_MAP = {
    "RELEASE_STOP_UNCHECKED": "출고중지요청",
    "RETURNS_UNCHECKED": "반품접수",
    "VENDOR_WAREHOUSE_CONFIRM": "입고확인",
    "REQUEST_COUPANG_CHECK": "쿠팡확인요청",
    "RETURNS_COMPLETED": "반품완료",
}

_TYPE_MAP = {"RETURN": "반품", "CANCEL": "취소"}

_FAULT_MAP = {
    "CUSTOMER": "고객", "VENDOR": "셀러", "COUPANG": "쿠팡",
    "WMS": "WMS", "GENERAL": "일반",
}

_RELEASE_STATUS_MAP = {
    "Y": "출고됨", "N": "미출고", "S": "출고중지됨", "A": "이미출고됨",
}

_DELIVERY_COMPANIES = {
    "CJGLS": "CJ대한통운",
    "EPOST": "우체국택배",
    "KDEXP": "경동택배",
    "HANJIN": "한진택배",
    "LOTTE": "롯데택배",
    "LOGEN": "로젠택배",
    "DAESIN": "대신택배",
    "ILYANG": "일양로지스",
    "CHUNIL": "천일택배",
    "DIRECT": "직접배송",
}


def render(selected_account, accounts_df, account_names):
    st.title("반품 관리")

    # ── 상단 컨트롤 ──
    c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 2])
    with c1:
        _ret_accts = st.multiselect("계정", account_names, default=account_names, key="ret_accts")
    with c2:
        _ret_period = st.selectbox("기간", ["7일", "14일", "30일", "60일", "90일"], index=2, key="ret_period")
    with c3:
        _ret_status_filter = st.selectbox("상태", [
            "전체", "출고중지요청", "반품접수", "입고확인", "쿠팡확인요청", "반품완료"
        ], key="ret_status")
    with c4:
        _ret_type_filter = st.selectbox("유형", ["전체", "반품", "취소"], key="ret_type")
    with c5:
        st.markdown("<br>", unsafe_allow_html=True)
        _btn_sync = st.button("반품 동기화", type="primary", key="btn_ret_sync", use_container_width=True)

    # 기간 계산
    _days = int(_ret_period.replace("일", ""))
    _date_to = date.today()
    _date_from = _date_to - timedelta(days=_days)

    # 한글 → 영문 역매핑
    _status_reverse = {v: k for k, v in _STATUS_MAP.items()}
    _type_reverse = {v: k for k, v in _TYPE_MAP.items()}

    # WHERE 파라미터 구성 (parameterized)
    _params: dict = {
        "date_from": _date_from.isoformat(),
        "date_to": f"{_date_to.isoformat()} 23:59:59",
    }

    # 계정 필터
    _acct_where = ""
    if _ret_accts and len(_ret_accts) < len(account_names):
        _acct_placeholders = ", ".join(f":acct_{i}" for i in range(len(_ret_accts)))
        _acct_where = f"AND a.account_name IN ({_acct_placeholders})"
        for i, name in enumerate(_ret_accts):
            _params[f"acct_{i}"] = name

    # 상태 필터
    _status_where = ""
    if _ret_status_filter != "전체":
        _status_en = _status_reverse.get(_ret_status_filter, _ret_status_filter)
        _status_where = "AND r.receipt_status = :status_filter"
        _params["status_filter"] = _status_en

    # 유형 필터
    _type_where = ""
    if _ret_type_filter != "전체":
        _type_en = _type_reverse.get(_ret_type_filter, _ret_type_filter)
        _type_where = "AND r.receipt_type = :type_filter"
        _params["type_filter"] = _type_en

    _date_where = "AND r.created_at_api >= :date_from AND r.created_at_api <= :date_to"

    # 동기화 실행
    if _btn_sync:
        with st.spinner("반품 데이터 동기화 중..."):
            try:
                from scripts.sync.sync_returns import ReturnSync
                syncer = ReturnSync()
                _sync_acct = _ret_accts[0] if len(_ret_accts) == 1 else None
                _progress = st.progress(0, text="동기화 시작...")
                def _progress_cb(current, total, msg):
                    if total > 0:
                        _progress.progress(min(current / total, 1.0), text=msg)
                results = syncer.sync_all(
                    days=_days,
                    account_name=_sync_acct,
                    progress_callback=_progress_cb,
                )
                _total_f = sum(r["fetched"] for r in results)
                _total_u = sum(r["upserted"] for r in results)
                st.success(f"동기화 완료! 조회 {_total_f:,}건, 저장 {_total_u:,}건")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"동기화 오류: {e}")

    # ── 테이블 존재 확인 ──
    try:
        _table_exists = sa_inspect(engine).has_table("return_requests")
    except Exception:
        _table_exists = False

    if not _table_exists:
        st.info("return_requests 테이블이 없습니다. '반품 동기화' 버튼을 눌러 데이터를 가져오세요.")
        return

    # ── 공통 base ──
    _base = f"""
        FROM return_requests r
        JOIN accounts a ON r.account_id = a.id
        WHERE 1=1 {_acct_where} {_date_where}
    """
    _base_filtered = f"{_base} {_status_where} {_type_where}"

    # ── KPI 카드 ──
    def _cnt(df):
        return int(df.iloc[0]["c"]) if not df.empty else 0

    _total = _cnt(query_df(f"SELECT COUNT(*) as c {_base_filtered}", _params))
    _pending = _cnt(query_df(
        f"SELECT COUNT(*) as c {_base} AND r.receipt_status IN ('RELEASE_STOP_UNCHECKED', 'RETURNS_UNCHECKED')",
        _params
    ))
    _completed = _cnt(query_df(
        f"SELECT COUNT(*) as c {_base} AND r.receipt_status = 'RETURNS_COMPLETED'",
        _params
    ))
    _warehouse = _cnt(query_df(
        f"SELECT COUNT(*) as c {_base} AND r.receipt_status = 'VENDOR_WAREHOUSE_CONFIRM'",
        _params
    ))

    # 귀책 비율
    _fault_df = query_df(f"""
        SELECT
            SUM(CASE WHEN r.fault_by_type = 'CUSTOMER' THEN 1 ELSE 0 END) as 고객,
            SUM(CASE WHEN r.fault_by_type = 'VENDOR' THEN 1 ELSE 0 END) as 셀러,
            SUM(CASE WHEN r.fault_by_type = 'COUPANG' THEN 1 ELSE 0 END) as 쿠팡
        {_base}
    """, _params)
    _cust = int(_fault_df.iloc[0]["고객"] or 0) if not _fault_df.empty else 0
    _vendor = int(_fault_df.iloc[0]["셀러"] or 0) if not _fault_df.empty else 0
    _coupang = int(_fault_df.iloc[0]["쿠팡"] or 0) if not _fault_df.empty else 0

    # 반품배송비 합계
    _charge_df = query_df(f"""
        SELECT COALESCE(SUM(CASE WHEN r.return_shipping_charge > 0 THEN r.return_shipping_charge ELSE 0 END), 0) as 셀러부담,
               COALESCE(SUM(CASE WHEN r.return_shipping_charge < 0 THEN ABS(r.return_shipping_charge) ELSE 0 END), 0) as 고객부담
        {_base}
    """, _params)
    _seller_charge = int(_charge_df.iloc[0]["셀러부담"] or 0) if not _charge_df.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("총 반품/취소", f"{_total:,}건")
    k2.metric("미처리", f"{_pending:,}건")
    k3.metric("입고확인 대기", f"{_warehouse:,}건")
    k4.metric("처리완료", f"{_completed:,}건")
    _fault_text = f"고객 {_cust} / 셀러 {_vendor}" + (f" / 쿠팡 {_coupang}" if _coupang else "")
    k5.metric("귀책 분포", _fault_text if (_cust + _vendor + _coupang) > 0 else "-")

    if _seller_charge > 0:
        st.caption(f"셀러 부담 배송비 합계: {_seller_charge:,}원")

    st.divider()

    # ── 일별 추이 차트 ──
    _daily = query_df(f"""
        SELECT DATE(r.created_at_api) as 날짜,
               COUNT(*) as 건수,
               SUM(CASE WHEN r.receipt_type = 'RETURN' THEN 1 ELSE 0 END) as 반품,
               SUM(CASE WHEN r.receipt_type = 'CANCEL' THEN 1 ELSE 0 END) as 취소,
               COALESCE(SUM(r.return_shipping_charge), 0) as 배송비부담
        {_base}
        GROUP BY DATE(r.created_at_api)
        ORDER BY 날짜
    """, _params)

    if not _daily.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=_daily["날짜"], y=_daily["반품"], name="반품", marker_color="#EF553B"),
            secondary_y=False,
        )
        fig.add_trace(
            go.Bar(x=_daily["날짜"], y=_daily["취소"], name="취소", marker_color="#FFA15A"),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(x=_daily["날짜"], y=_daily["배송비부담"], name="배송비 부담액",
                       line=dict(color="#636EFA", width=2)),
            secondary_y=True,
        )
        fig.update_layout(
            title="일별 반품/취소 추이",
            barmode="stack",
            height=350,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig.update_yaxes(title_text="건수", secondary_y=False)
        fig.update_yaxes(title_text="배송비 (원)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── 탭 ──
    tab1, tab2, tab3 = st.tabs(["반품 목록", "반품 처리", "회수 송장 등록"])

    # ── 탭1: 반품 목록 ──
    with tab1:
        _list = query_df(f"""
            SELECT
                a.account_name as 계정,
                r.receipt_id as 접수번호,
                r.order_id as 주문번호,
                r.receipt_type as 유형,
                r.receipt_status as 상태,
                DATE(r.created_at_api) as 접수일,
                r.cancel_reason_category1 as 사유분류,
                r.cancel_reason_category2 as 사유상세,
                r.cancel_reason as 비고,
                r.cancel_count_sum as 수량,
                COALESCE(r.return_shipping_charge, 0) as 배송비,
                r.fault_by_type as 귀책,
                r.requester_name as 요청자,
                r.release_stop_status as 출고중지상태,
                r.return_delivery_type as 회수종류,
                CASE WHEN r.pre_refund THEN 'Y' ELSE 'N' END as 선환불,
                r.reason_code_text as 사유코드설명,
                r.return_items_json as items_json,
                r.return_delivery_json as delivery_json
            {_base_filtered}
            ORDER BY r.created_at_api DESC
            LIMIT 500
        """, _params)

        if _list.empty:
            st.info("해당 조건의 반품/취소 건이 없습니다.")
        else:
            # 한글 매핑
            _list["상태"] = _list["상태"].map(lambda x: _STATUS_MAP.get(x, x))
            _list["유형"] = _list["유형"].map(lambda x: _TYPE_MAP.get(x, x))
            _list["귀책"] = _list["귀책"].map(lambda x: _FAULT_MAP.get(x, x) if x else "-")
            _list["배송비"] = _list["배송비"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

            # 아이템에서 상품명 추출
            def _extract_item_names(json_str):
                if not json_str:
                    return ""
                try:
                    items = json.loads(json_str) if isinstance(json_str, str) else json_str
                    names = []
                    for item in (items if isinstance(items, list) else []):
                        name = item.get("sellerProductName") or item.get("vendorItemName", "")
                        if name:
                            names.append(name)
                    return " / ".join(names) if names else ""
                except Exception:
                    return ""

            _list["상품명"] = _list["items_json"].apply(_extract_item_names)

            # 그리드 표시 컬럼 선택
            _display_cols = [
                "계정", "접수번호", "주문번호", "유형", "상태", "접수일",
                "상품명", "사유분류", "수량", "배송비", "귀책", "요청자",
                "출고중지상태", "회수종류", "선환불",
            ]
            _grid_df = _list[_display_cols]

            gb = GridOptionsBuilder.from_dataframe(_grid_df)
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
            gb.configure_column("상품명", width=300)
            gb.configure_column("사유분류", width=150)
            gb.configure_column("접수번호", width=120)
            gb.configure_selection(selection_mode="single", use_checkbox=True)
            grid_opts = gb.build()
            grid_resp = AgGrid(_grid_df, gridOptions=grid_opts, height=500, theme="streamlit")

            # 선택된 행 상세 보기
            selected_rows = grid_resp.get("selected_rows")
            if selected_rows is not None and len(selected_rows) > 0:
                row = selected_rows.iloc[0] if isinstance(selected_rows, pd.DataFrame) else selected_rows[0]
                receipt_id = row["접수번호"]
                _detail_row = _list[_list["접수번호"] == receipt_id].iloc[0]

                with st.expander(f"상세 정보 — 접수번호 {receipt_id}", expanded=True):
                    dc1, dc2, dc3 = st.columns(3)
                    dc1.write(f"**주문번호:** {_detail_row['주문번호']}")
                    dc1.write(f"**유형:** {_detail_row['유형']}")
                    dc1.write(f"**상태:** {_detail_row['상태']}")
                    dc2.write(f"**요청자:** {_detail_row['요청자']}")
                    dc2.write(f"**귀책:** {_detail_row['귀책']}")
                    dc2.write(f"**선환불:** {_detail_row['선환불']}")
                    dc3.write(f"**출고중지상태:** {_detail_row.get('출고중지상태', '-')}")
                    dc3.write(f"**회수종류:** {_detail_row.get('회수종류', '-')}")
                    dc3.write(f"**배송비:** {_detail_row['배송비']}원")

                    # 사유 상세
                    st.write(f"**사유:** {_detail_row.get('사유분류', '')} > {_detail_row.get('사유상세', '')} | {_detail_row.get('사유코드설명', '')}")
                    if _detail_row.get("비고"):
                        st.write(f"**비고:** {_detail_row['비고']}")

                    # 반품 아이템 상세
                    if _detail_row.get("items_json"):
                        try:
                            items = json.loads(_detail_row["items_json"]) if isinstance(_detail_row["items_json"], str) else _detail_row["items_json"]
                            if items:
                                st.write("**반품 아이템:**")
                                item_rows = []
                                for item in items:
                                    item_rows.append({
                                        "상품명": item.get("sellerProductName", ""),
                                        "옵션명": item.get("vendorItemName", ""),
                                        "옵션ID": item.get("vendorItemId", ""),
                                        "주문수량": item.get("purchaseCount", 0),
                                        "취소수량": item.get("cancelCount", 0),
                                        "출고상태": _RELEASE_STATUS_MAP.get(item.get("releaseStatus", ""), item.get("releaseStatus", "")),
                                    })
                                st.dataframe(pd.DataFrame(item_rows), use_container_width=True, hide_index=True)
                        except Exception:
                            pass

                    # 회수 송장 정보
                    if _detail_row.get("delivery_json"):
                        try:
                            dtos = json.loads(_detail_row["delivery_json"]) if isinstance(_detail_row["delivery_json"], str) else _detail_row["delivery_json"]
                            valid_dtos = [d for d in (dtos or []) if d.get("deliveryInvoiceNo")]
                            if valid_dtos:
                                st.write("**회수 송장:**")
                                for d in valid_dtos:
                                    company = _DELIVERY_COMPANIES.get(d.get("deliveryCompanyCode", ""), d.get("deliveryCompanyCode", ""))
                                    st.write(f"  - {company}: {d['deliveryInvoiceNo']}")
                        except Exception:
                            pass

            # CSV 다운로드
            st.download_button(
                "CSV 다운로드",
                _grid_df.to_csv(index=False, encoding="utf-8-sig"),
                file_name=f"returns_{_date_from.isoformat()}_{_date_to.isoformat()}.csv",
                mime="text/csv",
                key="ret_csv_dl",
            )

    # ── 탭2: 반품 처리 ──
    with tab2:
        if selected_account is None:
            st.warning("사이드바에서 계정을 선택하세요.")
        else:
            _aid = int(selected_account["id"])
            _client = create_wing_client(selected_account)

            # ── 입고 확인 대기 ──
            st.subheader("입고 확인 대기")
            st.caption("반품접수(RETURNS_UNCHECKED) 상태 → 입고확인 처리. 빠른환불 대상이 아니거나 회수 송장 트래킹 불가 시 사용.")

            _unchecked = query_df("""
                SELECT r.receipt_id as 접수번호,
                       r.order_id as 주문번호,
                       r.receipt_type as 유형,
                       r.cancel_reason_category1 as 사유,
                       r.cancel_count_sum as 수량,
                       r.fault_by_type as 귀책,
                       CASE WHEN r.pre_refund THEN 'Y' ELSE 'N' END as 선환불,
                       r.return_delivery_type as 회수종류,
                       DATE(r.created_at_api) as 접수일
                FROM return_requests r
                WHERE r.account_id = :aid
                      AND r.receipt_status = 'RETURNS_UNCHECKED'
                ORDER BY r.created_at_api
            """, {"aid": _aid})

            if _unchecked.empty:
                st.info("입고 확인 대기 중인 반품이 없습니다.")
            else:
                _unchecked["유형"] = _unchecked["유형"].map(lambda x: _TYPE_MAP.get(x, x))
                _unchecked["귀책"] = _unchecked["귀책"].map(lambda x: _FAULT_MAP.get(x, x) if x else "-")
                st.dataframe(_unchecked, use_container_width=True, hide_index=True)

                uc1, uc2 = st.columns(2)
                with uc1:
                    _sel_confirm = st.selectbox(
                        "접수번호 선택 (입고확인)",
                        _unchecked["접수번호"].tolist(),
                        key="sel_receipt_confirm"
                    )
                with uc2:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("입고 확인", type="primary", key="btn_confirm_receipt"):
                        if _client:
                            try:
                                _client.confirm_return_receipt(int(_sel_confirm))
                                with engine.connect() as conn:
                                    conn.execute(text(
                                        "UPDATE return_requests SET receipt_status = 'VENDOR_WAREHOUSE_CONFIRM', updated_at = :now WHERE account_id = :aid AND receipt_id = :rid"
                                    ), {"now": datetime.utcnow().isoformat(), "aid": _aid, "rid": int(_sel_confirm)})
                                    conn.commit()
                                st.success(f"입고 확인 완료: 접수번호 {_sel_confirm}")
                                st.cache_data.clear()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트를 생성할 수 없습니다.")

            st.divider()

            # ── 반품 승인 대기 ──
            st.subheader("반품 승인 대기")
            st.caption("입고확인(VENDOR_WAREHOUSE_CONFIRM) 상태 → 반품 승인(환불 처리). 빠른환불 대상은 자동 처리됩니다.")

            _confirmed = query_df("""
                SELECT r.receipt_id as 접수번호,
                       r.order_id as 주문번호,
                       r.receipt_type as 유형,
                       r.cancel_reason_category1 as 사유,
                       r.cancel_count_sum as 수량,
                       r.fault_by_type as 귀책,
                       CASE WHEN r.pre_refund THEN 'Y' ELSE 'N' END as 선환불,
                       DATE(r.created_at_api) as 접수일
                FROM return_requests r
                WHERE r.account_id = :aid
                      AND r.receipt_status = 'VENDOR_WAREHOUSE_CONFIRM'
                ORDER BY r.created_at_api
            """, {"aid": _aid})

            if _confirmed.empty:
                st.info("승인 대기 중인 반품이 없습니다.")
            else:
                _confirmed["유형"] = _confirmed["유형"].map(lambda x: _TYPE_MAP.get(x, x))
                _confirmed["귀책"] = _confirmed["귀책"].map(lambda x: _FAULT_MAP.get(x, x) if x else "-")
                st.dataframe(_confirmed, use_container_width=True, hide_index=True)

                ac1, ac2 = st.columns(2)
                with ac1:
                    _sel_approve = st.selectbox(
                        "접수번호 선택 (승인)",
                        _confirmed["접수번호"].tolist(),
                        key="sel_receipt_approve"
                    )
                with ac2:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("반품 승인", type="primary", key="btn_approve_return"):
                        if _client:
                            try:
                                # cancelCount 가져오기
                                _approve_row = _confirmed[_confirmed["접수번호"] == _sel_approve].iloc[0]
                                _cancel_count = int(_approve_row["수량"]) if pd.notna(_approve_row["수량"]) else 1
                                _client.approve_return_request(int(_sel_approve), cancel_count=_cancel_count)
                                with engine.connect() as conn:
                                    conn.execute(text(
                                        "UPDATE return_requests SET receipt_status = 'RETURNS_COMPLETED', updated_at = :now WHERE account_id = :aid AND receipt_id = :rid"
                                    ), {"now": datetime.utcnow().isoformat(), "aid": _aid, "rid": int(_sel_approve)})
                                    conn.commit()
                                st.success(f"반품 승인 완료: 접수번호 {_sel_approve}")
                                st.cache_data.clear()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트를 생성할 수 없습니다.")

            st.divider()

            # ── 출고중지 요청 ──
            st.subheader("출고중지 요청")
            st.caption("상품준비중 단계에서 고객이 반품 접수한 건. 출고 전 중지 처리가 필요합니다.")

            _stop_req = query_df("""
                SELECT r.receipt_id as 접수번호,
                       r.order_id as 주문번호,
                       r.cancel_reason_category1 as 사유,
                       r.cancel_count_sum as 수량,
                       r.release_stop_status as 출고중지상태,
                       DATE(r.created_at_api) as 접수일
                FROM return_requests r
                WHERE r.account_id = :aid
                      AND r.receipt_status = 'RELEASE_STOP_UNCHECKED'
                ORDER BY r.created_at_api
            """, {"aid": _aid})

            if _stop_req.empty:
                st.info("출고중지 요청 건이 없습니다.")
            else:
                st.dataframe(_stop_req, use_container_width=True, hide_index=True)

    # ── 탭3: 회수 송장 등록 ──
    with tab3:
        if selected_account is None:
            st.warning("사이드바에서 계정을 선택하세요.")
        else:
            _aid3 = int(selected_account["id"])
            _client3 = create_wing_client(selected_account)

            st.subheader("회수 송장 등록")
            st.caption("굿스플로(반품자동연동)를 사용하지 않고 자체 회수하는 경우, 반품접수(RETURNS_UNCHECKED) 상태에서 회수 송장을 등록합니다.")

            # 등록 가능한 반품 목록
            _invoice_target = query_df("""
                SELECT r.receipt_id as 접수번호,
                       r.order_id as 주문번호,
                       r.cancel_reason_category1 as 사유,
                       r.cancel_count_sum as 수량,
                       r.requester_name as 요청자,
                       r.requester_address as 회수지주소,
                       r.return_delivery_type as 회수종류,
                       DATE(r.created_at_api) as 접수일
                FROM return_requests r
                WHERE r.account_id = :aid
                      AND r.receipt_status = 'RETURNS_UNCHECKED'
                ORDER BY r.created_at_api
            """, {"aid": _aid3})

            if _invoice_target.empty:
                st.info("회수 송장 등록 가능한 반품이 없습니다.")
            else:
                st.dataframe(_invoice_target, use_container_width=True, hide_index=True)

                st.markdown("---")

                ic1, ic2 = st.columns(2)
                with ic1:
                    _sel_inv_receipt = st.selectbox(
                        "접수번호", _invoice_target["접수번호"].tolist(), key="sel_inv_receipt"
                    )
                    _sel_company = st.selectbox(
                        "택배사",
                        list(_DELIVERY_COMPANIES.keys()),
                        format_func=lambda x: f"{_DELIVERY_COMPANIES[x]} ({x})",
                        key="sel_inv_company",
                    )
                with ic2:
                    _inv_number = st.text_input("운송장번호", key="inv_number")
                    _reg_number = st.text_input("택배사 회수번호 (선택)", key="reg_number")

                if st.button("회수 송장 등록", type="primary", key="btn_create_invoice"):
                    if not _inv_number.strip():
                        st.error("운송장번호를 입력하세요.")
                    elif _client3:
                        try:
                            result = _client3.create_return_invoice(
                                receipt_id=int(_sel_inv_receipt),
                                delivery_company_code=_sel_company,
                                invoice_number=_inv_number.strip(),
                                delivery_type="RETURN",
                                reg_number=_reg_number.strip(),
                            )
                            st.success(f"회수 송장 등록 완료: {_DELIVERY_COMPANIES.get(_sel_company, _sel_company)} {_inv_number}")
                        except CoupangWingError as e:
                            st.error(f"API 오류: {e}")
                    else:
                        st.error("WING API 클라이언트를 생성할 수 없습니다.")
