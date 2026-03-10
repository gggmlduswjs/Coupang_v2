"""
주문/배송 통합 페이지
====================
탭1: 결제완료 (ACCEPT) → 발주확인
탭2: 상품준비중 (INSTRUCT) → 발주서/극동/배송리스트/한진/송장등록
탭3: 배송지시 (DEPARTURE) → 조회 전용
"""
import io
import logging
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from core.api.wing_client import CoupangWingError
from core.constants import (
    is_gift_item,
    match_publisher_from_text,
    resolve_distributor,
)
from dashboard.utils import (
    create_wing_client,
    query_df,
    query_df_cached,
)
from dashboard.services.order_data import (
    load_all_orders_live,
    get_instruct_orders,
    get_instruct_by_box,
    clear_order_caches,
    fmt_krw_short,
    STATUS_MAP,
    build_delivery_rows,
    build_delivery_excel_bytes,
)
from dashboard.services.order_service import (
    load_hanjin_creds as _load_hanjin_creds,
    save_hanjin_creds as _save_hanjin_creds,
    update_orders_status_after_invoice as _update_orders_status,
)
from dashboard.services.invoice_matcher import (
    load_latest_batch,
    match_invoices,
    check_registerable,
)
from core.database import SessionLocal
from core.models.delivery_log import DeliveryListLog

logger = logging.getLogger(__name__)


def render(selected_account, accounts_df, account_names):
    st.title("주문/배송")

    # ── 상단 컨트롤 ──
    _top_c1, _top_c2 = st.columns([2, 5])
    with _top_c1:
        if st.button("🔄 주문 새로고침", key="btn_live_refresh", use_container_width=True,
                     help="WING API에서 실시간 주문 조회", type="primary"):
            clear_order_caches()
            st.rerun()
    with _top_c2:
        _last_synced = st.session_state.get("order_last_synced")
        if _last_synced:
            st.caption(f"마지막 조회: {_last_synced} (WING API 실시간)")

    # ── 데이터 로드 ──
    _all_orders = load_all_orders_live(accounts_df)

    def _filter_status(df, status):
        if df.empty:
            return pd.DataFrame()
        return df[df["상태"] == status].copy()

    def _kpi_count(df, status):
        sub = _filter_status(df, status)
        if sub.empty:
            return {}
        return sub.groupby("계정")["묶음배송번호"].nunique().to_dict()

    _accept_all = _filter_status(_all_orders, "ACCEPT")
    _instruct_live = _filter_status(_all_orders, "INSTRUCT")
    _instruct_all = _instruct_live[~_instruct_live["취소"]].copy() if not _instruct_live.empty else pd.DataFrame()

    _kpi_accept = _accept_all.groupby("계정")["묶음배송번호"].nunique().to_dict() if not _accept_all.empty else {}
    _kpi_instruct = _instruct_all.groupby("계정")["묶음배송번호"].nunique().to_dict() if not _instruct_all.empty else {}
    _kpi_departure = _kpi_count(_all_orders, "DEPARTURE")
    _kpi_delivering = _kpi_count(_all_orders, "DELIVERING")
    _kpi_final = _kpi_count(_all_orders, "FINAL_DELIVERY")

    # ── 상단 KPI (항상 표시) ──
    _kc1, _kc2, _kc3, _kc4, _kc5 = st.columns(5)

    def _render_kpi(col, label, counts):
        total = sum(counts.values())
        col.metric(label, f"{total:,}건")
        if counts:
            parts = [f"{k}: {v}" for k, v in sorted(counts.items())]
            col.caption(" | ".join(parts))

    _render_kpi(_kc1, "결제완료", _kpi_accept)
    _render_kpi(_kc2, "상품준비중", _kpi_instruct)
    _render_kpi(_kc3, "배송지시", _kpi_departure)
    _render_kpi(_kc4, "배송중", _kpi_delivering)
    _render_kpi(_kc5, "배송완료(30일)", _kpi_final)

    st.divider()

    # ── 3탭 ──
    _tab1, _tab2, _tab3 = st.tabs(["결제완료", "상품준비중", "배송지시"])

    # ══════════════════════════════════════
    # 탭1: 결제완료 (ACCEPT) → 발주확인
    # ══════════════════════════════════════
    with _tab1:
        st.caption("WING API 실시간 · 체크박스로 선택 → 발주확인(상품준비중) 처리")

        _t1_accts = st.multiselect("계정", account_names, default=account_names, key="t1_acct")
        _t1_data = _accept_all.copy() if not _accept_all.empty else pd.DataFrame()
        if not _t1_data.empty and _t1_accts:
            _t1_data = _t1_data[_t1_data["계정"].isin(_t1_accts)]

        _accept_total = _t1_data["묶음배송번호"].nunique() if not _t1_data.empty else 0
        _accept_amount = int(_t1_data["결제금액"].sum()) if not _t1_data.empty else 0
        _accept_by_acct = _t1_data.groupby("계정")["묶음배송번호"].nunique().to_dict() if not _t1_data.empty else {}

        _ak1, _ak2, _ak3 = st.columns(3)
        _ak1.metric("결제완료 주문", f"{_accept_total:,}건")
        _ak2.metric("총 금액", f"₩{fmt_krw_short(_accept_amount)}")
        if _accept_by_acct:
            _acct_parts = [f"{k}: {v}" for k, v in sorted(_accept_by_acct.items())]
            _ak3.metric("계정별", " | ".join(_acct_parts))

        st.divider()

        # ── 발주확인 후 DeliveryList 다운로드 (rerun 후 표시) ──
        if "_ack_delivery_excel" in st.session_state:
            _ack_xl = st.session_state.pop("_ack_delivery_excel")
            st.success(f"발주확인 완료 — DeliveryList 엑셀을 다운로드하세요.")
            st.download_button(
                f"📥 DeliveryList 다운로드 ({_ack_xl['count']}건)",
                _ack_xl["data"],
                file_name=_ack_xl["filename"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="t1_ack_dl",
                type="primary",
                use_container_width=True,
            )
            st.divider()

        if _t1_data.empty:
            st.info("결제완료(ACCEPT) 상태의 주문이 없습니다.")
        else:
            _accept_display = _t1_data.copy()
            _accept_display["상품/옵션/수량"] = _accept_display.apply(
                lambda r: f"{r['상품명']} / {r['옵션명']} / {int(r['수량'])}권", axis=1
            )
            _accept_display["수취인/연락처"] = _accept_display.apply(
                lambda r: f"{r['수취인']}" + (f" ({r['수취인전화번호']})" if r.get('수취인전화번호') else ""), axis=1
            )
            _accept_display["배송상태"] = _accept_display["상태"].map(STATUS_MAP)
            _accept_display["결제금액"] = _accept_display["결제금액"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

            _display_cols = [
                "주문번호", "상품/옵션/수량", "수취인/연락처", "수취인주소", "배송상태",
                "주문일시", "묶음배송번호", "계정", "결제금액",
            ]
            _grid_df = _accept_display[_display_cols].rename(columns={"수취인주소": "배송지"})

            gb = GridOptionsBuilder.from_dataframe(_grid_df)
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
            gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
            gb.configure_column("상품/옵션/수량", width=350)
            gb.configure_column("배송지", width=250)
            gb.configure_column("수취인/연락처", width=130)
            gb.configure_column("주문번호", width=120)
            gb.configure_column("묶음배송번호", width=120)
            gb.configure_column("주문일시", width=140)
            gb.configure_selection(
                "multiple", use_checkbox=True,
                header_checkbox=True,
                pre_selected_rows=list(range(len(_grid_df))),
            )
            grid_opts = gb.build()
            _grid_result = AgGrid(
                _grid_df, gridOptions=grid_opts, height=500, theme="streamlit",
                key="t1_accept_grid", update_mode="SELECTION_CHANGED",
            )

            _selected_rows = _grid_result.get("selected_rows", None)
            _has_selection = False
            if _selected_rows is not None:
                if isinstance(_selected_rows, pd.DataFrame) and not _selected_rows.empty:
                    _selected_df = _selected_rows
                    _has_selection = True
                elif isinstance(_selected_rows, list) and len(_selected_rows) > 0:
                    _selected_df = pd.DataFrame(_selected_rows)
                    _has_selection = True

            if _has_selection:
                _sel_box_ids = _selected_df["묶음배송번호"].unique().tolist()
                _sel_data = _t1_data[_t1_data["묶음배송번호"].isin(_sel_box_ids)].copy()
                _sel_boxes = len(_sel_box_ids)
                st.info(f"선택: {len(_selected_df)}건 ({_sel_boxes}묶음) / 전체: {len(_accept_display)}건")
            else:
                _sel_data = _t1_data.copy()
                _sel_boxes = _t1_data["묶음배송번호"].nunique() if not _t1_data.empty else 0
                st.info(f"전체 {len(_accept_display)}건 ({_sel_boxes}묶음) — 체크박스로 배송 불가 주문 제외 가능")

            st.divider()

            # ── 발주확인 처리 ──
            st.subheader("발주확인 처리")
            st.info(f"선택한 {_sel_boxes}건(묶음배송)을 상품준비중(INSTRUCT)으로 변경합니다.")

            if st.button(f"발주확인 ({_sel_boxes}건)", type="primary", key="t1_btn_ack"):
                _total_success = 0
                _total_fail = 0

                for _aid, _grp in _sel_data.groupby("_account_id"):
                    _acct_name = _grp.iloc[0]["계정"]
                    _acct_row = accounts_df[accounts_df["id"] == _aid]
                    if _acct_row.empty:
                        st.error(f"[{_acct_name}] 계정 정보를 찾을 수 없습니다.")
                        continue
                    _client = create_wing_client(_acct_row.iloc[0])
                    if not _client:
                        st.error(f"[{_acct_name}] WING API 클라이언트 생성 실패")
                        continue

                    _ack_ids = _grp["묶음배송번호"].unique().tolist()
                    try:
                        _ack_result = _client.acknowledge_ordersheets([int(x) for x in _ack_ids])
                        _success_ids = []
                        _fail_items = []
                        if isinstance(_ack_result, dict) and "data" in _ack_result:
                            _resp_data = _ack_result["data"]
                            _resp_code = _resp_data.get("responseCode")
                            _resp_list = _resp_data.get("responseList", [])
                            for _item in _resp_list:
                                if _item.get("succeed"):
                                    _success_ids.append(_item["shipmentBoxId"])
                                else:
                                    _fail_items.append(_item)
                            if _resp_code == 0:
                                st.success(f"[{_acct_name}] 완료: {len(_success_ids)}건")
                            elif _resp_code == 1:
                                st.warning(f"[{_acct_name}] 부분 성공: {len(_success_ids)}건 성공, {len(_fail_items)}건 실패")
                                for _fi in _fail_items:
                                    st.error(f"  {_fi.get('shipmentBoxId')}: {_fi.get('resultMessage', '')}")
                            elif _resp_code == 99:
                                st.error(f"[{_acct_name}] 전체 실패: {_resp_data.get('responseMessage', '')}")
                            else:
                                _success_ids = [int(x) for x in _ack_ids]
                                st.success(f"[{_acct_name}] 완료: {len(_success_ids)}건")
                        else:
                            _success_ids = [int(x) for x in _ack_ids]
                            st.success(f"[{_acct_name}] 완료: {len(_success_ids)}건")

                        _total_success += len(_success_ids)
                        _total_fail += len(_fail_items)

                    except CoupangWingError as e:
                        st.error(f"[{_acct_name}] API 오류: {e}")
                        _total_fail += len(_ack_ids)

                if _total_success > 0:
                    # 발주확인 후 배송지 재확인 (쿠팡 API 권고사항)
                    # 결제완료 중 고객이 배송지를 변경할 수 있으므로 INSTRUCT 재조회
                    clear_order_caches()
                    _refreshed = load_all_orders_live(accounts_df)
                    _refreshed_instruct = _refreshed[_refreshed["상태"] == "INSTRUCT"].copy() if not _refreshed.empty else pd.DataFrame()
                    if not _refreshed_instruct.empty:
                        _ack_box_ids = _sel_data["묶음배송번호"].unique().tolist()
                        _ack_orders = _refreshed_instruct[_refreshed_instruct["묶음배송번호"].isin(_ack_box_ids)]
                        if not _ack_orders.empty:
                            _ack_xl_bytes, _ = build_delivery_excel_bytes(_ack_orders, sort_and_color=False)
                        else:
                            _ack_xl_bytes, _ = build_delivery_excel_bytes(_sel_data, sort_and_color=False)
                    else:
                        _ack_xl_bytes, _ = build_delivery_excel_bytes(_sel_data, sort_and_color=False)
                    st.session_state["_ack_delivery_excel"] = {
                        "data": _ack_xl_bytes,
                        "count": len(_sel_data),
                        "filename": f"DeliveryList({date.today().isoformat()}).xlsx",
                    }
                    st.rerun()

            # ── 주문 취소 ──
            with st.expander("주문 취소", expanded=False):
                _render_cancel_section(accounts_df, account_names, _accept_all, _instruct_live)

    # ══════════════════════════════════════
    # 탭2: 상품준비중 (INSTRUCT) — 원스톱 배송
    # ══════════════════════════════════════
    with _tab2:
        st.caption("확정된 주문의 전체 배송 업무: 발주서 → 극동 → 배송리스트 → 한진 → 송장등록")

        _t2_accts = st.multiselect("계정", account_names, default=account_names, key="t2_acct")
        _t2_instruct = _instruct_all.copy() if not _instruct_all.empty else pd.DataFrame()
        if not _t2_instruct.empty and _t2_accts:
            _t2_instruct = _t2_instruct[_t2_instruct["계정"].isin(_t2_accts)]

        _inst_by_box = get_instruct_by_box(_t2_instruct)

        # KPI
        _inst_total = len(_inst_by_box)
        _inst_amount = int(_inst_by_box["결제금액"].sum()) if not _inst_by_box.empty else 0
        _ik1, _ik2 = st.columns(2)
        _ik1.metric("상품준비중 주문", f"{_inst_total:,}건")
        _ik2.metric("총 금액", f"₩{fmt_krw_short(_inst_amount)}")

        if _t2_instruct.empty:
            st.info("상품준비중(INSTRUCT) 상태의 주문이 없습니다.")
        else:
            # 2-1. 주문 확인 그리드 (체크박스)
            _inst_display = _inst_by_box[["계정", "묶음배송번호", "주문번호", "상품명", "수량", "결제금액", "주문일", "수취인"]].copy()
            _inst_display["결제금액_표시"] = _inst_display["결제금액"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
            _grid_cols = ["계정", "묶음배송번호", "주문번호", "상품명", "수량", "결제금액_표시", "주문일", "수취인"]
            _inst_grid_df = _inst_display[_grid_cols].rename(columns={"결제금액_표시": "결제금액"})

            gb_inst = GridOptionsBuilder.from_dataframe(_inst_grid_df)
            gb_inst.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            gb_inst.configure_default_column(resizable=True, sorteable=True, filterable=True)
            gb_inst.configure_column("상품명", width=350)
            gb_inst.configure_selection(
                "multiple", use_checkbox=True,
                header_checkbox=True,
                pre_selected_rows=list(range(len(_inst_grid_df))),
            )
            _inst_grid_result = AgGrid(
                _inst_grid_df, gridOptions=gb_inst.build(), height=400, theme="streamlit",
                key="t2_instruct_grid", update_mode="SELECTION_CHANGED",
            )

            # 선택된 주문만 필터링
            _t2_selected_rows = _inst_grid_result.get("selected_rows", None)
            _t2_has_sel = False
            if _t2_selected_rows is not None:
                if isinstance(_t2_selected_rows, pd.DataFrame) and not _t2_selected_rows.empty:
                    _t2_sel_df = _t2_selected_rows
                    _t2_has_sel = True
                elif isinstance(_t2_selected_rows, list) and len(_t2_selected_rows) > 0:
                    _t2_sel_df = pd.DataFrame(_t2_selected_rows)
                    _t2_has_sel = True

            if _t2_has_sel:
                _sel_box_ids = _t2_sel_df["묶음배송번호"].unique().tolist()
                _t2_filtered = _t2_instruct[_t2_instruct["묶음배송번호"].isin(_sel_box_ids)].copy()
                st.info(f"선택: {len(_t2_sel_df)}건 ({len(_sel_box_ids)}묶음) / 전체: {len(_inst_by_box)}건 — 체크 해제한 주문은 아래 엑셀/송장에서 제외됩니다")
            else:
                _t2_filtered = _t2_instruct.copy()
                st.info(f"전체 {len(_inst_by_box)}건 — 체크박스로 배송 보류 주문 제외 가능")

            st.divider()

            # 2-1. 발주서
            _render_purchase_order(_t2_filtered, accounts_df, key_prefix="t2")

            # 2-2. 극동 엑셀
            _render_geukdong_excel(_t2_filtered, accounts_df, key_prefix="t2")

            # 2-3. 배송리스트 다운로드
            _render_delivery_list(_t2_filtered)

            # 2-4. 한진 N-Focus 송장 발급
            _render_hanjin_nfocus()

            # 2-5. 쿠팡 송장 등록
            _render_invoice_upload(_t2_filtered, accounts_df)

    # ══════════════════════════════════════
    # 탭3: 배송지시 (DEPARTURE) — 조회 전용
    # ══════════════════════════════════════
    with _tab3:
        st.caption("배송지시(출고완료) 주문 조회")

        _t3_accts = st.multiselect("계정", account_names, default=account_names, key="t3_acct")

        _t3_data = _filter_status(_all_orders, "DEPARTURE")
        if not _t3_data.empty and _t3_accts:
            _t3_data = _t3_data[_t3_data["계정"].isin(_t3_accts)]

        _t3_total = _t3_data["묶음배송번호"].nunique() if not _t3_data.empty else 0
        _t3_amount = int(_t3_data["결제금액"].sum()) if not _t3_data.empty else 0
        _t3k1, _t3k2 = st.columns(2)
        _t3k1.metric("배송지시 주문", f"{_t3_total:,}건")
        _t3k2.metric("총 금액", f"₩{fmt_krw_short(_t3_amount)}")

        if _t3_data.empty:
            st.info("배송지시(DEPARTURE) 상태의 주문이 없습니다.")
        else:
            _t3_display = _t3_data.copy()
            _t3_display["상품/옵션/수량"] = _t3_display.apply(
                lambda r: f"{r['상품명']} / {r['옵션명']} / {int(r['수량'])}권", axis=1
            )
            _t3_cols = ["주문번호", "상품/옵션/수량", "택배사", "운송장번호", "수취인",
                        "수취인주소", "주문일시", "묶음배송번호", "계정"]
            _t3_grid = _t3_display[_t3_cols].rename(columns={"수취인주소": "배송지"})

            gb3 = GridOptionsBuilder.from_dataframe(_t3_grid)
            gb3.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
            gb3.configure_default_column(resizable=True, sorteable=True, filterable=True)
            gb3.configure_column("상품/옵션/수량", width=350)
            gb3.configure_column("배송지", width=250)
            AgGrid(_t3_grid, gridOptions=gb3.build(), height=500, theme="streamlit", key="t3_grid")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 하위 렌더 함수들
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _render_cancel_section(accounts_df, account_names, _accept_all, _instruct_live):
    """주문 취소 UI"""
    st.caption("ACCEPT/INSTRUCT 상태의 주문을 취소합니다.")

    _cancel_acct = st.selectbox("취소할 계정", account_names, key="t1_cancel_acct")
    _cancel_acct_row = None
    if _cancel_acct and not accounts_df.empty:
        _mask = accounts_df["account_name"] == _cancel_acct
        if _mask.any():
            _cancel_acct_row = accounts_df[_mask].iloc[0]

    if _cancel_acct_row is not None:
        _cancel_account_id = int(_cancel_acct_row["id"])
        _cancel_client = create_wing_client(_cancel_acct_row)

        _cancel_frames = []
        if not _accept_all.empty:
            _cancel_frames.append(_accept_all)
        if not _instruct_live.empty:
            _cancel_frames.append(_instruct_live)
        _cancel_all = pd.concat(_cancel_frames, ignore_index=True) if _cancel_frames else pd.DataFrame()
        _cancelable = pd.DataFrame()
        if not _cancel_all.empty:
            _cancel_acct_df = _cancel_all[
                (_cancel_all["_account_id"] == _cancel_account_id) & (~_cancel_all["취소"])
            ].copy()
            if not _cancel_acct_df.empty:
                _cancelable = _cancel_acct_df.rename(columns={"_vendor_item_id": "옵션ID"})[
                    ["주문번호", "옵션ID", "상품명", "수량", "결제금액", "상태", "주문일"]
                ].copy()

        if _cancelable.empty:
            st.info(f"[{_cancel_acct}] 취소 가능한 주문이 없습니다.")
        else:
            _cancelable_display = _cancelable.copy()
            _cancelable_display["상태"] = _cancelable_display["상태"].map(lambda x: STATUS_MAP.get(x, x))
            st.dataframe(_cancelable_display, use_container_width=True, hide_index=True)

            _cancel_reasons = {
                "SOLD_OUT": "재고 소진",
                "PRICE_ERROR": "가격 오류",
                "PRODUCT_ERROR": "상품 정보 오류",
                "OTHER": "기타 사유",
            }
            _sel_reason = st.selectbox("취소 사유", list(_cancel_reasons.keys()),
                                        format_func=lambda x: _cancel_reasons[x],
                                        key="t1_cancel_reason")
            _cancel_detail = st.text_input("상세 사유", value=_cancel_reasons[_sel_reason], key="t1_cancel_detail")

            _confirm_cancel = st.checkbox(
                f"{len(_cancelable)}건을 정말 취소하시겠습니까? (되돌릴 수 없음)",
                key="t1_cancel_confirm",
            )
            if _confirm_cancel:
                if st.button(f"주문 취소 ({len(_cancelable)}건)", type="secondary", key="t1_btn_cancel"):
                    if _cancel_client:
                        try:
                            _cancel_groups = _cancelable.groupby("주문번호")
                            _cancel_count = 0
                            for _oid, _group in _cancel_groups:
                                _vids = [int(x) for x in _group["옵션ID"].tolist() if pd.notna(x)]
                                _cnts = [int(x) for x in _group["수량"].tolist()]
                                if _vids:
                                    _cancel_client.cancel_order(
                                        order_id=int(_oid),
                                        vendor_item_ids=_vids,
                                        receipt_counts=_cnts,
                                        cancel_reason_category=_sel_reason,
                                        cancel_reason=_cancel_detail,
                                    )
                                    _cancel_count += len(_vids)
                            st.success(f"취소 요청 완료: {_cancel_count}건")
                            clear_order_caches()
                            st.rerun()
                        except CoupangWingError as e:
                            st.error(f"API 오류: {e}")
                    else:
                        st.error("WING API 클라이언트를 생성할 수 없습니다.")


def _render_purchase_order(instruct_all, accounts_df, key_prefix="t2"):
    """발주서 생성"""
    import re as _re

    _dist_orders = instruct_all.copy()

    with st.expander(f"📋 발주서 ({len(_dist_orders)}건)", expanded=False):
        if _dist_orders.empty:
            st.info("발주서 대상 주문이 없습니다.")
            return

        _pub_list = query_df_cached("SELECT name FROM publishers WHERE is_active = true ORDER BY LENGTH(name) DESC")
        _pub_names = _pub_list["name"].tolist() if not _pub_list.empty else []

        def _match_pub(row):
            result = match_publisher_from_text(str(row.get("옵션명") or ""), _pub_names)
            if not result:
                result = match_publisher_from_text(str(row.get("상품명") or ""), _pub_names)
            return result

        _isbn_lookup = query_df_cached("""
            SELECT l.coupang_product_id, l.isbn as isbn,
                   b.title as db_title, l.product_name as listing_name
            FROM listings l
            LEFT JOIN books b ON l.isbn = b.isbn AND l.isbn IS NOT NULL AND l.isbn != ''
            WHERE l.coupang_product_id IS NOT NULL
        """)
        _isbn_map = {}
        if not _isbn_lookup.empty:
            for _, _r in _isbn_lookup.iterrows():
                _isbn_map[str(_r["coupang_product_id"])] = {
                    "isbn": str(_r["isbn"]) if pd.notna(_r["isbn"]) else "",
                    "title": str(_r["db_title"]) if pd.notna(_r["db_title"]) else "",
                    "listing_name": str(_r["listing_name"]) if pd.notna(_r["listing_name"]) else "",
                }

        _TITLE_RE_PATTERNS = [
            r'\s*[-–]\s*2\d{3}\s*개정\s*교육과정.*$',
            r'\s+2\d{3}\s*개정\s*교육과정.*$',
            r'\s*[-–]\s*202\d학년도\s*수능\s*연계.*$',
            r'\s*\(202\d년?\s*수능대비\).*$',
            r'\s*\(202\d학년도\s*수능대비\).*$',
            r'\s*:\s*202\d학년도\s*수능.*$',
            r'\s*:\s*슝슝.*$',
            r'\s*:\s*동영상\s*강의.*$',
            r'\s*:\s*유형의\s*완성.*$',
            r'\s*#.*$',
            r'\s+사은품증정\s+\S+.*$',
            r'\s+/\s*본교재.*$',
            r'\s+\d+rd\s+edition.*$',
            r'\(2\d{3}년용\)',
            r'\s+고등\s+한국교육방송공사.*$',
            r'\s+한국교육방송공사.*$',
            r'\s+고등학교\s*[123]학년.*$',
            r'\s+고등\s*[123]학년.*$',
        ]

        def _clean_title(title: str) -> str:
            if title.startswith("사은품+"):
                title = title[4:]
            if "," in title:
                title = title[:title.index(",")].strip()
            for pat in _TITLE_RE_PATTERNS:
                title = _re.sub(pat, "", title, flags=_re.IGNORECASE).strip()
            if " : " in title:
                parts = title.split(" : ")
                if len(parts[0]) >= 10:
                    title = parts[0].strip()
            return title.strip()

        def _resolve_book_info(row):
            spid = str(row.get("_seller_product_id", ""))
            info = _isbn_map.get(spid, {})
            isbn = info.get("isbn", "")
            title = info.get("title", "")
            if not title:
                title = info.get("listing_name", "")
            if not title:
                title = str(row.get("상품명", "")).strip()
            if not title:
                title = str(row.get("옵션명", "")).strip()
            return pd.Series({"도서명": _clean_title(title), "ISBN": isbn})

        _dist_orders[["도서명", "ISBN"]] = _dist_orders.apply(_resolve_book_info, axis=1)

        _isbn_found = _dist_orders["ISBN"].apply(lambda x: bool(x and str(x).strip())).sum()
        _isbn_missing = len(_dist_orders) - _isbn_found
        if _isbn_missing > 0:
            st.caption(f"ℹ️ ISBN 없음: {_isbn_missing}/{len(_dist_orders)}건 (삭제된 상품 또는 세트물은 정상)")

        _ord_date_from_str = date.today().isoformat()
        _ord_date_to_str = date.today().isoformat()
        if "주문일" in _dist_orders.columns and not _dist_orders.empty:
            _dist_dates = _dist_orders["주문일"].dropna()
            if not _dist_dates.empty:
                _ord_date_from_str = str(_dist_dates.min())
                _ord_date_to_str = str(_dist_dates.max())

        _dist_orders["출판사"] = _dist_orders.apply(_match_pub, axis=1)
        _dist_orders["거래처"] = _dist_orders["출판사"].apply(resolve_distributor)

        _store_name = st.text_input(
            "가게명 (발주서 첫 줄에 표시)",
            value=st.session_state.get("order_store_name", "잉글리쉬존"),
            key=f"{key_prefix}_store_name",
            help="예: 잉글리쉬존, 북마트"
        )
        st.session_state["order_store_name"] = _store_name

        _dist_summary = _dist_orders.groupby("거래처").agg(
            건수=("도서명", "count"), 수량합계=("수량", "sum"), 금액합계=("결제금액", "sum"),
        ).reset_index().sort_values("건수", ascending=False)
        _dist_summary["금액합계"] = _dist_summary["금액합계"].apply(lambda x: f"{int(x):,}")
        st.dataframe(_dist_summary, hide_index=True, use_container_width=True)

        _dist_orders["_group_key"] = _dist_orders.apply(
            lambda r: r["ISBN"] if r.get("ISBN") else r["도서명"], axis=1
        )
        _agg = _dist_orders.groupby(["거래처", "출판사", "_group_key"]).agg(
            도서명=("도서명", "first"), ISBN=("ISBN", "first"), 주문수량=("수량", "sum"),
        ).reset_index().drop(columns=["_group_key"])
        _agg = _agg.sort_values(["거래처", "출판사", "도서명"])

        from openpyxl.styles import Font as _OXFont, Alignment as _OXAlign
        _xl_buf = io.BytesIO()
        with pd.ExcelWriter(_xl_buf, engine="openpyxl") as writer:
            _dist_order = ["백석", "강우사", "하람", "서부", "동아", "제일", "일신", "대성", "북전", "대원", "일반"]
            _all_dists = sorted(
                _agg["거래처"].unique(),
                key=lambda d: _dist_order.index(d) if d in _dist_order else 99,
            )
            for _dname in _all_dists:
                _sdf = _agg[_agg["거래처"] == _dname][["도서명", "출판사", "주문수량"]].copy()
                if _sdf.empty:
                    continue
                _sdf = _sdf.sort_values(["출판사", "도서명"]).reset_index(drop=True)
                _sdf["주문수량"] = _sdf["주문수량"].astype(int)
                _safe = _dname[:31].replace("/", "_").replace("\\", "_")
                _sdf.to_excel(writer, sheet_name=_safe, index=False, header=False, startrow=1)
                ws = writer.sheets[_safe]
                ws.merge_cells("A1:C1")
                _t = ws.cell(row=1, column=1)
                _t.value = f"{_store_name} 주문"
                _t.font = _OXFont(name="맑은 고딕", size=11)
                _t.alignment = _OXAlign(horizontal="center", vertical="center")
                for _r in range(2, ws.max_row + 1):
                    ws.cell(_r, 1).font = _OXFont(name="맑은 고딕", size=10)
                    ws.cell(_r, 1).alignment = _OXAlign(horizontal="left", vertical="center")
                    ws.cell(_r, 2).font = _OXFont(name="맑은 고딕", size=10)
                    ws.cell(_r, 2).alignment = _OXAlign(horizontal="left", vertical="center")
                    ws.cell(_r, 3).font = _OXFont(name="맑은 고딕", size=10)
                    ws.cell(_r, 3).alignment = _OXAlign(horizontal="center", vertical="center")
                ws.column_dimensions["A"].width = 57.5
                ws.column_dimensions["B"].width = 9.0
                ws.column_dimensions["C"].width = 13.0

        _xl_buf.seek(0)
        _file_date = _ord_date_to_str.replace("-", "")[2:]

        st.download_button(
            "📥 발주서 Excel 다운로드",
            _xl_buf.getvalue(),
            file_name=f"쿠팡{_file_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_dist_xlsx_dl",
            type="primary",
            use_container_width=True,
        )

        _dist_names_sorted = _dist_summary["거래처"].tolist()
        _dist_filter = st.multiselect(
            "거래처 필터", _dist_names_sorted,
            default=_dist_names_sorted, key=f"{key_prefix}_dist_filter",
        )
        _filtered_agg = _agg[_agg["거래처"].isin(_dist_filter)] if _dist_filter else _agg
        _show_agg = _filtered_agg[["거래처", "ISBN", "출판사", "도서명", "주문수량"]].copy()

        gb2 = GridOptionsBuilder.from_dataframe(_show_agg)
        gb2.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
        gb2.configure_default_column(resizable=True, sorteable=True, filterable=True)
        gb2.configure_column("도서명", width=350)
        gb2.configure_column("주문수량", width=80)
        AgGrid(_show_agg, gridOptions=gb2.build(), height=500, theme="streamlit", key=f"{key_prefix}_dist_grid")


def _render_geukdong_excel(instruct_all, accounts_df, key_prefix="t2"):
    """극동 엑셀"""
    with st.expander("📦 극동 엑셀", expanded=False):
        if instruct_all.empty:
            st.info("대상 주문이 없습니다.")
            return

        _gk_orders = instruct_all.copy()

        # sellerProductId → listings.isbn → books 매칭
        _gk_isbn_lookup = query_df_cached("""
            SELECT l.coupang_product_id,
                   l.isbn as "ISBN",
                   b.title as "DB도서명",
                   l.product_name as 리스팅도서명,
                   b.list_price as 정가,
                   COALESCE(b.author, '') as 저자,
                   b.year as 출판년도,
                   pub.name as 출판사,
                   pub.supply_rate as 공급률
            FROM listings l
            LEFT JOIN books b ON l.isbn = b.isbn AND l.isbn IS NOT NULL AND l.isbn != ''
            LEFT JOIN publishers pub ON b.publisher_id = pub.id
            WHERE l.coupang_product_id IS NOT NULL
        """)
        _gk_map = {}
        if not _gk_isbn_lookup.empty:
            for _, _r in _gk_isbn_lookup.iterrows():
                _gk_map[str(_r["coupang_product_id"])] = {
                    "ISBN": str(_r["ISBN"]) if pd.notna(_r["ISBN"]) else "",
                    "DB도서명": str(_r["DB도서명"]) if pd.notna(_r["DB도서명"]) else "",
                    "리스팅도서명": str(_r["리스팅도서명"]) if pd.notna(_r["리스팅도서명"]) else "",
                    "정가": _r["정가"] if pd.notna(_r["정가"]) else 0,
                    "저자": str(_r["저자"]) if pd.notna(_r["저자"]) else "",
                    "출판년도": _r["출판년도"] if pd.notna(_r["출판년도"]) else None,
                    "출판사": str(_r["출판사"]) if pd.notna(_r["출판사"]) else "",
                    "공급률": _r["공급률"] if pd.notna(_r["공급률"]) else None,
                }

        def _gk_enrich(row):
            info = _gk_map.get(str(row.get("_seller_product_id", "")), {})
            return pd.Series({
                "ISBN": info.get("ISBN", ""),
                "DB도서명": info.get("DB도서명", ""),
                "리스팅도서명": info.get("리스팅도서명", ""),
                "정가": info.get("정가", 0),
                "저자": info.get("저자", ""),
                "출판년도": info.get("출판년도", None),
                "출판사": info.get("출판사", ""),
                "공급률": info.get("공급률", None),
            })

        _gk_extra = _gk_orders.apply(_gk_enrich, axis=1)
        _gk_orders = pd.concat([_gk_orders, _gk_extra], axis=1)

        if _gk_orders.empty:
            st.info("극동 대상 주문이 없습니다.")
            return

        # 도서명 정리
        def _resolve_gk_title(r):
            if pd.notna(r.get("DB도서명")) and r["DB도서명"]:
                return str(r["DB도서명"]).strip()
            if pd.notna(r.get("리스팅도서명")) and r["리스팅도서명"]:
                return str(r["리스팅도서명"]).strip()
            return str(r["옵션명"]).strip()

        _gk_orders["도서명"] = _gk_orders.apply(_resolve_gk_title, axis=1)
        _gk_orders["ISBN_clean"] = _gk_orders["ISBN"].apply(lambda x: str(x).strip() if pd.notna(x) and x else "")

        # ISBN 기반 그룹핑
        _gk_orders["_key"] = _gk_orders.apply(lambda r: r["ISBN_clean"] if r["ISBN_clean"] else r["도서명"], axis=1)
        _gk_agg = _gk_orders.groupby("_key").agg(
            상품바코드=("ISBN_clean", "first"),
            상품명=("도서명", "first"),
            정가=("정가", "first"),
            수량=("수량", "sum"),
            공급률=("공급률", "first"),
            출판사=("출판사", "first"),
            저자=("저자", "first"),
            출판년도=("출판년도", "first"),
        ).reset_index(drop=True)

        # KPI
        _gk_total_amount = int(_gk_agg.apply(
            lambda r: (r["정가"] * r["공급률"] * r["수량"]) if pd.notna(r["공급률"]) and r["공급률"] and pd.notna(r["정가"]) else 0,
            axis=1
        ).sum()) if not _gk_agg.empty else 0
        _gk_k1, _gk_k2, _gk_k3 = st.columns(3)
        _gk_k1.metric("출고 품목", f"{len(_gk_agg)}종")
        _gk_k2.metric("출고 수량", f"{int(_gk_agg['수량'].sum())}권")
        _gk_k3.metric("총 금액", f"₩{fmt_krw_short(_gk_total_amount)}")

        _gk_show = _gk_agg[["상품바코드", "상품명", "수량", "출판사"]].copy()
        st.dataframe(_gk_show, hide_index=True, use_container_width=True)

        # 극동 형식 엑셀 생성
        _gk_result = pd.DataFrame()
        _gk_result["NO."] = range(1, len(_gk_agg) + 1)
        _gk_result["상품바코드"] = _gk_agg["상품바코드"].values
        _gk_result["상품명"] = _gk_agg["상품명"].values
        _gk_result["#"] = ""
        _gk_result["정 가"] = _gk_agg["정가"].apply(lambda x: int(x) if pd.notna(x) else 0).values
        _gk_result["수 량"] = _gk_agg["수량"].values
        _gk_result["%"] = _gk_agg["공급률"].apply(lambda x: f"{x*100:.0f}" if pd.notna(x) and x else "").values
        _gk_result["단 가"] = _gk_agg.apply(
            lambda r: int(r["정가"] * r["공급률"]) if pd.notna(r["공급률"]) and r["공급률"] and pd.notna(r["정가"]) else (int(r["정가"]) if pd.notna(r["정가"]) else 0),
            axis=1
        ).values
        _gk_result["금 액"] = (_gk_result["단 가"] * _gk_result["수 량"]).values
        _gk_result[""] = ""
        _gk_result["출판사"] = _gk_agg["출판사"].apply(lambda x: str(x) if pd.notna(x) else "").values
        _gk_result["저자"] = _gk_agg["저자"].apply(lambda x: str(x) if pd.notna(x) else "").values
        _gk_result["출판년도"] = _gk_agg["출판년도"].apply(lambda x: str(int(x)) if pd.notna(x) else "").values

        _gk_buf = io.BytesIO()
        with pd.ExcelWriter(_gk_buf, engine="openpyxl") as writer:
            _gk_result.to_excel(writer, sheet_name="극동", index=False)
        _gk_buf.seek(0)

        st.download_button(
            f"극동 엑셀 다운로드 ({len(_gk_agg)}종 / {int(_gk_agg['수량'].sum())}권)",
            _gk_buf.getvalue(),
            file_name=f"극동_{date.today().strftime('%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{key_prefix}_gk_xlsx_dl",
            type="primary",
            use_container_width=True,
        )


def _render_delivery_list(instruct_all):
    """2-4. 배송리스트 다운로드 (책별 정렬, 세션 저장)"""
    with st.expander("📦 배송리스트 다운로드", expanded=False):
        if instruct_all.empty:
            st.info("상품준비중 주문이 없습니다.")
            return

        try:
            _dl_orders = instruct_all.copy()
            _acct_counts = _dl_orders.groupby("계정").size().reset_index(name="건수")
            st.dataframe(_acct_counts, hide_index=True)

            # 공유 함수로 엑셀 생성
            _xl_bytes, _dl_df = build_delivery_excel_bytes(_dl_orders, sort_and_color=True)

            # 세션에 저장 (송장 매칭용)
            st.session_state["_delivery_list_df"] = _dl_df.copy()

            # ── 중복 다운로드 방지 (DB 기반) ──
            _current_boxes = set(int(b) for b in _dl_df["묶음배송번호"])
            try:
                _db = SessionLocal()
                _existing = _db.query(DeliveryListLog.shipment_box_id).filter(
                    DeliveryListLog.shipment_box_id.in_(list(_current_boxes))
                ).all()
                _db.close()
                _overlap_boxes = {r[0] for r in _existing}
                _overlap = _current_boxes & _overlap_boxes
            except Exception as e:
                logger.warning(f"배송리스트 중복 체크 실패: {e}")
                _overlap = set()
            if _overlap:
                st.error(
                    f"⚠️ 이미 배송리스트를 다운받은 주문 {len(_overlap)}건이 포함되어 있습니다.\n\n"
                    "같은 주문을 한진에 2번 입력하면 **송장이 중복 발급**됩니다!"
                )
                _force_dl = st.checkbox(
                    "중복 확인했음 — 그래도 다운로드",
                    key="t2_force_dl",
                    value=False,
                )
                if not _force_dl:
                    return
        except Exception as e:
            st.error(f"배송리스트 생성 오류: {e}")
            logger.exception("배송리스트 생성 오류")
            return

        # 책별 픽킹 요약
        _pick_summary = (
            _dl_df.groupby("등록상품명")
            .agg(건수=("묶음배송번호", "count"), 총수량=("구매수(수량)", "sum"))
            .sort_index()
            .reset_index()
        )
        _pick_summary.columns = ["도서명", "주문건수", "총수량"]
        with st.expander(f"📚 책별 픽킹 요약 ({len(_pick_summary)}종)", expanded=True):
            st.dataframe(_pick_summary, hide_index=True, use_container_width=True,
                         column_config={"도서명": st.column_config.TextColumn(width="large")})

        if st.download_button(
            f"📦 배송리스트 다운로드 ({len(_dl_orders)}건, 책별 정렬)",
            _xl_bytes,
            file_name=f"DeliveryList({date.today().isoformat()})_통합.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="t2_dl_delivery_list",
            type="primary",
            use_container_width=True,
        ):
            # 다운로드 클릭 시 DB에 기록 (UPSERT: 중복 시 업데이트, batch_id로 묶음)
            try:
                from uuid import uuid4
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                from dashboard.utils import engine as _eng

                _batch_id = str(uuid4())
                with _eng.connect() as _conn:
                    for _seq, (_, _r) in enumerate(_dl_df.iterrows(), 1):
                        _vals = {
                            "shipment_box_id": int(_r["묶음배송번호"]),
                            "account_id": int(_r["_account_id"]),
                            "order_id": int(_r.get("주문번호", 0) or 0),
                            "vendor_item_id": int(_r.get("_vendor_item_id", 0) or 0),
                            "receiver_name": str(_r.get("수취인이름", "")).strip(),
                            "buyer_name": str(_r.get("구매자", "")).strip(),
                            "seq_no": _seq,
                            "batch_id": _batch_id,
                        }
                        _stmt = pg_insert(DeliveryListLog.__table__).values(**_vals)
                        _stmt = _stmt.on_conflict_do_update(
                            index_elements=["shipment_box_id"],
                            set_={
                                "account_id": _vals["account_id"],
                                "order_id": _vals["order_id"],
                                "vendor_item_id": _vals["vendor_item_id"],
                                "receiver_name": _vals["receiver_name"],
                                "buyer_name": _vals["buyer_name"],
                                "seq_no": _vals["seq_no"],
                                "batch_id": _batch_id,
                                "downloaded_at": datetime.utcnow(),
                            },
                        )
                        _conn.execute(_stmt)
                    _conn.commit()
                st.session_state["_last_batch_id"] = _batch_id
            except Exception as e:
                logger.warning(f"배송리스트 다운로드 기록 실패: {e}")
        st.caption("Sheet1: 한진택배 업로드용 (책 순 정렬) | Sheet2: 픽킹리스트")


def _render_hanjin_nfocus():
    """2-5. 한진 N-Focus 바로가기"""
    with st.expander("🚚 한진 N-Focus 송장 발급", expanded=False):
        st.caption("배송리스트 엑셀을 한진 N-Focus에 업로드하여 송장을 발급하세요.")
        st.markdown("""
**순서:**
1. 위 '배송리스트 다운로드'에서 엑셀 다운로드
2. 아래 링크에서 N-Focus 접속 → 출력자료등록 → 엑셀 업로드 → 오류체크 → 출력
3. 출력자료등록 엑셀 다운로드
4. 아래 '쿠팡 송장 등록'에서 해당 엑셀 업로드 → 자동 매칭 → 쿠팡 등록
        """)
        st.link_button("한진 N-Focus 열기", "https://focus.hanjin.com/release/listup", type="primary", use_container_width=True)


def _render_invoice_upload(instruct_all, accounts_df):
    """2-6. 쿠팡 송장 등록 — invoice_matcher 기반 간소화"""
    with st.expander("📋 쿠팡 송장 등록", expanded=False):
        st.caption("한진 출력자료등록 엑셀을 업로드하면 자동으로 배송리스트와 매칭하여 쿠팡에 송장을 등록합니다.")

        _inv_file = st.file_uploader("한진 출력자료등록 엑셀 (운송장번호 포함)", type=["xlsx", "xls"], key="t2_inv_file")
        if _inv_file is None:
            return

        try:
            _inv_df = pd.read_excel(_inv_file)
        except Exception as e:
            st.error(f"엑셀 파일 읽기 오류: {e}")
            return

        # ── 1. 최신 배치 로드 (DB, 세션/컴퓨터 무관) ──
        _batch_df = load_latest_batch()
        if _batch_df is not None:
            _dl_at = _batch_df["_downloaded_at"].iloc[0]
            _dl_date = _dl_at.strftime("%m/%d %H:%M") if hasattr(_dl_at, "strftime") else str(_dl_at)[:16]
            st.info(f"배치: {_dl_date} ({len(_batch_df)}건)")
        else:
            st.caption("DB 배치 없음 — 이름 매칭(fallback) 사용")

        # ── 2. 매칭 ──
        _matched_df, _method = match_invoices(_inv_df, _batch_df)

        if _matched_df is None or _matched_df.empty:
            st.warning("매칭 결과가 없습니다. 엑셀 형식을 확인하세요. ('순번/운송장번호' 또는 '묶음배송번호/주문번호/운송장번호' 필요)")
            return

        st.success(f"매칭 완료: {len(_matched_df)}건 ({_method})")

        # ── 3. 등록 가능 여부 분류 ──
        _result = check_registerable(_matched_df, instruct_all)
        _reg_df = _result["registerable"]
        _shipped_df = _result["already_shipped"]
        _summary = _result["summary"]

        st.info(f"등록 가능: {_summary['등록가능']}건 / 이미 출고: {_summary['이미출고']}건")

        if not _shipped_df.empty:
            with st.expander(f"이미 출고된 주문 ({_summary['이미출고']}건)"):
                st.dataframe(_shipped_df[["묶음배송번호", "주문번호", "운송장번호"]].drop_duplicates(), hide_index=True)

        if _reg_df.empty:
            st.info("등록할 송장이 없습니다.")
            return

        # 계정별 건수
        _acct_id_map = dict(zip(accounts_df["id"].astype(int), accounts_df["account_name"]))
        _reg_df["계정"] = _reg_df["_account_id"].astype(int).map(_acct_id_map)
        _acct_summary = _reg_df.groupby("계정").size().reset_index(name="송장건수")
        st.dataframe(_acct_summary, hide_index=True)

        # ── 4. 출고중지요청 체크 ──
        _stop_orders, _safe_df = _check_stop_shipment_requests(_reg_df, accounts_df)

        if not _stop_orders.empty:
            st.warning(f"출고중지요청 {len(_stop_orders)}건 감지 — 해당 주문은 제외됩니다.")
            with st.expander(f"출고중지요청 상세 ({len(_stop_orders)}건)", expanded=True):
                _stop_display = _stop_orders[["계정", "주문번호", "묶음배송번호", "_receipt_id", "_cancel_count", "_cancel_reason"]].copy()
                _stop_display.columns = ["계정", "주문번호", "묶음배송번호", "접수번호", "취소수량", "취소사유"]
                st.dataframe(_stop_display, hide_index=True)

                if st.button("출고중지완료 처리 (미출고 확인)", key="t2_btn_stop_shipment", type="secondary"):
                    _stop_ok = 0
                    _stop_fail = 0
                    for _aid, _sg in _stop_orders.groupby("_account_id"):
                        _aid = int(_aid)
                        _acct_row = accounts_df[accounts_df["id"] == _aid]
                        if _acct_row.empty:
                            continue
                        _acct_row = _acct_row.iloc[0]
                        _client = create_wing_client(_acct_row)
                        if not _client:
                            continue
                        for _, _sr in _sg.iterrows():
                            try:
                                _client.stop_shipment(int(_sr["_receipt_id"]), int(_sr["_cancel_count"]))
                                _stop_ok += 1
                            except Exception as e:
                                _stop_fail += 1
                                st.error(f"[{_acct_row['account_name']}] 접수번호 {_sr['_receipt_id']}: {e}")
                    if _stop_ok > 0:
                        st.success(f"출고중지완료 처리: {_stop_ok}건 성공" + (f", {_stop_fail}건 실패" if _stop_fail else ""))
                        clear_order_caches()

        if _safe_df.empty:
            if not _stop_orders.empty:
                st.info("출고중지 건을 제외하면 등록할 송장이 없습니다.")
            return

        # ── 5. 묶음배송 중복 제거 + 등록 ──
        _before_dedup = len(_safe_df)
        _safe_df = _safe_df.drop_duplicates(subset=["묶음배송번호"], keep="first").copy()
        if _before_dedup != len(_safe_df):
            st.caption(f"묶음배송 중복 제거: {_before_dedup}행 → {len(_safe_df)}건")

        if st.button(f"전체 송장 등록 ({len(_safe_df)}건)", key="t2_btn_bulk_invoice", type="primary"):
            _total_success = 0
            _total_fail = 0
            _success_items = []

            for _aid, _grp in _safe_df.groupby("_account_id"):
                _aid = int(_aid)
                _acct_row = accounts_df[accounts_df["id"] == _aid]
                if _acct_row.empty:
                    continue
                _acct_row = _acct_row.iloc[0]
                _client = create_wing_client(_acct_row)
                if not _client:
                    st.error(f"[{_acct_row['account_name']}] API 클라이언트 생성 실패")
                    continue

                _inv_data = []
                for _, _r in _grp.iterrows():
                    _vid = int(_r["_vendor_item_id"]) if pd.notna(_r.get("_vendor_item_id")) else 0
                    _inv_data.append({
                        "shipmentBoxId": int(_r["묶음배송번호"]),
                        "orderId": int(_r["주문번호"]),
                        "vendorItemId": _vid,
                        "deliveryCompanyCode": "HANJIN",
                        "invoiceNumber": str(_r["운송장번호"]).strip(),
                        "splitShipping": False,
                        "preSplitShipped": False,
                        "estimatedShippingDate": "",
                    })

                try:
                    _result = _client.upload_invoice(_inv_data)
                    _s_cnt = 0
                    _f_cnt = 0
                    if isinstance(_result, dict) and "data" in _result:
                        for _ri in _result["data"].get("responseList", []):
                            if _ri.get("succeed"):
                                _s_cnt += 1
                                _success_items.append({
                                    "shipmentBoxId": _ri.get("shipmentBoxId"),
                                    "invoiceNumber": str(_ri.get("invoiceNumber", "")),
                                    "deliveryCompanyCode": "HANJIN",
                                })
                            else:
                                _f_cnt += 1
                                st.error(f"  [{_acct_row['account_name']}] {_ri.get('shipmentBoxId')}: {_ri.get('resultMessage', '')}")
                    else:
                        _s_cnt = len(_inv_data)
                        _success_items.extend(_inv_data)
                    _total_success += _s_cnt
                    _total_fail += _f_cnt
                    st.info(f"[{_acct_row['account_name']}] 성공 {_s_cnt}건" + (f", 실패 {_f_cnt}건" if _f_cnt else ""))
                except Exception as e:
                    _total_fail += len(_inv_data)
                    st.error(f"[{_acct_row['account_name']}] API 오류: {e}")

            if _success_items:
                _update_orders_status(_success_items)

            if _total_success > 0:
                st.success(f"송장 등록 완료: 총 {_total_success}건 성공" + (f", {_total_fail}건 실패" if _total_fail else ""))
                clear_order_caches()
                st.session_state.pop("_delivery_list_df", None)
                st.rerun()
            elif _total_fail > 0:
                st.error(f"전체 실패: {_total_fail}건 — 원인 확인 후 재시도하세요.")


def _check_stop_shipment_requests(matched_df, accounts_df):
    """송장 등록 전 출고중지요청(RU) 체크 — 해당 주문 분리 반환.

    Returns:
        (stop_orders_df, safe_df): 출고중지 대상 / 안전한 송장 등록 대상
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _today = date.today()
    _from = (_today - timedelta(days=14)).isoformat()
    _to = _today.isoformat()

    # 계정별 출고중지요청 조회
    _stop_order_ids = {}  # order_id → {receipt_id, cancel_count, cancel_reason, account_id}

    def _fetch_stops(acct_row, client):
        try:
            reqs = client.get_all_return_requests(_from, _to, status="RU")
            return acct_row, reqs
        except Exception as e:
            logger.warning(f"[{acct_row['account_name']}] 출고중지요청 조회 실패: {e}")
            return acct_row, []

    _acct_ids_in_matched = matched_df["_account_id"].astype(int).unique()
    _tasks = []
    for _aid in _acct_ids_in_matched:
        _acct_row = accounts_df[accounts_df["id"] == int(_aid)]
        if _acct_row.empty:
            continue
        _acct_row = _acct_row.iloc[0]
        _client = create_wing_client(_acct_row)
        if _client:
            _tasks.append((_acct_row, _client))

    if _tasks:
        with ThreadPoolExecutor(max_workers=min(len(_tasks), 10)) as pool:
            futures = [pool.submit(_fetch_stops, a, c) for a, c in _tasks]
            for f in as_completed(futures):
                acct_row, reqs = f.result()
                for req in reqs:
                    _oid = req.get("orderId")
                    if _oid:
                        _stop_order_ids[int(_oid)] = {
                            "receipt_id": req.get("receiptId"),
                            "cancel_count": req.get("cancelCountSum", 1),
                            "cancel_reason": req.get("cancelReason", ""),
                            "account_id": int(acct_row["id"]),
                            "account_name": acct_row["account_name"],
                        }

    if not _stop_order_ids:
        return pd.DataFrame(), matched_df

    # 매칭된 주문 중 출고중지 대상 분리
    _matched = matched_df.copy()
    _matched["_oid_int"] = _matched["주문번호"].astype(int)
    _is_stopped = _matched["_oid_int"].isin(_stop_order_ids.keys())

    _stop_df = _matched[_is_stopped].copy()
    _safe_df = _matched[~_is_stopped].copy()

    # 출고중지 상세 정보 추가
    if not _stop_df.empty:
        _stop_df["_receipt_id"] = _stop_df["_oid_int"].map(lambda x: _stop_order_ids.get(x, {}).get("receipt_id", ""))
        _stop_df["_cancel_count"] = _stop_df["_oid_int"].map(lambda x: _stop_order_ids.get(x, {}).get("cancel_count", 1))
        _stop_df["_cancel_reason"] = _stop_df["_oid_int"].map(lambda x: _stop_order_ids.get(x, {}).get("cancel_reason", ""))
        _acct_id_map = dict(zip(accounts_df["id"].astype(int), accounts_df["account_name"]))
        _stop_df["계정"] = _stop_df["_account_id"].astype(int).map(_acct_id_map)

    # 임시 컬럼 정리
    _stop_df = _stop_df.drop(columns=["_oid_int"], errors="ignore")
    _safe_df = _safe_df.drop(columns=["_oid_int"], errors="ignore")

    return _stop_df, _safe_df

