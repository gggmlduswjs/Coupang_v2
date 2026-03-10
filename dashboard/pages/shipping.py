"""
배송/송장 관리 페이지
====================
STEP 1: 주문 확인 → STEP 2: 배송리스트 → STEP 3: 한진 송장 → STEP 4: 쿠팡 등록

orders.py 탭2와 동일한 안전장치 적용:
- 4가지 매칭 방식 (직접/순번/수취인명/INSTRUCT 직접)
- 중복 송장 등록 방지 (INSTRUCT 상태 체크)
- 출고중지요청(RU) 감지
"""
import logging
from datetime import date

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from dashboard.utils import create_wing_client
from dashboard.services.order_data import (
    load_all_orders_live,
    get_instruct_orders,
    get_instruct_by_box,
    clear_order_caches,
    fmt_krw_short,
    build_delivery_excel_bytes,
)
from dashboard.services.order_service import (
    load_hanjin_creds as _load_hanjin_creds,
    save_hanjin_creds as _save_hanjin_creds,
    update_orders_status_after_invoice as _update_orders_status,
)
# orders.py의 매칭/안전 함수 재사용
from dashboard.pages.orders import (
    _match_direct,
    _match_by_sequence,
    _match_by_name,
    _match_by_name_from_orders,
    _match_by_row_order,
    _match_by_memo_box_id,
    _check_stop_shipment_requests,
)

logger = logging.getLogger(__name__)


def render(selected_account, accounts_df, account_names):
    st.title("배송/송장 관리")

    # ── 공유 데이터 로드 (WING API 실시간) ──
    _all_orders = load_all_orders_live(accounts_df)
    _instruct_all = get_instruct_orders(_all_orders)
    _inst_by_box = get_instruct_by_box(_instruct_all)

    # ── 상단 컨트롤 ──
    _top_c1, _top_c2 = st.columns([2, 5])
    with _top_c1:
        if st.button("🔄 주문 새로고침", key="ship_btn_refresh", use_container_width=True,
                     help="WING API에서 실시간 주문 조회", type="primary"):
            clear_order_caches()
            st.rerun()
    with _top_c2:
        _last_synced = st.session_state.get("order_last_synced")
        if _last_synced:
            st.caption(f"마지막 조회: {_last_synced} (WING API 실시간)")

    st.caption("상품준비중 주문 배송 처리: 주문 확인 → 배송리스트 → 한진 송장 발급 → 쿠팡 송장 등록")

    # ── Step 워크플로우 표시 ──
    _s1, _s2, _s3, _s4 = st.columns(4)
    with _s1:
        st.markdown("**STEP 1** 주문 확인")
    with _s2:
        st.markdown("**STEP 2** 배송리스트")
    with _s3:
        st.markdown("**STEP 3** 한진 송장")
    with _s4:
        st.markdown("**STEP 4** 쿠팡 등록")

    st.divider()

    # ── STEP 1: 상품준비중 주문 확인 ──
    st.subheader("STEP 1: 상품준비중 주문 확인")

    _inst_total = len(_inst_by_box)
    _inst_amount = int(_inst_by_box["결제금액"].sum()) if not _inst_by_box.empty else 0

    _ik1, _ik2 = st.columns(2)
    _ik1.metric("상품준비중 주문", f"{_inst_total:,}건")
    _ik2.metric("총 금액", f"₩{fmt_krw_short(_inst_amount)}")

    if _inst_by_box.empty:
        st.info("상품준비중(INSTRUCT) 상태의 주문이 없습니다.")
    else:
        _inst_display = _inst_by_box[["계정", "묶음배송번호", "주문번호", "상품명", "수량", "결제금액", "주문일", "수취인"]].copy()
        _inst_display["결제금액"] = _inst_display["결제금액"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

        gb = GridOptionsBuilder.from_dataframe(_inst_display)
        gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
        gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
        gb.configure_column("상품명", width=350)
        grid_opts = gb.build()
        AgGrid(_inst_display, gridOptions=grid_opts, height=400, theme="streamlit", key="ship_instruct_grid")

    st.divider()

    # ── STEP 2: 배송리스트 다운로드 (공유 함수 사용) ──
    st.subheader("STEP 2: 배송리스트 다운로드")

    if _instruct_all.empty:
        st.info("상품준비중 주문이 없습니다.")
    else:
        _dl_orders = _instruct_all.copy()

        # 계정별 건수 표시
        _acct_counts = _dl_orders.groupby("계정").size().reset_index(name="건수")
        st.dataframe(_acct_counts, hide_index=True)

        # 공유 함수로 엑셀 생성
        _xl_bytes, _dl_df = build_delivery_excel_bytes(_dl_orders, sort_and_color=True)

        # 세션에 저장 (송장 매칭용)
        st.session_state["_delivery_list_df"] = _dl_df.copy()

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

        st.download_button(
            f"📦 배송리스트 다운로드 ({len(_dl_orders)}건, 책별 정렬)",
            _xl_bytes,
            file_name=f"DeliveryList({date.today().isoformat()})_통합.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="ship_dl_delivery_list",
            type="primary",
            use_container_width=True,
        )
        st.caption("Sheet1: 한진택배 업로드용 (책 순 정렬) | Sheet2: 픽킹리스트")

    st.divider()

    # ── STEP 3: 한진 N-Focus 송장 발급 ──
    st.subheader("STEP 3: 한진 N-Focus 송장 발급")
    st.caption("STEP 2에서 다운로드한 배송리스트 엑셀을 업로드하면 N-Focus에서 송장을 자동 발급합니다.")

    _hanjin_creds = _load_hanjin_creds()
    if not _hanjin_creds.get("user_id"):
        with st.expander("한진 N-Focus 로그인 설정", expanded=True):
            _hj_id = st.text_input("N-Focus 아이디", key="ship_hanjin_id")
            _hj_pw = st.text_input("N-Focus 비밀번호", type="password", key="ship_hanjin_pw")
            if st.button("저장", key="ship_hanjin_save"):
                if _hj_id and _hj_pw:
                    _save_hanjin_creds(_hj_id, _hj_pw)
                    st.success("한진 크레덴셜 저장 완료")
                    st.rerun()
                else:
                    st.warning("아이디와 비밀번호를 모두 입력하세요.")
    else:
        with st.expander(f"N-Focus 계정: {_hanjin_creds['user_id']}", expanded=False):
            _hj_id = st.text_input("N-Focus 아이디", value=_hanjin_creds.get("user_id", ""), key="ship_hanjin_id_edit")
            _hj_pw = st.text_input("N-Focus 비밀번호", type="password", key="ship_hanjin_pw_edit")
            if st.button("변경 저장", key="ship_hanjin_save_edit"):
                if _hj_id and _hj_pw:
                    _save_hanjin_creds(_hj_id, _hj_pw)
                    st.success("한진 크레덴셜 업데이트 완료")
                    st.rerun()

    _nfocus_file = st.file_uploader(
        "배송리스트 엑셀 (STEP 2에서 다운로드한 파일)",
        type=["xlsx", "xls"],
        key="ship_nfocus_upload",
    )

    _nfocus_disabled = (
        not _hanjin_creds.get("user_id")
        or _nfocus_file is None
        or st.session_state.get("nfocus_running", False)
    )

    if st.button(
        "한진 N-Focus 자동 처리",
        key="ship_btn_nfocus",
        type="primary",
        disabled=_nfocus_disabled,
        use_container_width=True,
    ):
        st.session_state["nfocus_running"] = True
        try:
            from operations.hanjin_nfocus import HanjinNFocusClient

            _hc = _load_hanjin_creds()
            with st.status("N-Focus 처리 중...", expanded=True) as status:
                with HanjinNFocusClient(
                    user_id=_hc["user_id"],
                    password=_hc["password"],
                    headless=False,
                ) as client:
                    _nf_result = client.process_full_workflow(
                        excel_bytes=_nfocus_file.getvalue(),
                        filename=_nfocus_file.name,
                        progress_callback=lambda msg: st.write(msg),
                    )

                if _nf_result["success"]:
                    status.update(label="N-Focus 처리 완료!", state="complete")
                    st.success(
                        f"정상 출력: {_nf_result['registered']}건"
                        + (f" / 오류: {_nf_result['error']}건" if _nf_result["error"] else "")
                    )
                    if _nf_result["error_details"]:
                        with st.expander("오류 상세"):
                            for _err in _nf_result["error_details"]:
                                st.warning(_err)
                    if _nf_result["invoice_excel"]:
                        st.download_button(
                            "운송장 엑셀 다운로드 → STEP 4에서 업로드",
                            _nf_result["invoice_excel"],
                            file_name=f"Invoice_{date.today().isoformat()}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="ship_dl_nfocus_invoice",
                        )
                else:
                    status.update(label="N-Focus 처리 실패", state="error")
        except ImportError:
            st.error("playwright 미설치. `pip install playwright && playwright install chromium`")
        except Exception as e:
            st.error(f"N-Focus 오류: {e}")
        finally:
            st.session_state["nfocus_running"] = False

    st.divider()

    # ── STEP 4: 쿠팡 송장 등록 (orders.py와 동일한 안전장치) ──
    st.subheader("STEP 4: 쿠팡 송장 등록")
    st.caption("운송장번호가 포함된 엑셀을 업로드하면 자동 매칭 후 쿠팡에 등록합니다.")

    _inv_file = st.file_uploader("송장 엑셀 파일 (운송장번호 포함)", type=["xlsx", "xls"], key="ship_inv_file_upload")
    _inv_df = None

    if _inv_file is not None:
        try:
            _inv_df = pd.read_excel(_inv_file)
        except Exception as e:
            st.error(f"엑셀 파일 읽기 오류: {e}")

    if _inv_df is None:
        return

    # ── 매칭 로직 (orders.py와 동일한 4가지 방식) ──
    _delivery_df = st.session_state.get("_delivery_list_df")

    # 한진 엑셀 컬럼 확인
    _has_direct_cols = all(c in _inv_df.columns for c in ["묶음배송번호", "주문번호", "운송장번호"])
    _has_hanjin_seq_cols = "순번" in _inv_df.columns and "운송장번호" in _inv_df.columns
    _recv_col_name = None
    for _rc in ["받으시는 분", "받으시는분", "수취인", "수취인이름"]:
        if _rc in _inv_df.columns:
            _recv_col_name = _rc
            break
    _has_hanjin_name_cols = "운송장번호" in _inv_df.columns and _recv_col_name is not None

    _matched_df = None

    if _has_direct_cols:
        st.info("묶음배송번호/주문번호 컬럼 감지 → 직접 매칭 모드")
        _matched_df = _match_direct(_inv_df, _instruct_all, accounts_df)

    elif _has_hanjin_seq_cols and _delivery_df is not None:
        st.info("한진 출력자료등록 형식 감지 → 순번 기반 자동 매칭 모드")
        _matched_df = _match_by_sequence(_inv_df, _delivery_df, accounts_df)

    elif _has_hanjin_name_cols:
        if _delivery_df is not None:
            # 1차: BOX:{묶음배송번호} 메모 매칭
            _matched_df = _match_by_memo_box_id(_inv_df, _delivery_df)

            # 2차: 행 순서 매칭
            if _matched_df is None:
                _hj_valid = _inv_df[_inv_df["운송장번호"].notna() & (_inv_df["운송장번호"] != "")]
                if len(_hj_valid) == len(_delivery_df):
                    st.info("행 순서 기반 매칭 (배송리스트와 행 수 일치)")
                    _matched_df = _match_by_row_order(_inv_df, _delivery_df, _recv_col_name, accounts_df)
                else:
                    st.info("수취인/구매자 이름 기반 매칭 (배송리스트)")
                    _matched_df = _match_by_name(_inv_df, _delivery_df, _recv_col_name, accounts_df)
        else:
            st.info("수취인/구매자 이름 기반 매칭 (INSTRUCT 주문)")
            _matched_df = _match_by_name_from_orders(_inv_df, _instruct_all, _recv_col_name)

    elif _has_hanjin_seq_cols and _delivery_df is None:
        st.warning("배송리스트가 세션에 없습니다. 먼저 STEP 2에서 '배송리스트 다운로드'를 실행하세요.")
        return

    else:
        st.error("엑셀 형식을 인식할 수 없습니다. '묶음배송번호/주문번호/운송장번호' 또는 '순번/운송장번호' 컬럼이 필요합니다.")
        return

    if _matched_df is None or _matched_df.empty:
        st.info("등록할 송장이 없습니다.")
        return

    # 계정별 건수 표시
    _acct_id_map = dict(zip(accounts_df["id"].astype(int), accounts_df["account_name"]))
    _matched_df["계정"] = _matched_df["_account_id"].astype(int).map(_acct_id_map)
    _acct_summary = _matched_df.groupby("계정").size().reset_index(name="송장건수")
    st.dataframe(_acct_summary, hide_index=True)

    st.success(f"매칭 완료: {len(_matched_df)}건")

    # ── 중복 등록 방지: 현재 INSTRUCT 주문과 대조 ──
    if not _instruct_all.empty:
        _current_boxes = set(_instruct_all["묶음배송번호"].astype(str))
        _matched_df["_box_str"] = _matched_df["묶음배송번호"].astype(str)
        _already_done = _matched_df[~_matched_df["_box_str"].isin(_current_boxes)]
        _matched_df = _matched_df[_matched_df["_box_str"].isin(_current_boxes)].copy()
        _matched_df = _matched_df.drop(columns=["_box_str"])
        if not _already_done.empty:
            st.warning(f"⚠️ 이미 처리된 주문 {len(_already_done)}건 제외 (INSTRUCT 아님 → 중복 등록 방지)")
            with st.expander(f"제외된 주문 상세 ({len(_already_done)}건)"):
                st.dataframe(_already_done[["묶음배송번호", "주문번호", "운송장번호"]].drop_duplicates(), hide_index=True)
    else:
        st.warning("현재 INSTRUCT 주문이 없습니다. 등록할 대상이 없습니다.")
        return

    if _matched_df.empty:
        st.info("모든 주문이 이미 처리되었습니다. 등록할 송장이 없습니다.")
        return

    # ── 출고중지요청 체크 ──
    _stop_orders, _safe_df = _check_stop_shipment_requests(_matched_df, accounts_df)

    if not _stop_orders.empty:
        st.warning(f"⚠️ 출고중지요청 {len(_stop_orders)}건 감지 — 해당 주문은 송장 등록에서 제외됩니다.")
        with st.expander(f"출고중지요청 상세 ({len(_stop_orders)}건)", expanded=True):
            _stop_display = _stop_orders[["계정", "주문번호", "묶음배송번호", "_receipt_id", "_cancel_count", "_cancel_reason"]].copy()
            _stop_display.columns = ["계정", "주문번호", "묶음배송번호", "접수번호", "취소수량", "취소사유"]
            st.dataframe(_stop_display, hide_index=True)

            if st.button("출고중지완료 처리 (미출고 확인)", key="ship_btn_stop_shipment", type="secondary"):
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

    # 묶음배송번호 기준 중복 제거
    _before_dedup = len(_safe_df)
    _safe_df = _safe_df.drop_duplicates(subset=["묶음배송번호"], keep="first").copy()
    if _before_dedup != len(_safe_df):
        st.caption(f"묶음배송 중복 제거: {_before_dedup}행 → {len(_safe_df)}건")

    if not _stop_orders.empty:
        st.info(f"출고중지 제외 후 송장 등록 대상: {len(_safe_df)}건")

    if st.button(f"전체 송장 등록 ({len(_safe_df)}건)", key="ship_btn_bulk_invoice", type="primary"):
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

        # 성공 건 DB 상태 → DEPARTURE 업데이트
        if _success_items:
            _update_orders_status(_success_items)

        if _total_success > 0:
            st.success(f"송장 등록 완료: 총 {_total_success}건 성공" + (f", {_total_fail}건 실패" if _total_fail else ""))
            clear_order_caches()
            if _total_fail == 0:
                st.session_state.pop("_delivery_list_df", None)
            else:
                st.warning("일부 실패 건이 있어 배송리스트를 보존합니다. 새로고침 후 재시도하세요.")
            st.rerun()
        elif _total_fail > 0:
            st.error(f"전체 실패: {_total_fail}건 — 배송리스트를 보존합니다. 원인 확인 후 재시도하세요.")
