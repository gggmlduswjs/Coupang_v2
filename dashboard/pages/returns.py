"""
반품/교환 관리 페이지
====================
WING API 실시간 반품/취소/교환 목록 조회 + 처리.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from core.api.wing_client import CoupangWingError
from dashboard.utils import (
    create_wing_client,
    engine,
)
from sqlalchemy import text

logger = logging.getLogger(__name__)

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

# API 반품 상태 코드
_STATUS_CODES = {
    "출고중지요청": "RU",
    "반품접수": "UC",
    "쿠팡확인요청": "CC",
    "반품완료": "PR",
}


# ─────────────────────────────────────────────
# WING API → DataFrame 변환
# ─────────────────────────────────────────────

def _parse_return_row(acct_name: str, item: dict) -> dict:
    """API 응답 1건 → DataFrame 행"""
    # 반품 아이템 정보
    return_items = item.get("returnItems") or []
    item_names = []
    total_cancel = 0
    for ri in return_items:
        name = ri.get("sellerProductName") or ri.get("vendorItemName", "")
        if name:
            item_names.append(name)
        total_cancel += int(ri.get("cancelCount", 0) or 0)

    # 배송비 파싱
    shipping_charge = item.get("returnShippingCharge") or {}
    charge_units = int(shipping_charge.get("units", 0) or 0)

    # 회수 송장
    delivery_dtos = item.get("returnDeliveryDtos") or []
    invoice_parts = []
    for d in delivery_dtos:
        inv = d.get("deliveryInvoiceNo", "")
        if inv:
            company = _DELIVERY_COMPANIES.get(d.get("deliveryCompanyCode", ""), d.get("deliveryCompanyCode", ""))
            invoice_parts.append(f"{company}:{inv}")

    return {
        "계정": acct_name,
        "접수번호": item.get("receiptId"),
        "주문번호": item.get("orderId"),
        "유형": _TYPE_MAP.get(item.get("receiptType", ""), item.get("receiptType", "")),
        "상태": _STATUS_MAP.get(item.get("receiptStatus", ""), item.get("receiptStatus", "")),
        "접수일": (item.get("createdAt") or "")[:10],
        "상품명": " / ".join(item_names),
        "사유분류": item.get("cancelReasonCategory1", ""),
        "사유상세": item.get("cancelReasonCategory2", ""),
        "비고": item.get("cancelReason", ""),
        "수량": total_cancel or item.get("cancelCountSum", 0),
        "배송비": charge_units,
        "귀책": _FAULT_MAP.get(item.get("faultByType", ""), item.get("faultByType", "")),
        "요청자": item.get("requesterName", ""),
        "출고중지상태": item.get("releaseStopStatus", ""),
        "회수종류": item.get("returnDeliveryType", ""),
        "선환불": "Y" if item.get("preRefund") else "N",
        "사유코드": item.get("reasonCodeText", ""),
        "회수송장": " | ".join(invoice_parts) if invoice_parts else "",
        # 내부용
        "_receipt_status": item.get("receiptStatus", ""),
        "_receipt_type": item.get("receiptType", ""),
        "_return_items": return_items,
        "_raw": item,
    }


def _load_returns_live(accounts_df, days: int = 30):
    """전 계정 반품/취소 WING API 실시간 조회. 세션 캐시 30초."""
    cache_key = "_returns_live_cache"
    ts_key = "_returns_live_ts"
    now = time.time()
    if now - st.session_state.get(ts_key, 0) < 30 and cache_key in st.session_state:
        return st.session_state[cache_key]

    _today = date.today()
    # API 최대 31일이므로 31일 단위로 분할
    _date_to = _today
    _date_from = _today - timedelta(days=min(days, 31))

    acct_clients = []
    for _, acct in accounts_df.iterrows():
        client = create_wing_client(acct)
        if client:
            acct_clients.append((acct, client))

    all_rows = []

    if acct_clients:
        def _fetch(acct, client, cancel_type):
            try:
                data = client.get_all_return_requests(
                    date_from=_date_from.isoformat(),
                    date_to=_date_to.isoformat(),
                    cancel_type=cancel_type,
                )
                return acct, cancel_type, data
            except Exception as e:
                logger.warning(f"[{acct['account_name']}] 반품 {cancel_type} 조회 실패: {e}")
                return acct, cancel_type, []

        # 반품(RETURN) + 취소(CANCEL) 병렬 조회
        max_workers = min(len(acct_clients) * 2, 15) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(_fetch, acct, client, ct)
                for acct, client in acct_clients
                for ct in ["RETURN", "CANCEL"]
            ]
            for f in as_completed(futures):
                acct, cancel_type, items = f.result()
                acct_name = acct["account_name"]
                for item in items:
                    all_rows.append(_parse_return_row(acct_name, item))

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

    st.session_state[cache_key] = df
    st.session_state[ts_key] = time.time()
    st.session_state["returns_last_synced"] = datetime.now().strftime("%H:%M:%S")
    return df


def _clear_return_cache():
    for k in ["_returns_live_cache", "_returns_live_ts",
              "_exchange_live_cache", "_exchange_live_ts"]:
        st.session_state.pop(k, None)


# ─────────────────────────────────────────────
# 교환 WING API → DataFrame
# ─────────────────────────────────────────────

_EXCHANGE_STATUS_MAP = {
    "RECEIPT": "접수", "PROGRESS": "진행", "SUCCESS": "완료",
    "REJECT": "불가", "CANCEL": "철회",
}

_COLLECT_STATUS_MAP = {
    "BeforeDirection": "회수연동전", "CompleteDirection": "회수연동",
    "Delivering": "회수중", "CompleteCollect": "업체전달완료",
    "DirectionFail": "회수연동실패", "Fail": "회수실패",
    "Withdraw": "교환회수철회", "NoCollect": "회수불필요", "NoneData": "정보없음",
}

_DELIVERY_STATUS_MAP = {
    "BeforeDirection": "배송연동전", "CompleteDirection": "배송연동",
    "Delivering": "배송중", "CompleteDelivery": "배송완료",
    "DirectionFail": "배송연동실패", "VendorDirect": "업체직송",
    "Fail": "배송실패", "Withdraw": "교환배송철회", "NoneData": "정보없음",
}


def _parse_exchange_row(acct_name: str, item: dict) -> dict:
    ex_items = item.get("exchangeItemDtoV1s") or []
    item_names = [ei.get("targetItemName", "") for ei in ex_items if ei.get("targetItemName")]
    total_qty = sum(int(ei.get("quantity", 0) or 0) for ei in ex_items)

    return {
        "계정": acct_name,
        "교환ID": item.get("exchangeId"),
        "주문번호": item.get("orderId"),
        "교환상태": _EXCHANGE_STATUS_MAP.get(item.get("exchangeStatus", ""), item.get("exchangeStatus", "")),
        "접수일": (item.get("createdAt") or "")[:10],
        "상품명": " / ".join(item_names),
        "수량": total_qty,
        "귀책": _FAULT_MAP.get(item.get("faultType", ""), item.get("faultType", "")),
        "사유": item.get("reasonCodeText", ""),
        "사유상세": item.get("reasonEtcDetail", ""),
        "회수상태": _COLLECT_STATUS_MAP.get(item.get("collectStatus", ""), item.get("collectStatus", "")),
        "재배송상태": _DELIVERY_STATUS_MAP.get(item.get("deliveryStatus", ""), item.get("deliveryStatus", "")),
        "교환배송비": item.get("exchangeAmount", 0),
        "거부가능": "Y" if item.get("rejectable") else "N",
        "송장입력가능": "Y" if item.get("deliveryInvoiceModifiable") else "N",
        "_exchange_status": item.get("exchangeStatus", ""),
        "_collect_status": item.get("collectStatus", ""),
        "_exchange_items": ex_items,
        "_delivery_groups": item.get("deliveryInvoiceGroupDtos") or [],
        "_raw": item,
    }


def _load_exchanges_live(accounts_df):
    cache_key = "_exchange_live_cache"
    ts_key = "_exchange_live_ts"
    now = time.time()
    if now - st.session_state.get(ts_key, 0) < 30 and cache_key in st.session_state:
        return st.session_state[cache_key]

    _today = date.today()
    _date_from = (_today - timedelta(days=6)).isoformat()
    _date_to = _today.isoformat()

    acct_clients = []
    for _, acct in accounts_df.iterrows():
        client = create_wing_client(acct)
        if client:
            acct_clients.append((acct, client))

    all_rows = []
    if acct_clients:
        def _fetch(acct, client):
            try:
                return acct, client.get_all_exchange_requests(_date_from, _date_to)
            except Exception as e:
                logger.warning(f"[{acct['account_name']}] 교환 조회 실패: {e}")
                return acct, []

        with ThreadPoolExecutor(max_workers=min(len(acct_clients), 10)) as pool:
            futures = [pool.submit(_fetch, acct, client) for acct, client in acct_clients]
            for f in as_completed(futures):
                acct, items = f.result()
                for ex in items:
                    all_rows.append(_parse_exchange_row(acct["account_name"], ex))

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    st.session_state[cache_key] = df
    st.session_state[ts_key] = time.time()
    return df


# ─────────────────────────────────────────────
# 렌더
# ─────────────────────────────────────────────

def render(selected_account, accounts_df, account_names):
    st.title("반품/교환 관리")

    # ── 상단 컨트롤 ──
    c1, c2 = st.columns([2, 5])
    with c1:
        if st.button("새로고침", key="btn_ret_refresh", use_container_width=True,
                     type="primary", help="WING API에서 실시간 반품/교환 조회"):
            _clear_return_cache()
            st.rerun()
    with c2:
        _last = st.session_state.get("returns_last_synced")
        if _last:
            st.caption(f"마지막 조회: {_last} (WING API 실시간)")
        else:
            st.caption("WING API 실시간")

    # ── 데이터 로드 (실시간 API) ──
    _all = _load_returns_live(accounts_df, days=31)

    # ── 미처리 건수 계산 ──
    _pending = _all[_all["_receipt_status"].isin(["RELEASE_STOP_UNCHECKED", "RETURNS_UNCHECKED"])] if not _all.empty and "_receipt_status" in _all.columns else pd.DataFrame()
    _warehouse = _all[_all["_receipt_status"] == "VENDOR_WAREHOUSE_CONFIRM"] if not _all.empty and "_receipt_status" in _all.columns else pd.DataFrame()
    _completed = _all[_all["_receipt_status"] == "RETURNS_COMPLETED"] if not _all.empty and "_receipt_status" in _all.columns else pd.DataFrame()

    # ── KPI 카드 ──
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("미처리", f"{len(_pending):,}건")
    k2.metric("입고확인 대기", f"{len(_warehouse):,}건")
    k3.metric("처리완료", f"{len(_completed):,}건")
    k4.metric("전체", f"{len(_all):,}건" if not _all.empty else "0건")

    st.divider()

    # ── 탭 ──
    tab1, tab2, tab4 = st.tabs([
        f"미처리 ({len(_pending)})",
        "전체 내역",
        "교환",
    ])

    # ── 헬퍼: 계정명으로 client 생성 ──
    def _client_for(acct_name):
        if not acct_name or accounts_df.empty:
            return None, None
        _m = accounts_df["account_name"] == acct_name
        if not _m.any():
            return None, None
        _acct = accounts_df[_m].iloc[0]
        return _acct, create_wing_client(_acct)

    # ══════════════════════════════════════════════
    # 탭1: 미처리 (반품접수 + 출고중지 + 승인대기)
    # ══════════════════════════════════════════════
    with tab1:
        # 미처리 = 반품접수 + 출고중지 + 승인대기 합침
        _actionable = _all[_all["_receipt_status"].isin([
            "RELEASE_STOP_UNCHECKED", "RETURNS_UNCHECKED", "VENDOR_WAREHOUSE_CONFIRM",
        ])] if not _all.empty else pd.DataFrame()

        if _actionable.empty:
            st.info("미처리 반품 건이 없습니다.")
        else:
            _act_cols = [
                "계정", "접수번호", "주문번호", "유형", "상태", "접수일",
                "상품명", "사유분류", "수량", "귀책", "요청자", "회수종류", "선환불",
            ]
            _act_show = _actionable[[c for c in _act_cols if c in _actionable.columns]].reset_index(drop=True)
            _act_evt = st.dataframe(
                _act_show, use_container_width=True, hide_index=True, height=400,
                selection_mode="single-row", on_select="rerun", key="ret_action_table",
            )
            _act_sel = _act_evt.selection.rows if _act_evt and _act_evt.selection else []

            if _act_sel:
                _row = _actionable.iloc[_act_sel[0]]
                _receipt_id = _row["접수번호"]
                _status = _row["_receipt_status"]

                # ── 주문 상세 조회 (운송장번호) ──
                _acct, _client = _client_for(_row["계정"])
                _orig_invoice = ""
                _orig_company = ""
                _receiver_name = ""
                _receiver_phone = ""
                _orderer_name = ""
                _orderer_phone = ""
                if _client:
                    _return_items = _row.get("_return_items", [])
                    _sbid = _return_items[0].get("shipmentBoxId") if _return_items else None
                    if _sbid:
                        try:
                            _order_detail = _client.get_ordersheet_by_shipment(int(_sbid))
                            _od = _order_detail.get("data", {})
                            _orig_invoice = _od.get("invoiceNumber", "")
                            _orig_company = _od.get("deliveryCompanyName", "")
                            _receiver = _od.get("receiver", {})
                            _receiver_name = _receiver.get("name", "")
                            _receiver_phone = _receiver.get("safeNumber", "")
                            _orderer = _od.get("orderer", {})
                            _orderer_name = _orderer.get("name", "")
                            _orderer_phone = _orderer.get("safeNumber", "")
                        except Exception:
                            pass

                # ── 배송 운송장 (한진택배 입력용) ──
                if _orig_invoice:
                    st.success(f"**배송 운송장: {_orig_company} {_orig_invoice}**")
                    oi1, oi2 = st.columns(2)
                    with oi1:
                        st.write(f"**수취인:** {_receiver_name} / {_receiver_phone}")
                    with oi2:
                        st.write(f"**주문자:** {_orderer_name} / {_orderer_phone}")

                # ── 상세 정보 ──
                with st.expander(f"상세 — 접수번호 {_receipt_id}", expanded=False):
                    dc1, dc2, dc3 = st.columns(3)
                    dc1.write(f"**주문번호:** {_row['주문번호']}")
                    dc1.write(f"**유형:** {_row['유형']}")
                    dc1.write(f"**상태:** {_row['상태']}")
                    dc2.write(f"**요청자:** {_row['요청자']}")
                    dc2.write(f"**귀책:** {_row['귀책']}")
                    dc2.write(f"**선환불:** {_row['선환불']}")
                    dc3.write(f"**출고중지상태:** {_row.get('출고중지상태', '-')}")
                    dc3.write(f"**회수종류:** {_row.get('회수종류', '-')}")
                    dc3.write(f"**계정:** {_row['계정']}")

                    st.write(f"**사유:** {_row.get('사유분류', '')} > {_row.get('사유상세', '')} | {_row.get('사유코드', '')}")
                    if _row.get("비고"):
                        st.write(f"**비고:** {_row['비고']}")

                    items = _row.get("_return_items", [])
                    if items:
                        st.write("**반품 아이템:**")
                        item_rows = []
                        for ri in items:
                            item_rows.append({
                                "상품명": ri.get("sellerProductName", ""),
                                "옵션명": ri.get("vendorItemName", ""),
                                "옵션ID": ri.get("vendorItemId", ""),
                                "주문수량": ri.get("purchaseCount", 0),
                                "취소수량": ri.get("cancelCount", 0),
                                "출고상태": _RELEASE_STATUS_MAP.get(ri.get("releaseStatus", ""), ri.get("releaseStatus", "")),
                            })
                        st.dataframe(pd.DataFrame(item_rows), use_container_width=True, hide_index=True)

                st.divider()

                # ── 처리 액션 (상태에 따라) ──

                # 반품접수 → 입고확인 + 회수송장
                if _status == "RETURNS_UNCHECKED":
                    act1, act2 = st.columns(2)
                    with act1:
                        st.markdown("**입고 확인**")
                        if st.button("입고 확인 처리", type="primary", key="btn_confirm"):
                            if _client:
                                try:
                                    _client.confirm_return_receipt(int(_receipt_id))
                                    st.success(f"입고 확인 완료: {_receipt_id}")
                                    _clear_return_cache()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("WING API 클라이언트 생성 불가")

                    with act2:
                        st.markdown("**회수 송장 등록**")
                        _sel_company = st.selectbox(
                            "택배사", list(_DELIVERY_COMPANIES.keys()),
                            format_func=lambda x: f"{_DELIVERY_COMPANIES[x]} ({x})",
                            key="sel_inv_company",
                            index=list(_DELIVERY_COMPANIES.keys()).index("HANJIN"),
                        )
                        _inv_num = st.text_input("운송장번호", key="inv_number")
                        if st.button("회수 송장 등록", type="secondary", key="btn_create_invoice"):
                            if not _inv_num.strip():
                                st.error("운송장번호를 입력하세요.")
                            elif _client:
                                try:
                                    _client.create_return_invoice(
                                        receipt_id=int(_receipt_id),
                                        delivery_company_code=_sel_company,
                                        invoice_number=_inv_num.strip(),
                                        delivery_type="RETURN",
                                    )
                                    st.success(f"회수 송장 등록 완료: {_DELIVERY_COMPANIES.get(_sel_company, _sel_company)} {_inv_num}")
                                    _clear_return_cache()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("WING API 클라이언트 생성 불가")

                # 승인대기 → 반품 승인
                elif _status == "VENDOR_WAREHOUSE_CONFIRM":
                    st.markdown("**반품 승인 (환불)**")
                    st.caption("빠른환불 대상은 자동 처리됩니다.")
                    if st.button("반품 승인", type="primary", key="btn_approve"):
                        if _client:
                            try:
                                _cancel_count = int(_row["수량"]) if pd.notna(_row["수량"]) and int(_row["수량"]) > 0 else 1
                                _client.approve_return_request(int(_receipt_id), cancel_count=_cancel_count)
                                st.success(f"반품 승인 완료: {_receipt_id}")
                                _clear_return_cache()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트 생성 불가")

                # 출고중지 요청
                elif _status == "RELEASE_STOP_UNCHECKED":
                    st.warning(f"출고중지 요청 건입니다. 출고중지상태: {_row.get('출고중지상태', '-')}")
            else:
                st.caption("테이블에서 처리할 건을 선택하세요.")

    # ══════════════════════════════════════════════
    # 탭2: 전체 내역
    # ══════════════════════════════════════════════
    with tab2:
        if _all.empty:
            st.info("반품/취소 건이 없습니다.")
        else:
            # 필터
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                _ret_status_filter = st.selectbox("상태", ["전체"] + list(_STATUS_MAP.values()), key="ret_status")
            with fc2:
                _ret_type_filter = st.selectbox("유형", ["전체", "반품", "취소"], key="ret_type")
            with fc3:
                _ret_fault_filter = st.selectbox("귀책", ["전체", "고객", "셀러", "쿠팡"], key="ret_fault")

            df = _all.copy()
            if _ret_status_filter != "전체":
                df = df[df["상태"] == _ret_status_filter]
            if _ret_type_filter != "전체":
                df = df[df["유형"] == _ret_type_filter]
            if _ret_fault_filter != "전체":
                df = df[df["귀책"] == _ret_fault_filter]

            if df.empty:
                st.info("해당 조건의 반품/취소 건이 없습니다.")
            else:
                _display_cols = [
                    "계정", "접수번호", "주문번호", "유형", "상태", "접수일",
                    "상품명", "사유분류", "수량", "배송비", "귀책", "요청자",
                    "출고중지상태", "회수종류", "선환불", "회수송장",
                ]
                _show = df[[c for c in _display_cols if c in df.columns]].copy()
                _show["배송비"] = _show["배송비"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

                _evt2 = st.dataframe(
                    _show.reset_index(drop=True), use_container_width=True, hide_index=True, height=500,
                    selection_mode="single-row", on_select="rerun", key="ret_list_table",
                )

                _sel2 = _evt2.selection.rows if _evt2 and _evt2.selection else []
                if _sel2:
                    _detail_row = df.iloc[_sel2[0]]
                    _sel_detail = _detail_row["접수번호"]

                    with st.expander(f"상세 — 접수번호 {_sel_detail}", expanded=True):
                        dc1, dc2, dc3 = st.columns(3)
                        dc1.write(f"**주문번호:** {_detail_row['주문번호']}")
                        dc1.write(f"**유형:** {_detail_row['유형']}")
                        dc1.write(f"**상태:** {_detail_row['상태']}")
                        dc2.write(f"**요청자:** {_detail_row['요청자']}")
                        dc2.write(f"**귀책:** {_detail_row['귀책']}")
                        dc2.write(f"**선환불:** {_detail_row['선환불']}")
                        dc3.write(f"**출고중지상태:** {_detail_row.get('출고중지상태', '-')}")
                        dc3.write(f"**회수종류:** {_detail_row.get('회수종류', '-')}")
                        dc3.write(f"**배송비:** {_detail_row['배송비']}")

                        st.write(f"**사유:** {_detail_row.get('사유분류', '')} > {_detail_row.get('사유상세', '')} | {_detail_row.get('사유코드', '')}")
                        if _detail_row.get("비고"):
                            st.write(f"**비고:** {_detail_row['비고']}")

                        items = _detail_row.get("_return_items", [])
                        if items:
                            st.write("**반품 아이템:**")
                            item_rows = []
                            for ri in items:
                                item_rows.append({
                                    "상품명": ri.get("sellerProductName", ""),
                                    "옵션명": ri.get("vendorItemName", ""),
                                    "옵션ID": ri.get("vendorItemId", ""),
                                    "주문수량": ri.get("purchaseCount", 0),
                                    "취소수량": ri.get("cancelCount", 0),
                                    "출고상태": _RELEASE_STATUS_MAP.get(ri.get("releaseStatus", ""), ri.get("releaseStatus", "")),
                                })
                            st.dataframe(pd.DataFrame(item_rows), use_container_width=True, hide_index=True)

                        if _detail_row.get("회수송장"):
                            st.write(f"**회수송장:** {_detail_row['회수송장']}")

                st.download_button(
                    "CSV 다운로드",
                    _show.to_csv(index=False, encoding="utf-8-sig"),
                    file_name=f"returns_{date.today().isoformat()}.csv",
                    mime="text/csv",
                    key="ret_csv_dl",
                )

    # ── 탭4: 교환 ──
    with tab4:
        _ex_all = _load_exchanges_live(accounts_df)

        if _ex_all.empty:
            st.info("최근 7일간 교환 건이 없습니다.")
        else:
            # 교환 KPI
            _ex_total = len(_ex_all)
            _ex_receipt = len(_ex_all[_ex_all["_exchange_status"] == "RECEIPT"])
            _ex_progress = len(_ex_all[_ex_all["_exchange_status"] == "PROGRESS"])
            _ex_success = len(_ex_all[_ex_all["_exchange_status"] == "SUCCESS"])
            _ex_reject = len(_ex_all[_ex_all["_exchange_status"] == "REJECT"])

            ek1, ek2, ek3, ek4, ek5 = st.columns(5)
            ek1.metric("교환 전체", f"{_ex_total:,}건")
            ek2.metric("접수", f"{_ex_receipt:,}건")
            ek3.metric("진행중", f"{_ex_progress:,}건")
            ek4.metric("완료", f"{_ex_success:,}건")
            ek5.metric("불가/철회", f"{_ex_reject + len(_ex_all[_ex_all['_exchange_status'] == 'CANCEL']):,}건")

            st.divider()

            # 교환 목록
            _ex_display_cols = [
                "계정", "교환ID", "주문번호", "교환상태", "접수일",
                "상품명", "수량", "귀책", "사유", "회수상태", "재배송상태",
                "교환배송비", "거부가능", "송장입력가능",
            ]
            _ex_show = _ex_all[[c for c in _ex_display_cols if c in _ex_all.columns]].copy()
            if "교환배송비" in _ex_show.columns:
                _ex_show["교환배송비"] = _ex_show["교환배송비"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
            _ex_evt = st.dataframe(
                _ex_show.reset_index(drop=True), use_container_width=True, hide_index=True, height=400,
                selection_mode="single-row", on_select="rerun", key="ex_list_table",
            )
            _ex_sel = _ex_evt.selection.rows if _ex_evt and _ex_evt.selection else []

            # 교환 상세 (행 선택 시)
            if _ex_sel:
                _ex_row = _ex_all.iloc[_ex_sel[0]]
                _ex_id = _ex_row["교환ID"]
                _ex_acct_name = _ex_row["계정"]

                with st.expander(f"교환 상세 — {_ex_id}", expanded=True):
                    xc1, xc2, xc3 = st.columns(3)
                    xc1.write(f"**교환ID:** {_ex_id}")
                    xc1.write(f"**주문번호:** {_ex_row['주문번호']}")
                    xc1.write(f"**계정:** {_ex_acct_name}")
                    xc2.write(f"**교환상태:** {_ex_row['교환상태']}")
                    xc2.write(f"**귀책:** {_ex_row['귀책']}")
                    xc2.write(f"**사유:** {_ex_row.get('사유', '-')}")
                    xc3.write(f"**회수상태:** {_ex_row['회수상태']}")
                    xc3.write(f"**재배송상태:** {_ex_row['재배송상태']}")
                    xc3.write(f"**상품명:** {_ex_row['상품명'][:60]}")

                st.divider()

                # ── 교환 처리 (선택된 행 기준) ──
                _ex_acct, _ex_client = _client_for(_ex_acct_name)

                # 입고확인 (RECEIPT)
                if _ex_row["_exchange_status"] == "RECEIPT":
                    st.info(f"**[교환 {_ex_id}]** {_ex_row['상품명'][:80]} — 입고확인 가능")
                    if st.button("입고 확인", type="primary", key="btn_ex_confirm"):
                        if _ex_client:
                            try:
                                _ex_client.confirm_exchange_receipt(int(_ex_id))
                                st.success(f"교환 입고확인 완료: {_ex_id}")
                                _clear_return_cache()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트 생성 불가")

                # 거부 (거부가능한 건)
                if _ex_row["거부가능"] == "Y":
                    st.warning(f"**[교환 {_ex_id}]** 거부 가능")
                    _reject_code = st.selectbox("거부 사유", ["SOLDOUT", "WITHDRAW"],
                                                 format_func=lambda x: "품절" if x == "SOLDOUT" else "철회",
                                                 key="ex_reject_code")
                    if st.button("교환 거부", type="secondary", key="btn_ex_reject"):
                        if _ex_client:
                            try:
                                _ex_client.reject_exchange_request(int(_ex_id), _reject_code)
                                st.success(f"교환 거부 완료: {_ex_id} ({_reject_code})")
                                _clear_return_cache()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트 생성 불가")

                # 재배송 송장 등록 (송장입력 가능한 건)
                if _ex_row["송장입력가능"] == "Y":
                    st.info(f"**[교환 {_ex_id}]** 재배송 송장 등록 가능")
                    ei1, ei2 = st.columns(2)
                    with ei1:
                        _ex_company = st.selectbox(
                            "택배사", list(_DELIVERY_COMPANIES.keys()),
                            format_func=lambda x: f"{_DELIVERY_COMPANIES[x]} ({x})",
                            key="sel_ex_company",
                        )
                    with ei2:
                        _dg = _ex_row.get("_delivery_groups", [])
                        _shipment_box_ids = [str(g["shipmentBoxId"]) for g in _dg if g.get("shipmentBoxId")]
                        if _shipment_box_ids:
                            _sel_sbid = st.selectbox("배송번호(shipmentBoxId)", _shipment_box_ids, key="sel_ex_sbid")
                        else:
                            _sel_sbid = st.text_input("배송번호(shipmentBoxId)", key="sel_ex_sbid_input")
                    _ex_inv_num = st.text_input("운송장번호", key="ex_inv_number")

                    if st.button("재배송 송장 등록", type="primary", key="btn_ex_invoice"):
                        if not _ex_inv_num.strip():
                            st.error("운송장번호를 입력하세요.")
                        elif not _sel_sbid:
                            st.error("배송번호(shipmentBoxId)를 입력하세요.")
                        elif _ex_client:
                            try:
                                _ex_client.upload_exchange_invoice(
                                    exchange_id=int(_ex_id),
                                    shipment_box_id=int(_sel_sbid),
                                    delivery_code=_ex_company,
                                    invoice_number=_ex_inv_num.strip(),
                                )
                                st.success(f"재배송 송장 등록 완료: {_DELIVERY_COMPANIES.get(_ex_company, _ex_company)} {_ex_inv_num}")
                                _clear_return_cache()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트 생성 불가")

            # CSV 다운로드
            if not _ex_all.empty:
                st.download_button(
                    "교환 CSV 다운로드",
                    _ex_show.to_csv(index=False, encoding="utf-8-sig"),
                    file_name=f"exchanges_{date.today().isoformat()}.csv",
                    mime="text/csv",
                    key="ex_csv_dl",
                )