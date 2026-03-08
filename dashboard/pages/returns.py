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
    c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2])
    with c1:
        if st.button("새로고침", key="btn_ret_refresh", use_container_width=True,
                     type="primary", help="WING API에서 실시간 반품/교환 조회"):
            _clear_return_cache()
            st.rerun()
    with c2:
        _last = st.session_state.get("returns_last_synced")
        if _last:
            st.caption(f"마지막 조회: {_last}")
        else:
            st.caption("WING API 실시간")
    with c3:
        _ret_status_filter = st.selectbox("상태", ["전체"] + list(_STATUS_MAP.values()), key="ret_status")
    with c4:
        _ret_type_filter = st.selectbox("유형", ["전체", "반품", "취소"], key="ret_type")
    with c5:
        _ret_fault_filter = st.selectbox("귀책", ["전체", "고객", "셀러", "쿠팡"], key="ret_fault")

    # ── 데이터 로드 (실시간 API) ──
    _all = _load_returns_live(accounts_df, days=31)

    if _all.empty:
        st.info("반품/취소 건이 없습니다. '새로고침'을 눌러 조회하세요.")

    # ── 필터 적용 ──
    df = _all.copy() if not _all.empty else pd.DataFrame()
    if not df.empty:
        if _ret_status_filter != "전체":
            df = df[df["상태"] == _ret_status_filter]
        if _ret_type_filter != "전체":
            df = df[df["유형"] == _ret_type_filter]
        if _ret_fault_filter != "전체":
            df = df[df["귀책"] == _ret_fault_filter]

    # ── KPI 카드 ──
    _total = len(df)
    _pending = len(df[df["_receipt_status"].isin(["RELEASE_STOP_UNCHECKED", "RETURNS_UNCHECKED"])]) if not df.empty and "_receipt_status" in df.columns else 0
    _warehouse = len(df[df["_receipt_status"] == "VENDOR_WAREHOUSE_CONFIRM"]) if not df.empty and "_receipt_status" in df.columns else 0
    _completed = len(df[df["_receipt_status"] == "RETURNS_COMPLETED"]) if not df.empty and "_receipt_status" in df.columns else 0

    # 귀책 분포 (필터 전 전체 기준)
    _cust = len(_all[_all["귀책"] == "고객"]) if not _all.empty else 0
    _vendor = len(_all[_all["귀책"] == "셀러"]) if not _all.empty else 0
    _coup = len(_all[_all["귀책"] == "쿠팡"]) if not _all.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("조회 건수", f"{_total:,}건")
    k2.metric("미처리", f"{_pending:,}건")
    k3.metric("입고확인 대기", f"{_warehouse:,}건")
    k4.metric("처리완료", f"{_completed:,}건")
    _fault_text = f"고객 {_cust} / 셀러 {_vendor}" + (f" / 쿠팡 {_coup}" if _coup else "")
    k5.metric("귀책 분포", _fault_text if (_cust + _vendor + _coup) > 0 else "-")

    # 배송비 합계
    if not df.empty and "배송비" in df.columns:
        _seller_charge = int(df[df["배송비"] > 0]["배송비"].sum())
        _customer_charge = int(df[df["배송비"] < 0]["배송비"].abs().sum())
        if _seller_charge > 0 or _customer_charge > 0:
            st.caption(f"셀러 부담: {_seller_charge:,}원 / 고객 부담: {_customer_charge:,}원")

    st.divider()

    # ── 탭 ──
    tab1, tab2, tab3, tab4 = st.tabs(["반품 목록", "반품 처리", "회수 송장 등록", "교환"])

    # ── 탭1: 반품 목록 ──
    with tab1:
        if df.empty:
            st.info("해당 조건의 반품/취소 건이 없습니다.")
        else:
            # 표시 컬럼
            _display_cols = [
                "계정", "접수번호", "주문번호", "유형", "상태", "접수일",
                "상품명", "사유분류", "수량", "배송비", "귀책", "요청자",
                "출고중지상태", "회수종류", "선환불", "회수송장",
            ]
            _show = df[[c for c in _display_cols if c in df.columns]].copy()
            _show["배송비"] = _show["배송비"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

            st.dataframe(_show, use_container_width=True, hide_index=True, height=500)

            # 상세 조회 (접수번호 선택)
            _receipts = df["접수번호"].tolist()
            if _receipts:
                _sel_detail = st.selectbox("접수번호 선택 (상세 조회)", _receipts, key="sel_detail_receipt")
                _detail_row = df[df["접수번호"] == _sel_detail].iloc[0]

                with st.expander(f"상세 — 접수번호 {_sel_detail}", expanded=False):
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

                    # 반품 아이템 상세
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

            # CSV 다운로드
            st.download_button(
                "CSV 다운로드",
                _show.to_csv(index=False, encoding="utf-8-sig"),
                file_name=f"returns_{date.today().isoformat()}.csv",
                mime="text/csv",
                key="ret_csv_dl",
            )

    # ── 탭2: 반품 처리 ──
    with tab2:
        # 처리용 계정 선택
        _proc_acct_name = st.selectbox("처리 계정", account_names, key="ret_proc_acct")
        _proc_account = None
        if _proc_acct_name and not accounts_df.empty:
            _mask = accounts_df["account_name"] == _proc_acct_name
            if _mask.any():
                _proc_account = accounts_df[_mask].iloc[0]

        if _proc_account is None:
            st.warning("계정을 선택하세요.")
        else:
            _acct_name = _proc_account["account_name"]
            _client = create_wing_client(_proc_account)

            # 해당 계정 데이터만
            _acct_df = _all[_all["계정"] == _acct_name] if not _all.empty else pd.DataFrame()

            # ── 출고중지 요청 ──
            st.subheader("출고중지 요청")
            st.caption("상품준비중 단계에서 고객이 반품 접수 → 출고 전 중지 처리 필요")
            _stop = _acct_df[_acct_df["_receipt_status"] == "RELEASE_STOP_UNCHECKED"] if not _acct_df.empty else pd.DataFrame()
            if _stop.empty:
                st.info("출고중지 요청 건 없음")
            else:
                st.dataframe(
                    _stop[["접수번호", "주문번호", "상품명", "사유분류", "수량", "출고중지상태", "접수일"]],
                    use_container_width=True, hide_index=True,
                )

            st.divider()

            # ── 입고 확인 대기 ──
            st.subheader("입고 확인 대기")
            st.caption("반품접수(RETURNS_UNCHECKED) → 입고확인. 빠른환불 미대상이거나 회수 송장 트래킹 불가 시 처리.")
            _unchecked = _acct_df[_acct_df["_receipt_status"] == "RETURNS_UNCHECKED"] if not _acct_df.empty else pd.DataFrame()

            if _unchecked.empty:
                st.info("입고 확인 대기 건 없음")
            else:
                st.dataframe(
                    _unchecked[["접수번호", "주문번호", "상품명", "사유분류", "수량", "귀책", "선환불", "회수종류", "접수일"]],
                    use_container_width=True, hide_index=True,
                )
                uc1, uc2 = st.columns(2)
                with uc1:
                    _sel_confirm = st.selectbox("접수번호 (입고확인)", _unchecked["접수번호"].tolist(), key="sel_confirm")
                with uc2:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("입고 확인", type="primary", key="btn_confirm"):
                        if _client:
                            try:
                                _client.confirm_return_receipt(int(_sel_confirm))
                                st.success(f"입고 확인 완료: {_sel_confirm}")
                                _clear_return_cache()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트 생성 불가")

            st.divider()

            # ── 반품 승인 대기 ──
            st.subheader("반품 승인 대기")
            st.caption("입고확인(VENDOR_WAREHOUSE_CONFIRM) → 반품 승인(환불). 빠른환불 대상은 자동 처리됩니다.")
            _confirmed = _acct_df[_acct_df["_receipt_status"] == "VENDOR_WAREHOUSE_CONFIRM"] if not _acct_df.empty else pd.DataFrame()

            if _confirmed.empty:
                st.info("승인 대기 건 없음")
            else:
                st.dataframe(
                    _confirmed[["접수번호", "주문번호", "상품명", "사유분류", "수량", "귀책", "선환불", "접수일"]],
                    use_container_width=True, hide_index=True,
                )
                ac1, ac2 = st.columns(2)
                with ac1:
                    _sel_approve = st.selectbox("접수번호 (승인)", _confirmed["접수번호"].tolist(), key="sel_approve")
                with ac2:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("반품 승인", type="primary", key="btn_approve"):
                        if _client:
                            try:
                                _row = _confirmed[_confirmed["접수번호"] == _sel_approve].iloc[0]
                                _cancel_count = int(_row["수량"]) if pd.notna(_row["수량"]) and int(_row["수량"]) > 0 else 1
                                _client.approve_return_request(int(_sel_approve), cancel_count=_cancel_count)
                                st.success(f"반품 승인 완료: {_sel_approve}")
                                _clear_return_cache()
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e}")
                        else:
                            st.error("WING API 클라이언트 생성 불가")

    # ── 탭3: 회수 송장 등록 ──
    with tab3:
        # 처리용 계정 선택
        _inv_acct_name = st.selectbox("처리 계정", account_names, key="ret_inv_acct")
        _inv_account = None
        if _inv_acct_name and not accounts_df.empty:
            _mask3 = accounts_df["account_name"] == _inv_acct_name
            if _mask3.any():
                _inv_account = accounts_df[_mask3].iloc[0]

        if _inv_account is None:
            st.warning("계정을 선택하세요.")
        else:
            _acct_name3 = _inv_account["account_name"]
            _client3 = create_wing_client(_inv_account)

            st.subheader("회수 송장 등록")
            st.caption("굿스플로(반품자동연동) 미사용 시, 반품접수 상태에서 직접 회수 송장을 등록합니다.")

            _acct_df3 = _all[_all["계정"] == _acct_name3] if not _all.empty else pd.DataFrame()
            _targets = _acct_df3[_acct_df3["_receipt_status"] == "RETURNS_UNCHECKED"] if not _acct_df3.empty else pd.DataFrame()

            if _targets.empty:
                st.info("회수 송장 등록 가능한 반품 없음")
            else:
                st.dataframe(
                    _targets[["접수번호", "주문번호", "상품명", "사유분류", "수량", "요청자", "회수종류", "접수일"]],
                    use_container_width=True, hide_index=True,
                )
                st.markdown("---")

                ic1, ic2 = st.columns(2)
                with ic1:
                    _sel_inv = st.selectbox("접수번호", _targets["접수번호"].tolist(), key="sel_inv_receipt")
                    _sel_company = st.selectbox(
                        "택배사", list(_DELIVERY_COMPANIES.keys()),
                        format_func=lambda x: f"{_DELIVERY_COMPANIES[x]} ({x})",
                        key="sel_inv_company",
                    )
                with ic2:
                    _inv_num = st.text_input("운송장번호", key="inv_number")
                    _reg_num = st.text_input("택배사 회수번호 (선택)", key="reg_number")

                if st.button("회수 송장 등록", type="primary", key="btn_create_invoice"):
                    if not _inv_num.strip():
                        st.error("운송장번호를 입력하세요.")
                    elif _client3:
                        try:
                            _client3.create_return_invoice(
                                receipt_id=int(_sel_inv),
                                delivery_company_code=_sel_company,
                                invoice_number=_inv_num.strip(),
                                delivery_type="RETURN",
                                reg_number=_reg_num.strip(),
                            )
                            st.success(f"회수 송장 등록 완료: {_DELIVERY_COMPANIES.get(_sel_company, _sel_company)} {_inv_num}")
                            _clear_return_cache()
                        except CoupangWingError as e:
                            st.error(f"API 오류: {e}")
                    else:
                        st.error("WING API 클라이언트 생성 불가")

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
            st.dataframe(_ex_show, use_container_width=True, hide_index=True, height=400)

            st.divider()

            # ── 교환 처리 ──
            st.subheader("교환 처리")
            _ex_acct_name = st.selectbox("처리 계정", account_names, key="ex_proc_acct")
            _ex_account = None
            if _ex_acct_name and not accounts_df.empty:
                _ex_mask = accounts_df["account_name"] == _ex_acct_name
                if _ex_mask.any():
                    _ex_account = accounts_df[_ex_mask].iloc[0]

            if _ex_account is None:
                st.warning("계정을 선택하세요.")
            else:
                _ex_client = create_wing_client(_ex_account)
                _ex_acct_df = _ex_all[_ex_all["계정"] == _ex_acct_name]

                # 입고확인 (접수 상태)
                st.markdown("#### 입고 확인")
                st.caption("교환 접수(RECEIPT) 상태 → 입고확인 처리")
                _ex_receipts = _ex_acct_df[_ex_acct_df["_exchange_status"] == "RECEIPT"] if not _ex_acct_df.empty else pd.DataFrame()

                if _ex_receipts.empty:
                    st.info("입고확인 대기 건 없음")
                else:
                    st.dataframe(
                        _ex_receipts[["교환ID", "주문번호", "상품명", "수량", "귀책", "사유", "회수상태", "접수일"]],
                        use_container_width=True, hide_index=True,
                    )
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        _sel_ex_confirm = st.selectbox("교환ID (입고확인)", _ex_receipts["교환ID"].tolist(), key="sel_ex_confirm")
                    with ec2:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button("입고 확인", type="primary", key="btn_ex_confirm"):
                            if _ex_client:
                                try:
                                    _ex_client.confirm_exchange_receipt(int(_sel_ex_confirm))
                                    st.success(f"교환 입고확인 완료: {_sel_ex_confirm}")
                                    _clear_return_cache()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("WING API 클라이언트 생성 불가")

                st.divider()

                # 거부 처리
                st.markdown("#### 교환 거부")
                st.caption("교환 거부 가능한 건에 대해 거부 처리 (품절/철회)")
                _ex_rejectable = _ex_acct_df[_ex_acct_df["거부가능"] == "Y"] if not _ex_acct_df.empty else pd.DataFrame()

                if _ex_rejectable.empty:
                    st.info("거부 가능한 교환 건 없음")
                else:
                    st.dataframe(
                        _ex_rejectable[["교환ID", "주문번호", "상품명", "수량", "교환상태", "사유", "접수일"]],
                        use_container_width=True, hide_index=True,
                    )
                    rc1, rc2, rc3 = st.columns(3)
                    with rc1:
                        _sel_ex_reject = st.selectbox("교환ID (거부)", _ex_rejectable["교환ID"].tolist(), key="sel_ex_reject")
                    with rc2:
                        _reject_code = st.selectbox("거부 사유", ["SOLDOUT", "WITHDRAW"],
                                                     format_func=lambda x: "품절" if x == "SOLDOUT" else "철회",
                                                     key="ex_reject_code")
                    with rc3:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button("교환 거부", type="secondary", key="btn_ex_reject"):
                            if _ex_client:
                                try:
                                    _ex_client.reject_exchange_request(int(_sel_ex_reject), _reject_code)
                                    st.success(f"교환 거부 완료: {_sel_ex_reject} ({_reject_code})")
                                    _clear_return_cache()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("WING API 클라이언트 생성 불가")

                st.divider()

                # 재배송 송장 등록
                st.markdown("#### 재배송 송장 등록")
                st.caption("교환 진행중(PROGRESS) 상태에서 재배송 송장을 등록합니다.")
                _ex_invoice_targets = _ex_acct_df[_ex_acct_df["송장입력가능"] == "Y"] if not _ex_acct_df.empty else pd.DataFrame()

                if _ex_invoice_targets.empty:
                    st.info("송장 등록 가능한 교환 건 없음")
                else:
                    st.dataframe(
                        _ex_invoice_targets[["교환ID", "주문번호", "상품명", "수량", "교환상태", "재배송상태", "접수일"]],
                        use_container_width=True, hide_index=True,
                    )

                    ei1, ei2 = st.columns(2)
                    with ei1:
                        _sel_ex_inv = st.selectbox("교환ID", _ex_invoice_targets["교환ID"].tolist(), key="sel_ex_inv")
                        _ex_company = st.selectbox(
                            "택배사", list(_DELIVERY_COMPANIES.keys()),
                            format_func=lambda x: f"{_DELIVERY_COMPANIES[x]} ({x})",
                            key="sel_ex_company",
                        )
                    with ei2:
                        # shipmentBoxId 조회
                        _sel_ex_row = _ex_invoice_targets[_ex_invoice_targets["교환ID"] == _sel_ex_inv]
                        _shipment_box_ids = []
                        if not _sel_ex_row.empty:
                            _dg = _sel_ex_row.iloc[0].get("_delivery_groups", [])
                            for g in _dg:
                                sbid = g.get("shipmentBoxId")
                                if sbid:
                                    _shipment_box_ids.append(str(sbid))
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
                                    exchange_id=int(_sel_ex_inv),
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