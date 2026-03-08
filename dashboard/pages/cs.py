"""
CS 관리 페이지
==============
WING API 실시간 — 상품별 고객문의 + 쿠팡 고객센터 문의 조회/답변.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from core.api.wing_client import CoupangWingError
from dashboard.utils import create_wing_client

logger = logging.getLogger(__name__)

_STATUS_MAP = {
    "NOANSWER": "미답변",
    "ANSWERED": "답변완료",
    "ALL": "전체",
}

_CS_STATUS_MAP = {
    "NO_ANSWER": "미답변",
    "ANSWER": "답변완료",
    "TRANSFER": "미확인",
    "NONE": "전체",
}


# ─────────────────────────────────────────────
# 상품별 고객문의 로드
# ─────────────────────────────────────────────

def _parse_online_inquiry(acct_name: str, item: dict) -> dict:
    comments = item.get("commentDtoList") or []
    answered = len(comments) > 0
    return {
        "계정": acct_name,
        "문의ID": item.get("inquiryId"),
        "상품ID": item.get("sellerProductId"),
        "옵션ID": item.get("vendorItemId"),
        "문의내용": item.get("content", ""),
        "문의일시": (item.get("inquiryAt") or "")[:19].replace("T", " "),
        "답변상태": "답변완료" if answered else "미답변",
        "답변수": len(comments),
        "_comments": comments,
        "_raw": item,
    }


def _load_online_inquiries(accounts_df, answered_type="ALL"):
    cache_key = "_cs_online_cache"
    ts_key = "_cs_online_ts"
    now = time.time()
    if now - st.session_state.get(ts_key, 0) < 30 and cache_key in st.session_state:
        return st.session_state[cache_key]

    _today = date.today()
    _from = (_today - timedelta(days=6)).isoformat()
    _to = _today.isoformat()

    acct_clients = []
    for _, acct in accounts_df.iterrows():
        client = create_wing_client(acct)
        if client:
            acct_clients.append((acct, client))

    all_rows = []
    if acct_clients:
        def _fetch(acct, client):
            try:
                return acct, client.get_all_online_inquiries(_from, _to, answered_type)
            except Exception as e:
                logger.warning(f"[{acct['account_name']}] 고객문의 조회 실패: {e}")
                return acct, []

        with ThreadPoolExecutor(max_workers=min(len(acct_clients), 10)) as pool:
            futures = [pool.submit(_fetch, acct, client) for acct, client in acct_clients]
            for f in as_completed(futures):
                acct, items = f.result()
                for item in items:
                    all_rows.append(_parse_online_inquiry(acct["account_name"], item))

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    st.session_state[cache_key] = df
    st.session_state[ts_key] = time.time()
    return df


# ─────────────────────────────────────────────
# 쿠팡 고객센터 문의 로드
# ─────────────────────────────────────────────

def _parse_callcenter_inquiry(acct_name: str, item: dict) -> dict:
    replies = item.get("replies") or []
    need_answer = any(r.get("needAnswer") for r in replies)
    status = item.get("csPartnerCounselingStatus", "")

    # 상태 한글 매핑
    if status == "requestAnswer":
        status_kr = "미답변"
    elif status == "answered":
        status_kr = "답변완료"
    else:
        status_kr = status

    # 마지막 상담사 이관글 (parentAnswerId 용)
    last_agent_reply = None
    for r in replies:
        if r.get("answerType") == "csAgent" and r.get("partnerTransferStatus") == "requestAnswer":
            last_agent_reply = r

    return {
        "계정": acct_name,
        "상담번호": item.get("inquiryId"),
        "문의유형": item.get("receiptCategory", ""),
        "상품명": item.get("itemName", ""),
        "옵션ID": item.get("vendorItemId"),
        "문의내용": item.get("content", ""),
        "문의일시": (item.get("inquiryAt") or "")[:19].replace("T", " "),
        "주문번호": item.get("orderId"),
        "답변상태": status_kr,
        "답변필요": "Y" if need_answer else "N",
        "답변수": len(replies),
        "_status": status,
        "_inquiry_status": item.get("inquiryStatus", ""),
        "_replies": replies,
        "_last_agent_reply": last_agent_reply,
        "_raw": item,
    }


def _load_callcenter_inquiries(accounts_df, status="NONE"):
    cache_key = "_cs_callcenter_cache"
    ts_key = "_cs_callcenter_ts"
    now = time.time()
    if now - st.session_state.get(ts_key, 0) < 30 and cache_key in st.session_state:
        return st.session_state[cache_key]

    _today = date.today()
    _from = (_today - timedelta(days=6)).isoformat()
    _to = _today.isoformat()

    acct_clients = []
    for _, acct in accounts_df.iterrows():
        client = create_wing_client(acct)
        if client:
            acct_clients.append((acct, client))

    all_rows = []
    if acct_clients:
        def _fetch(acct, client):
            try:
                return acct, client.get_all_callcenter_inquiries(_from, _to, status)
            except Exception as e:
                logger.warning(f"[{acct['account_name']}] 콜센터문의 조회 실패: {e}")
                return acct, []

        with ThreadPoolExecutor(max_workers=min(len(acct_clients), 10)) as pool:
            futures = [pool.submit(_fetch, acct, client) for acct, client in acct_clients]
            for f in as_completed(futures):
                acct, items = f.result()
                for item in items:
                    all_rows.append(_parse_callcenter_inquiry(acct["account_name"], item))

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
    st.session_state[cache_key] = df
    st.session_state[ts_key] = time.time()
    st.session_state["cs_last_synced"] = datetime.now().strftime("%H:%M:%S")
    return df


def _clear_cs_cache():
    for k in ["_cs_online_cache", "_cs_online_ts", "_cs_callcenter_cache", "_cs_callcenter_ts"]:
        st.session_state.pop(k, None)


# ─────────────────────────────────────────────
# 렌더
# ─────────────────────────────────────────────

def render(selected_account, accounts_df, account_names):
    st.title("CS 관리")

    # 상단
    c1, c2 = st.columns([2, 5])
    with c1:
        if st.button("CS 새로고침", type="primary", key="btn_cs_refresh",
                     use_container_width=True, help="WING API 실시간 조회"):
            _clear_cs_cache()
            st.rerun()
    with c2:
        _last = st.session_state.get("cs_last_synced")
        if _last:
            st.caption(f"마지막 조회: {_last} (최근 7일, WING API 실시간)")
        else:
            st.caption("WING API 실시간 (최근 7일)")

    # 탭
    tab1, tab2 = st.tabs(["상품별 고객문의", "쿠팡 고객센터 문의"])

    # ═══ 탭1: 상품별 고객문의 ═══
    with tab1:
        _online = _load_online_inquiries(accounts_df)

        if _online.empty:
            st.info("최근 7일 고객문의가 없습니다.")
        else:
            # KPI
            _total = len(_online)
            _no_answer = len(_online[_online["답변상태"] == "미답변"])
            _answered = len(_online[_online["답변상태"] == "답변완료"])

            k1, k2, k3 = st.columns(3)
            k1.metric("총 문의", f"{_total:,}건")
            k2.metric("미답변", f"{_no_answer:,}건")
            k3.metric("답변완료", f"{_answered:,}건")

            st.divider()

            # 필터
            _filter = st.selectbox("답변상태", ["전체", "미답변", "답변완료"], key="cs_online_filter")
            df = _online.copy()
            if _filter != "전체":
                df = df[df["답변상태"] == _filter]

            if df.empty:
                st.info("해당 조건의 문의가 없습니다.")
            else:
                _show_cols = ["계정", "문의ID", "답변상태", "문의내용", "문의일시", "답변수"]
                _show_df = df[[c for c in _show_cols if c in df.columns]].reset_index(drop=True)
                _event = st.dataframe(
                    _show_df, use_container_width=True, hide_index=True, height=400,
                    selection_mode="single-row", on_select="rerun", key="cs_online_table",
                )

                # 행 클릭 → 상세
                _sel_rows = _event.selection.rows if _event and _event.selection else []
                _sel_idx = _sel_rows[0] if _sel_rows else 0
                _row = df.iloc[_sel_idx]
                _sel_id = _row["문의ID"]

                with st.expander(f"문의 상세 — {_sel_id}", expanded=bool(_sel_rows)):
                    st.write(f"**문의내용:** {_row['문의내용']}")
                    st.write(f"**문의일시:** {_row['문의일시']}")
                    st.write(f"**상품ID:** {_row.get('상품ID', '-')} | **옵션ID:** {_row.get('옵션ID', '-')}")

                    comments = _row.get("_comments", [])
                    if comments:
                        st.write("**답변:**")
                        for cm in comments:
                            st.markdown(f"> {cm.get('content', '')}")
                            st.caption(f"— {cm.get('inquiryCommentAt', '')[:19]}")

                # 답변 작성 (미답변만)
                if _row["답변상태"] == "미답변":
                    st.markdown("---")
                    st.subheader("답변 작성")
                    _reply_acct = st.selectbox("답변 계정", account_names, key="cs_online_reply_acct")
                    _reply_content = st.text_area("답변 내용", key="cs_online_reply_content", height=100)

                    if st.button("답변 등록", type="primary", key="btn_online_reply"):
                        if not _reply_content.strip():
                            st.error("답변 내용을 입력하세요.")
                        else:
                            _reply_account = None
                            if _reply_acct and not accounts_df.empty:
                                _m = accounts_df["account_name"] == _reply_acct
                                if _m.any():
                                    _reply_account = accounts_df[_m].iloc[0]
                            _cl = create_wing_client(_reply_account) if _reply_account is not None else None
                            if _cl:
                                try:
                                    _cl.reply_online_inquiry(
                                        inquiry_id=int(_sel_id),
                                        content=_reply_content.strip(),
                                        reply_by=str(_reply_account["vendor_id"]),
                                    )
                                    st.success(f"답변 등록 완료: 문의 {_sel_id}")
                                    _clear_cs_cache()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("API 클라이언트 생성 불가")

    # ═══ 탭2: 쿠팡 고객센터 문의 ═══
    with tab2:
        _cc = _load_callcenter_inquiries(accounts_df)

        if _cc.empty:
            st.info("최근 7일 고객센터 문의가 없습니다.")
        else:
            # KPI
            _cc_total = len(_cc)
            _cc_no = len(_cc[_cc["답변상태"] == "미답변"])
            _cc_transfer = len(_cc[_cc["답변필요"] == "N"])
            _cc_answered = len(_cc[_cc["답변상태"] == "답변완료"])

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("총 문의", f"{_cc_total:,}건")
            k2.metric("미답변", f"{_cc_no:,}건")
            k3.metric("답변완료", f"{_cc_answered:,}건")
            k4.metric("미확인(이관)", f"{_cc_transfer:,}건")

            st.divider()

            # 필터
            _cc_filter = st.selectbox("답변상태", ["전체", "미답변", "답변완료"], key="cs_cc_filter")
            cc_df = _cc.copy()
            if _cc_filter != "전체":
                cc_df = cc_df[cc_df["답변상태"] == _cc_filter]

            if cc_df.empty:
                st.info("해당 조건의 문의가 없습니다.")
            else:
                _cc_show = ["계정", "상담번호", "답변상태", "답변필요", "문의유형", "상품명", "문의일시", "주문번호", "답변수"]
                _cc_show_df = cc_df[[c for c in _cc_show if c in cc_df.columns]].reset_index(drop=True)
                _cc_event = st.dataframe(
                    _cc_show_df, use_container_width=True, hide_index=True, height=400,
                    selection_mode="single-row", on_select="rerun", key="cs_cc_table",
                )

                # 행 클릭 → 상세
                _cc_sel_rows = _cc_event.selection.rows if _cc_event and _cc_event.selection else []
                _cc_sel_idx = _cc_sel_rows[0] if _cc_sel_rows else 0
                _cc_row = cc_df.iloc[_cc_sel_idx]
                _cc_sel = _cc_row["상담번호"]

                with st.expander(f"상담 상세 — {_cc_sel}", expanded=bool(_cc_sel_rows)):
                    st.write(f"**문의유형:** {_cc_row.get('문의유형', '-')}")
                    st.write(f"**상품명:** {_cc_row.get('상품명', '-')}")
                    st.write(f"**문의일시:** {_cc_row['문의일시']}")
                    st.write(f"**주문번호:** {_cc_row.get('주문번호', '-')}")

                    replies = _cc_row.get("_replies", [])
                    if replies:
                        st.write("**상담 이력:**")
                        for r in replies:
                            _type = "상담사" if r.get("answerType") == "csAgent" else "판매자"
                            _name = r.get("receptionistName", "")
                            _at = (r.get("replyAt") or "")[:19].replace("T", " ")
                            _need = " (답변필요)" if r.get("needAnswer") else ""
                            st.markdown(f"**[{_type}] {_name}** {_at}{_need}")
                            st.markdown(f"> {r.get('content', '')}")
                            st.markdown("")

                # 답변/확인 처리
                if _cc_row.get("_inquiry_status") == "progress":
                    _last_agent = _cc_row.get("_last_agent_reply")

                    if _cc_row["_status"] == "requestAnswer" and _last_agent:
                        st.markdown("---")
                        st.subheader("답변 작성")
                        _cc_reply_acct = st.selectbox("답변 계정", account_names, key="cs_cc_reply_acct")
                        _cc_content = st.text_area("답변 내용 (2~1000자)", key="cs_cc_content", height=100)
                        _parent_id = _last_agent.get("answerId")

                        if st.button("답변 등록", type="primary", key="btn_cc_reply"):
                            if not _cc_content.strip() or len(_cc_content.strip()) < 2:
                                st.error("답변 내용을 2자 이상 입력하세요.")
                            else:
                                _cc_account = None
                                if _cc_reply_acct and not accounts_df.empty:
                                    _m = accounts_df["account_name"] == _cc_reply_acct
                                    if _m.any():
                                        _cc_account = accounts_df[_m].iloc[0]
                                _cl = create_wing_client(_cc_account) if _cc_account is not None else None
                                if _cl:
                                    try:
                                        _cl.reply_callcenter_inquiry(
                                            inquiry_id=int(_cc_sel),
                                            content=_cc_content.strip(),
                                            reply_by=str(_cc_account["vendor_id"]),
                                            parent_answer_id=int(_parent_id),
                                        )
                                        st.success(f"답변 등록 완료: 상담번호 {_cc_sel}")
                                        _clear_cs_cache()
                                    except CoupangWingError as e:
                                        st.error(f"API 오류: {e}")
                                else:
                                    st.error("API 클라이언트 생성 불가")

                    # 미확인(TRANSFER) 상태 → 확인 처리
                    if _cc_row["답변필요"] == "N" and _cc_row["_status"] != "answered":
                        st.markdown("---")
                        st.subheader("문의 확인 처리")
                        st.caption("쿠팡이 상담 완료한 이관건 — 판매자 확인 처리")
                        _confirm_acct = st.selectbox("확인 계정", account_names, key="cs_cc_confirm_acct")

                        if st.button("확인 처리", type="primary", key="btn_cc_confirm"):
                            _cf_account = None
                            if _confirm_acct and not accounts_df.empty:
                                _m = accounts_df["account_name"] == _confirm_acct
                                if _m.any():
                                    _cf_account = accounts_df[_m].iloc[0]
                            _cl = create_wing_client(_cf_account) if _cf_account is not None else None
                            if _cl:
                                try:
                                    _cl.confirm_callcenter_inquiry(
                                        inquiry_id=int(_cc_sel),
                                        confirm_by=str(_cf_account["vendor_id"]),
                                    )
                                    st.success(f"확인 처리 완료: 상담번호 {_cc_sel}")
                                    _clear_cs_cache()
                                except CoupangWingError as e:
                                    st.error(f"API 오류: {e}")
                            else:
                                st.error("API 클라이언트 생성 불가")
