"""
주문 관리 페이지
================
결제완료 → 발주서 → 출고/극동 워크플로우.
배송/송장은 별도 페이지(shipping.py)로 분리.
"""
import io
import logging
import os
import sys
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
    load_all_orders_from_db,
    get_instruct_orders,
    get_instruct_by_box,
    clear_order_caches,
    fmt_krw_short,
    sync_live_orders,
    can_call_api,
    STATUS_MAP,
)
logger = logging.getLogger(__name__)


def render(selected_account, accounts_df, account_names):
    st.title("주문 관리")

    # ── 상단 컨트롤 ──
    _can_call = can_call_api()
    _top_c1, _top_c2 = st.columns([2, 5])
    with _top_c1:
        if _can_call:
            if st.button("🔄 주문 새로고침", key="btn_live_refresh", use_container_width=True,
                         help="WING API에서 최근 7일 주문 즉시 조회", type="primary"):
                try:
                    with st.spinner("WING API 조회 중... (1~2분 소요)"):
                        _synced = sync_live_orders(accounts_df)
                    clear_order_caches()
                    st.success(f"✅ 완료: {_synced}건 갱신")
                except Exception as _e:
                    st.error(f"❌ 동기화 실패: {_e}")
    with _top_c2:
        _last_synced = st.session_state.get("order_last_synced")
        if _last_synced:
            st.caption(f"마지막 동기화: {_last_synced} | 10분마다 자동 동기화")

    _status_map = STATUS_MAP

    _ord_date_to_str = date.today().isoformat()
    _ord_date_from_str = (date.today() - timedelta(days=30)).isoformat()

    # ── DB에서 즉시 로드 (단일 쿼리, 30초 캐시) ──
    _all_orders = load_all_orders_from_db()

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
    _kpi_departure = _kpi_count(_all_orders, "DEPARTURE")
    _kpi_delivering = _kpi_count(_all_orders, "DELIVERING")
    _kpi_final = _kpi_count(_all_orders, "FINAL_DELIVERY")
    _instruct_all = _instruct_live[~_instruct_live["취소"]].copy() if not _instruct_live.empty else pd.DataFrame()

    # KPI 계정별 집계
    _kpi_accept = _accept_all.groupby("계정")["묶음배송번호"].nunique().to_dict() if not _accept_all.empty else {}
    _kpi_instruct = _instruct_all.groupby("계정")["묶음배송번호"].nunique().to_dict() if not _instruct_all.empty else {}

    # ── 실시간 주문 현황 KPI ──
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

    # ── 3 탭: 워크플로우 순서 (배송/송장은 별도 페이지) ──
    _ord_tab1, _ord_tab2, _ord_tab4 = st.tabs(["결제완료", "발주서", "출고/극동"])

    # ══════════════════════════════════════
    # 탭1: 결제완료 (ACCEPT)
    # ══════════════════════════════════════
    with _ord_tab1:
        st.caption("DB 기반 조회 (실시간 동기화 버튼으로 갱신)")

        # 계정 필터
        _t1_acct = st.selectbox("계정", ["전체"] + account_names, key="tab1_acct")
        _t1_data = _accept_all.copy() if not _accept_all.empty else pd.DataFrame()
        if not _t1_data.empty and _t1_acct != "전체":
            _t1_data = _t1_data[_t1_data["계정"] == _t1_acct]

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

        if _t1_data.empty:
            st.info("결제완료(ACCEPT) 상태의 주문이 없습니다.")
        else:
            _accept_display = _t1_data[["계정", "묶음배송번호", "주문번호", "상품명", "옵션명", "수량", "결제금액", "주문일", "수취인"]].copy()
            _accept_display["결제금액"] = _accept_display["결제금액"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")

            gb = GridOptionsBuilder.from_dataframe(_accept_display)
            gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
            gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
            gb.configure_column("상품명", width=250)
            gb.configure_column("옵션명", width=200)
            grid_opts = gb.build()
            AgGrid(_accept_display, gridOptions=grid_opts, height=450, theme="streamlit", key="tab1_accept_grid")

            st.divider()

            st.info("ACCEPT 주문을 상품준비중(INSTRUCT)으로 일괄 변경합니다.")
            _ack_unique = _t1_data[["계정", "묶음배송번호"]].drop_duplicates()
            _ack_total_count = len(_ack_unique)

            if st.button(f"상품준비중 처리 ({_ack_total_count}건)", type="primary", key="btn_ack_all_v2"):
                _acct_groups = _t1_data.groupby("_account_id")
                _total_success = 0
                _total_fail = 0

                for _aid, _grp in _acct_groups:
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
                    clear_order_caches()
                    st.rerun()

            # ── 주문 취소 ──
            with st.expander("주문 취소", expanded=False):
                st.caption("ACCEPT/INSTRUCT 상태의 주문을 취소합니다.")

                _cancel_acct = st.selectbox("취소할 계정", account_names, key="tab1_cancel_acct")
                _cancel_acct_row = None
                if _cancel_acct and not accounts_df.empty:
                    _mask = accounts_df["account_name"] == _cancel_acct
                    if _mask.any():
                        _cancel_acct_row = accounts_df[_mask].iloc[0]

                if _cancel_acct_row is not None:
                    _cancel_account_id = int(_cancel_acct_row["id"])
                    _cancel_client = create_wing_client(_cancel_acct_row)

                    # 공유 데이터에서 계정 필터
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
                        _cancelable_display["상태"] = _cancelable_display["상태"].map(lambda x: _status_map.get(x, x))
                        st.dataframe(_cancelable_display, use_container_width=True, hide_index=True)

                        _cancel_reasons = {
                            "SOLD_OUT": "재고 소진",
                            "PRICE_ERROR": "가격 오류",
                            "PRODUCT_ERROR": "상품 정보 오류",
                            "OTHER": "기타 사유",
                        }
                        _sel_reason = st.selectbox("취소 사유", list(_cancel_reasons.keys()),
                                                    format_func=lambda x: _cancel_reasons[x],
                                                    key="tab1_cancel_reason")
                        _cancel_detail = st.text_input("상세 사유", value=_cancel_reasons[_sel_reason], key="tab1_cancel_detail")

                        _confirm_cancel = st.checkbox(
                            f"{len(_cancelable)}건을 정말 취소하시겠습니까? (되돌릴 수 없음)",
                            key="tab1_cancel_confirm",
                        )
                        if _confirm_cancel:
                            if st.button(f"주문 취소 ({len(_cancelable)}건)", type="secondary", key="btn_cancel_ord"):
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

    # ══════════════════════════════════════
    # 탭2: 발주서
    # ══════════════════════════════════════
    with _ord_tab2:
            # ── 상단 컨트롤 ──
            _po_col1, _po_col2, _po_col3 = st.columns([3, 2, 2])
            with _po_col1:
                st.caption("INSTRUCT(상품준비중) 주문 기반 · 배송 처리하면 자동으로 사라짐")
            with _po_col2:
                if _can_call_api:
                    if st.button("🔄 주문 새로고침", key="btn_po_sync", use_container_width=True, type="primary"):
                        with st.spinner("INSTRUCT 주문 동기화 중..."):
                            _synced = _sync_live_orders()
                        st.success(f"완료 — {_synced}건 갱신")
                    load_all_orders_from_db.clear()
                    st.rerun()
            with _po_col3:
                st.caption("2시간마다 자동 동기화")

            # 상품준비중(INSTRUCT)만 발주서 대상
            _dist_orders = _instruct_all.copy() if not _instruct_all.empty else pd.DataFrame()

            # 사은품/증정품 필터링
            if not _dist_orders.empty:
                _before = len(_dist_orders)
                _dist_orders = _dist_orders[~_dist_orders["옵션명"].apply(lambda x: is_gift_item(str(x)))].copy()
                _gift_cnt = _before - len(_dist_orders)
                if _gift_cnt > 0:
                    st.caption(f"사은품/증정품 {_gift_cnt}건 제외됨")

            if _dist_orders.empty:
                st.info("발주서 대상 주문이 없습니다.")
            else:
                # 출판사 매칭 (거래처 그룹핑용)
                _pub_list = query_df_cached("SELECT name FROM publishers WHERE is_active = true ORDER BY LENGTH(name) DESC")
                _pub_names = _pub_list["name"].tolist() if not _pub_list.empty else []

                def _match_pub(row):
                    result = match_publisher_from_text(str(row.get("옵션명") or ""), _pub_names)
                    if not result:
                        result = match_publisher_from_text(str(row.get("상품명") or ""), _pub_names)
                    return result

                # ISBN 조회: listings.isbn → books (직접 매칭)
                _isbn_lookup = query_df_cached("""
                    SELECT l.coupang_product_id,
                           l.isbn as isbn,
                           b.title as db_title,
                           l.product_name as listing_name
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

                # 도서명 클렌징
                import re as _re
                _TITLE_RE_PATTERNS = [
                    r'\s*[-–]\s*2\d{3}\s*개정\s*교육과정.*$',   # - 2022 개정 교육과정
                    r'\s+2\d{3}\s*개정\s*교육과정.*$',            # 2022 개정 교육과정
                    r'\s*[-–]\s*202\d학년도\s*수능\s*연계.*$',    # - 2027학년도 수능 연계교재
                    r'\s*\(202\d년?\s*수능대비\).*$',             # (2027년 수능대비)
                    r'\s*\(202\d학년도\s*수능대비\).*$',          # (2027학년도 수능대비)
                    r'\s*:\s*202\d학년도\s*수능.*$',              # : 2027학년도 수능...
                    r'\s*:\s*슝슝.*$',                            # : 슝슝오늘출발
                    r'\s*:\s*동영상\s*강의.*$',                   # : 동영상 강의
                    r'\s*:\s*유형의\s*완성.*$',                   # : 유형의 완성
                    r'\s*#.*$',                                    # #내신필수#당일출고
                    r'\s+사은품증정\s+\S+.*$',                    # 사은품증정 이퓨처
                    r'\s+/\s*본교재.*$',                          # / 본교재 워크북
                    r'\s+\d+rd\s+edition.*$',                     # 3rd edition
                    r'\(2\d{3}년용\)',                             # (2026년용) 제거
                    r'\s+고등\s+한국교육방송공사.*$',             # 고등 한국교육방송공사
                    r'\s+한국교육방송공사.*$',                    # 한국교육방송공사
                    r'\s+고등학교\s*[123]학년.*$',                # 고등학교 N학년
                    r'\s+고등\s*[123]학년.*$',                    # 고등 N학년
                ]
                def _clean_title(title: str) -> str:
                    # 사은품+ prefix 제거
                    if title.startswith("사은품+"):
                        title = title[4:]
                    # 쉼표 이후 제거 (", 국어, 고등 3학년" 등 옵션값)
                    if "," in title:
                        title = title[:title.index(",")].strip()
                    # 정규식 패턴 순차 제거
                    for pat in _TITLE_RE_PATTERNS:
                        title = _re.sub(pat, "", title, flags=_re.IGNORECASE).strip()
                    # " : " 이후 광고문구 제거 (앞부분이 충분히 길 때만)
                    if " : " in title:
                        parts = title.split(" : ")
                        if len(parts[0]) >= 10:
                            title = parts[0].strip()
                    return title.strip()

                # 도서명/ISBN: 1) books.title 2) listing.product_name 3) 상품명(seller_product_name) 4) 옵션명
                def _resolve_book_info(row):
                    spid = str(row.get("_seller_product_id", ""))
                    info = _isbn_map.get(spid, {})
                    isbn = info.get("isbn", "")
                    title = info.get("title", "")
                    if not title:
                        title = info.get("listing_name", "")
                    if not title:
                        # 상품명(seller_product_name)이 옵션명보다 훨씬 깔끔
                        title = str(row.get("상품명", "")).strip()
                    if not title:
                        title = str(row.get("옵션명", "")).strip()
                    return pd.Series({"도서명": _clean_title(title), "ISBN": isbn})

                _dist_orders[["도서명", "ISBN"]] = _dist_orders.apply(_resolve_book_info, axis=1)
                _dist_df = _dist_orders

                # ISBN 없는 건 경고 (삭제된 상품/세트물은 원래 없으므로 정보성으로만 표시)
                _isbn_found = _dist_df["ISBN"].apply(lambda x: bool(x and str(x).strip())).sum()
                _isbn_total = len(_dist_df)
                _isbn_missing = _isbn_total - _isbn_found
                if _isbn_missing > 0:
                    st.caption(f"ℹ️ ISBN 없음: {_isbn_missing}/{_isbn_total}건 (삭제된 상품 또는 세트물은 정상)")

                # 발주서 날짜 범위: INSTRUCT 주문의 실제 주문일 범위
                if "주문일" in _dist_df.columns and not _dist_df.empty:
                    _dist_dates = _dist_df["주문일"].dropna()
                    if not _dist_dates.empty:
                        _ord_date_from_str = str(_dist_dates.min())
                        _ord_date_to_str = str(_dist_dates.max())

                _dist_df["출판사"] = _dist_df.apply(_match_pub, axis=1)
                _dist_df["거래처"] = _dist_df["출판사"].apply(resolve_distributor)

                # ── 가게명 입력 ──
                _store_name = st.text_input(
                    "가게명 (발주서 첫 줄에 표시)",
                    value=st.session_state.get("order_store_name", "잉글리쉬존"),
                    key="order_store_name_input",
                    help="예: 잉글리쉬존, 북마트"
                )
                st.session_state["order_store_name"] = _store_name

                # 거래처별 요약
                _dist_summary = _dist_df.groupby("거래처").agg(
                    건수=("도서명", "count"),
                    수량합계=("수량", "sum"),
                    금액합계=("결제금액", "sum"),
                ).reset_index().sort_values("건수", ascending=False)
                _dist_summary["금액합계"] = _dist_summary["금액합계"].apply(lambda x: f"{int(x):,}")

                st.dataframe(_dist_summary, hide_index=True, use_container_width=True)

                # ISBN 기반 중복 집계
                _dist_df["_group_key"] = _dist_df.apply(
                    lambda r: r["ISBN"] if r.get("ISBN") else r["도서명"], axis=1
                )
                _agg = _dist_df.groupby(["거래처", "출판사", "_group_key"]).agg(
                    도서명=("도서명", "first"),
                    ISBN=("ISBN", "first"),
                    주문수량=("수량", "sum"),
                ).reset_index().drop(columns=["_group_key"])
                _agg = _agg.sort_values(["거래처", "출판사", "도서명"])

                _dist_names_sorted = _dist_summary["거래처"].tolist()

                # ── Excel 생성 (쿠팡0302.xlsx 동일 포맷) ──
                # 형식: 시트=거래처, A1:C1 병합 타이틀, 이후 데이터: 도서명 | 출판사 | 수량
                # 폰트: 맑은 고딕 / 타이틀 11pt 가운데 / 데이터 10pt A·B 왼쪽 C 가운데
                # 열너비: A=57.5, B=9.0, C=13.0
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
                        # 수량을 int로 변환
                        _sdf["주문수량"] = _sdf["주문수량"].astype(int)
                        _safe = _dname[:31].replace("/", "_").replace("\\", "_")

                        # 2행부터 데이터 (1행은 타이틀용 빈 행)
                        _sdf.to_excel(writer, sheet_name=_safe, index=False, header=False, startrow=1)
                        ws = writer.sheets[_safe]

                        # ── 1행: 타이틀 (A1:C1 병합) ──
                        ws.merge_cells("A1:C1")
                        _t = ws.cell(row=1, column=1)
                        _t.value = f"{_store_name} 주문"
                        _t.font = _OXFont(name="맑은 고딕", size=11)
                        _t.alignment = _OXAlign(horizontal="center", vertical="center")

                        # ── 데이터 행 스타일 ──
                        for _r in range(2, ws.max_row + 1):
                            ws.cell(_r, 1).font = _OXFont(name="맑은 고딕", size=10)
                            ws.cell(_r, 1).alignment = _OXAlign(horizontal="left", vertical="center")
                            ws.cell(_r, 2).font = _OXFont(name="맑은 고딕", size=10)
                            ws.cell(_r, 2).alignment = _OXAlign(horizontal="left", vertical="center")
                            ws.cell(_r, 3).font = _OXFont(name="맑은 고딕", size=10)
                            ws.cell(_r, 3).alignment = _OXAlign(horizontal="center", vertical="center")

                        # ── 열 너비 ──
                        ws.column_dimensions["A"].width = 57.5
                        ws.column_dimensions["B"].width = 9.0
                        ws.column_dimensions["C"].width = 13.0

                _xl_buf.seek(0)
                _file_date = _ord_date_to_str.replace("-", "")[2:]  # YYMMDD

                st.download_button(
                    "📥 발주서 Excel 다운로드",
                    _xl_buf.getvalue(),
                    file_name=f"쿠팡{_file_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="dist_xlsx_dl",
                    type="primary",
                    use_container_width=True,
                )

                # 도서별 합산 목록
                st.subheader("도서별 주문 합산")
                _dist_filter = st.multiselect(
                    "거래처 필터", _dist_names_sorted,
                    default=_dist_names_sorted, key="dist_filter",
                )
                _filtered_agg = _agg[_agg["거래처"].isin(_dist_filter)] if _dist_filter else _agg
                _show_agg = _filtered_agg[["거래처", "ISBN", "출판사", "도서명", "주문수량"]].copy()

                gb = GridOptionsBuilder.from_dataframe(_show_agg)
                gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
                gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
                gb.configure_column("도서명", width=350)
                gb.configure_column("주문수량", width=80)
                grid_opts = gb.build()
                AgGrid(_show_agg, gridOptions=grid_opts, height=500, theme="streamlit", key="dist_grid")


    # ══════════════════════════════════════
    # 탭3: 출고/극동
    # ══════════════════════════════════════
    with _ord_tab4:
            st.caption("WING API 실시간 출고 주문 → 극동 프로그램용 엑셀 다운로드")

            _gk_col1, _gk_col2, _gk_col3 = st.columns([2, 2, 1])
            with _gk_col1:
                _gk_date_from = st.date_input("시작일", value=date.today() - timedelta(days=1), key="tab4_gk_date_from")
            with _gk_col2:
                _gk_date_to = st.date_input("종료일", value=date.today(), key="tab4_gk_date_to")
            with _gk_col3:
                _gk_status = st.selectbox("상태", ["INSTRUCT", "DEPARTURE", "DELIVERING", "FINAL_DELIVERY"], key="tab4_gk_status")

            # WING API 실시간 조회 (INSTRUCT는 상단 캐시 재사용)
            _gk_from_str = _gk_date_from.isoformat()
            _gk_to_str = _gk_date_to.isoformat()

            _gk_api_rows = []
            if _gk_status == "INSTRUCT" and not _instruct_all.empty:
                # 상단에서 이미 조회한 INSTRUCT 데이터 재사용
                for _, _row in _instruct_all.iterrows():
                    _gk_api_rows.append({
                        "옵션명": _row.get("옵션명", ""),
                        "상품명": _row.get("상품명", ""),
                        "수량": int(_row.get("수량", 0)),
                        "결제금액": int(_row.get("결제금액", 0)),
                        "_seller_product_id": _row.get("_seller_product_id", ""),
                        "계정": _row.get("계정", ""),
                    })
            else:
                with st.spinner(f"{_gk_status} 주문 조회 중..."):
                    for _, _gk_acct in accounts_df.iterrows():
                        _gk_client = create_wing_client(_gk_acct)
                        if not _gk_client:
                            continue
                        try:
                            _gk_result = _gk_client.get_all_ordersheets(_gk_from_str, _gk_to_str, status=_gk_status)
                            for _gk_os in _gk_result:
                                _gk_items = _gk_os.get("orderItems", [])
                                if not _gk_items:
                                    _gk_items = [_gk_os]
                                for _gk_item in _gk_items:
                                    _gk_api_rows.append({
                                        "옵션명": _gk_item.get("vendorItemName", ""),
                                        "상품명": _gk_item.get("sellerProductName") or _gk_os.get("sellerProductName", ""),
                                        "수량": int(_gk_item.get("shippingCount", 0) or 0),
                                        "결제금액": int(_gk_item.get("orderPrice", 0) or 0),
                                        "_seller_product_id": _gk_item.get("sellerProductId") or _gk_os.get("sellerProductId", ""),
                                        "계정": _gk_acct["account_name"],
                                    })
                        except Exception:
                            continue

            # API 결과 → DataFrame + DB에서 ISBN/도서명/출판사 매칭
            _gk_orders = pd.DataFrame(_gk_api_rows) if _gk_api_rows else pd.DataFrame()

            if not _gk_orders.empty:
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
                st.info(f"{_gk_from_str} ~ {_gk_to_str} 에 {_gk_status} 주문이 없습니다. 주문 동기화가 필요할 수 있습니다.")
            else:
                # 사은품 필터링
                _gk_before = len(_gk_orders)
                _gk_orders = _gk_orders[~_gk_orders["옵션명"].apply(lambda x: is_gift_item(str(x)))].copy()
                _gk_gift_cnt = _gk_before - len(_gk_orders)
                if _gk_gift_cnt > 0:
                    st.caption(f"사은품/증정품 {_gk_gift_cnt}건 제외됨")

                if _gk_orders.empty:
                    st.info("사은품 제외 후 주문이 없습니다.")
                else:
                    # 도서명 정리: 1) books.title 2) listing.product_name 3) 옵션명
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

                    # 테이블 표시
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
                        file_name=f"극동_{_gk_date_from.strftime('%m%d')}_{_gk_date_to.strftime('%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="gk_xlsx_dl",
                        type="primary",
                        use_container_width=True,
                    )
