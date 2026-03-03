"""상품 관리 -- Tab 5: 검색 등록"""
import logging
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

from dashboard.utils import (
    query_df, query_df_cached, run_sql, create_wing_client,
    engine, CoupangWingError,
)
from uploaders.coupang_api_uploader import CoupangAPIUploader
from core.constants import (
    BOOK_DISCOUNT_RATE, COUPANG_FEE_RATE, DEFAULT_SHIPPING_COST,
    DEFAULT_STOCK,
    determine_customer_shipping_fee,
    determine_delivery_charge_type,
    match_publisher_from_text,
)

logger = logging.getLogger(__name__)


def render_tab_search(account_id, selected_account, accounts_df, _wing_client):
    """Tab 5: 검색 등록 렌더링"""
    st.caption("교보문고에서 도서를 검색하고, 여러 권을 선택하여 여러 계정에 일괄 등록")

    # ── 교보문고 검색 함수 ──
    def _kyobo_search(keyword: str, max_results: int = 20) -> list:
        """교보문고 검색 -> 도서 정보 리스트 반환"""
        import requests as _req
        from urllib.parse import quote as _quote
        _headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        }
        import re as _re
        try:
            _r = _req.get(
                f"https://search.kyobobook.co.kr/search?keyword={_quote(keyword)}&target=total",
                headers=_headers, timeout=15,
            )
            _r.raise_for_status()
            _html = _r.text
        except Exception:
            return []

        # prod_item 블록 분리
        _starts = [m.start() for m in _re.finditer(r'<li class="prod_item">', _html)]
        _books = []
        for _i, _start in enumerate(_starts[:max_results]):
            _end = _starts[_i + 1] if _i + 1 < len(_starts) else _start + 30000
            _blk = _html[_start:_end]

            # PID, ISBN, 제목 (checkbox data 속성)
            _m_cb = _re.search(r'data-pid="([^"]*)"[^>]*data-bid="([^"]*)"[^>]*data-name="([^"]*)"', _blk)
            if not _m_cb:
                continue
            _isbn = _m_cb.group(2)
            _title = _m_cb.group(3)

            # 정가: "원" 앞 숫자 중 최대값
            _prices = [int(p.replace(",", "")) for p in _re.findall(r'(\d{1,3}(?:,\d{3})+)\s*원', _blk)]
            _original_price = max(_prices) if _prices else 0

            # 저자: prod_author_group 안의 첫 번째 a 태그
            _m_auth = _re.search(r'class="[^"]*prod_author[^"]*".*?<a[^>]*>([^<]+)</a>', _blk, _re.S)
            _author = _m_auth.group(1).strip() if _m_auth else ""

            # 출판사: prod_publish 안의 a.text
            _m_pub = _re.search(r'class="prod_publish">\s*<a[^>]*class="text"[^>]*>([^<]+)</a>', _blk, _re.S)
            _publisher = _m_pub.group(1).strip() if _m_pub else ""

            # 출간일
            _m_date = _re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', _blk)
            _pub_date = f"{_m_date.group(1)}-{_m_date.group(2).zfill(2)}-{_m_date.group(3).zfill(2)}" if _m_date else ""

            # 이미지
            _img = f"https://image.kyobobook.co.kr/images/book/xlarge/{_isbn[-3:]}/{_isbn}.jpg" if _isbn else ""

            _books.append({
                "isbn": _isbn,
                "title": _title,
                "author": _author,
                "publisher": _publisher,
                "original_price": _original_price,
                "image_url": _img,
                "description": "",
                "publish_date": _pub_date,
            })
        return _books

    # ── 검색 영역 ──
    _s_keyword = st.text_input("검색어 (제목 / 저자 / ISBN)", key="s5_keyword", placeholder="예: 2025 수능완성")

    _s_btn_search = st.button("검색", key="s5_btn_search", type="primary", disabled=not _s_keyword)

    # ── 검색 실행 ──
    if _s_btn_search and _s_keyword:
        with st.spinner("교보문고 검색 중..."):
            _results = _kyobo_search(_s_keyword, max_results=20)
        if _results:
            st.session_state["s5_results"] = _results
        else:
            st.warning("검색 결과가 없습니다.")
            st.session_state["s5_results"] = []

    # ── 검색 결과 표시 ──
    _s_results = st.session_state.get("s5_results", [])
    if _s_results:
        st.markdown(f"**검색 결과: {len(_s_results)}건**")

        # 출판사 DB 매칭을 위한 목록 로드
        _pub_df_s5 = query_df_cached("SELECT id, name, margin_rate FROM publishers WHERE is_active = true ORDER BY LENGTH(name) DESC")
        _pub_names_s5 = _pub_df_s5["name"].tolist() if not _pub_df_s5.empty else []
        _pub_map_s5 = dict(zip(_pub_df_s5["name"], _pub_df_s5["margin_rate"])) if not _pub_df_s5.empty else {}

        # 테이블 데이터 구성
        _tbl_data = []
        for _r in _s_results:
            _matched_pub = match_publisher_from_text(_r.get("publisher", ""), _pub_names_s5)
            _mr = int(_pub_map_s5.get(_matched_pub, 0)) if _matched_pub else 0
            _lp = _r.get("original_price", 0)
            _sp = int(_lp * BOOK_DISCOUNT_RATE)
            _coupang_fee = int(_sp * COUPANG_FEE_RATE)
            _supply_cost = int(_lp * (_mr / 100)) if _mr else 0
            _ship_fee = determine_customer_shipping_fee(_mr, _lp) if _mr else 0
            _ship_cost = DEFAULT_SHIPPING_COST - _ship_fee if _mr else DEFAULT_SHIPPING_COST
            _margin = _sp - _supply_cost - _coupang_fee - _ship_cost if _mr else 0

            _tbl_data.append({
                "선택": False,
                "제목": _r.get("title", "")[:60],
                "저자": (_r.get("author", "") or "")[:30],
                "출판사": _r.get("publisher", ""),
                "매칭": _matched_pub or "-",
                "매입율": f"{_mr}%" if _mr else "-",
                "정가": _lp,
                "마진": _margin if _mr else "-",
                "ISBN": _r.get("isbn", ""),
            })

        _tbl_df = pd.DataFrame(_tbl_data)
        _edited_tbl = st.data_editor(
            _tbl_df, hide_index=True, key="s5_table",
            column_config={
                "선택": st.column_config.CheckboxColumn("선택", default=False),
                "제목": st.column_config.TextColumn("제목", disabled=True, width="large"),
                "저자": st.column_config.TextColumn("저자", disabled=True),
                "출판사": st.column_config.TextColumn("출판사(원본)", disabled=True),
                "매칭": st.column_config.TextColumn("DB매칭", disabled=True),
                "매입율": st.column_config.TextColumn("매입율", disabled=True),
                "정가": st.column_config.NumberColumn("정가", format="W%d", disabled=True),
                "마진": st.column_config.TextColumn("마진", disabled=True),
                "ISBN": st.column_config.TextColumn("ISBN", disabled=True),
            },
            use_container_width=True,
        )

        # ── 선택된 도서 추출 ──
        _selected_books = []
        for _idx, _erow in _edited_tbl.iterrows():
            if _erow["선택"]:
                _orig = _s_results[_idx]
                _selected_books.append(_orig)

        if _selected_books:
            st.success(f"**{len(_selected_books)}권** 선택됨")

            # ── 카테고리 코드 ──
            _s5_category = st.number_input(
                "카테고리 코드", value=76236, key="s5_category",
                help="기본: 76236 (국내도서 > 수험서/자격증). 변경 가능.",
            )

            st.markdown("---")

            # ── 계정 선택 (Tab 4와 동일 패턴) ──
            st.markdown("**등록 계정 선택**")
            _wing_accs_s5 = accounts_df[accounts_df["wing_api_enabled"] == 1].to_dict("records")
            if not _wing_accs_s5:
                st.warning("WING API가 활성화된 계정이 없습니다.")
                st.stop()

            _acc_tbl_s5 = []
            for _acc in _wing_accs_s5:
                _acc_tbl_s5.append({
                    "선택": True,
                    "계정명": _acc["account_name"],
                    "vendorId": _acc.get("vendor_id", ""),
                    "출고지": _acc.get("outbound_shipping_code", "-"),
                    "반품센터": _acc.get("return_center_code", "-"),
                })
            _acc_df_s5 = pd.DataFrame(_acc_tbl_s5)
            _edited_acc_s5 = st.data_editor(
                _acc_df_s5, hide_index=True, key="s5_acc_editor",
                column_config={
                    "선택": st.column_config.CheckboxColumn("선택", default=True),
                    "계정명": st.column_config.TextColumn("계정명", disabled=True),
                    "vendorId": st.column_config.TextColumn("Vendor ID", disabled=True),
                    "출고지": st.column_config.TextColumn("출고지", disabled=True),
                    "반품센터": st.column_config.TextColumn("반품센터", disabled=True),
                },
                use_container_width=True,
            )

            _sel_accs_s5 = []
            for _idx, _erow in _edited_acc_s5.iterrows():
                if _erow["선택"]:
                    for _acc in _wing_accs_s5:
                        if _acc["account_name"] == _erow["계정명"]:
                            _sel_accs_s5.append(_acc)
                            break

            _n_books = len(_selected_books)
            _n_accs = len(_sel_accs_s5)
            _total_ops = _n_books * _n_accs
            st.caption(f"**{_n_books}권 x {_n_accs}계정 = {_total_ops}건** 등록 예정")

            st.markdown("---")

            # ── 일괄 등록 ──
            _s5_btn = st.button(
                f"일괄 등록 ({_total_ops}건)",
                type="primary", key="s5_btn_register",
                disabled=(_n_accs == 0),
            )

            if _s5_btn and _total_ops > 0:
                _prog = st.progress(0, text="등록 준비 중...")
                _res_container = st.container()
                _ok_list, _fail_list = [], []
                _step = 0

                for _book in _selected_books:
                    _b_isbn = _book.get("isbn", "")
                    _b_title = _book.get("title", "")
                    _b_author = _book.get("author", "")
                    _b_publisher = _book.get("publisher", "")
                    _b_lp = _book.get("original_price", 0)
                    _b_sp = int(_b_lp * BOOK_DISCOUNT_RATE)
                    _b_image = _book.get("image_url", "")
                    _b_desc = _book.get("description", "") or "상세페이지 참조"

                    # 출판사 매칭 -> 매입율
                    _b_matched = match_publisher_from_text(_b_publisher, _pub_names_s5)
                    _b_mr = int(_pub_map_s5.get(_b_matched, 65)) if _b_matched else 65

                    _product_data = {
                        "product_name": _b_title,
                        "publisher": _b_publisher,
                        "author": _b_author,
                        "isbn": _b_isbn,
                        "original_price": _b_lp,
                        "sale_price": _b_sp,
                        "main_image_url": _b_image,
                        "description": _b_desc,
                        "shipping_policy": "free",
                        "margin_rate": _b_mr,
                    }

                    for _acc in _sel_accs_s5:
                        _step += 1
                        _acc_name = _acc["account_name"]
                        _prog.progress(_step / _total_ops, text=f"[{_step}/{_total_ops}] {_b_title[:30]}... -> {_acc_name}")

                        _out_code = str(_acc.get("outbound_shipping_code", ""))
                        _ret_code = str(_acc.get("return_center_code", ""))

                        if not _out_code or not _ret_code:
                            _fail_list.append({"도서": _b_title[:40], "계정": _acc_name, "결과": "출고지/반품지 코드 미설정"})
                            continue

                        _client = create_wing_client(_acc)
                        if _client is None:
                            _fail_list.append({"도서": _b_title[:40], "계정": _acc_name, "결과": "API 키 미설정"})
                            continue

                        _uploader = CoupangAPIUploader(_client, vendor_user_id=_acc_name)
                        try:
                            _res = _uploader.upload_product(
                                _product_data, _out_code, _ret_code,
                                dashboard_override=True,
                                category_code=_s5_category,
                            )
                            if _res["success"]:
                                _sid = _res["seller_product_id"]
                                _ok_list.append({"도서": _b_title[:40], "계정": _acc_name, "쿠팡ID": _sid, "결과": "성공"})
                                # DB INSERT
                                _b_dct, _b_dc, _b_fsoa = determine_delivery_charge_type(_b_mr, _b_lp)
                                try:
                                    with engine.connect() as conn:
                                        conn.execute(text("""
                                            INSERT INTO listings
                                            (account_id, isbn, coupang_product_id,
                                             coupang_status, sale_price, original_price, product_name,
                                             stock_quantity, delivery_charge_type, delivery_charge, free_ship_over_amount,
                                             synced_at)
                                            VALUES (:aid, :isbn, :cid, 'active', :sp, :op, :pn,
                                                    :stock, :dct, :dc, :fsoa, :now)
                                            ON CONFLICT DO NOTHING
                                        """), {
                                            "aid": int(_acc["id"]),
                                            "isbn": _b_isbn,
                                            "cid": _sid,
                                            "sp": _b_sp,
                                            "op": _b_lp,
                                            "pn": _b_title,
                                            "stock": DEFAULT_STOCK,
                                            "dct": _b_dct, "dc": _b_dc, "fsoa": _b_fsoa,
                                            "now": datetime.now().isoformat(),
                                        })
                                        conn.commit()
                                except Exception as _db_e:
                                    pass  # DB 실패는 무시 (다음 sync에서 잡힘)
                            else:
                                _fail_list.append({"도서": _b_title[:40], "계정": _acc_name, "결과": _res.get("message", "")[:100]})
                        except Exception as _e:
                            _fail_list.append({"도서": _b_title[:40], "계정": _acc_name, "결과": str(_e)[:100]})

                _prog.progress(1.0, text="완료!")
                with _res_container:
                    if _ok_list:
                        st.success(f"성공: {len(_ok_list)}건")
                        st.dataframe(pd.DataFrame(_ok_list), use_container_width=True, hide_index=True)
                    if _fail_list:
                        st.error(f"실패: {len(_fail_list)}건")
                        st.dataframe(pd.DataFrame(_fail_list), use_container_width=True, hide_index=True)
                    if not _ok_list and not _fail_list:
                        st.info("등록할 항목이 없습니다.")
                query_df.clear()
