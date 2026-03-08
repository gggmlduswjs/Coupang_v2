"""상품조회 페이지 — 전체 대시보드 + 계정별 테이블 + 불일치/누락 관리"""
import logging
import json as _json

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from dashboard.utils import (
    query_df, query_df_cached, run_sql, create_wing_client,
    fmt_money_df, fmt_krw, CoupangWingError,
)
from core.constants import COUPANG_FEE_RATE, DEFAULT_SHIPPING_COST

logger = logging.getLogger(__name__)

# ── 브랜드 별칭 (products_list.py 와 동일) ──
_BRAND_ALIAS = {
    "크라운출판사": "크라운", "에듀크라운": "크라운", "이찬석": "크라운", "김준한": "크라운",
    "안혜숙": "크라운", "노수정": "크라운",
    "영진닷컴": "영진", "영진.com": "영진", "영진com": "영진", "영진.com(영진닷컴)": "영진",
    "영진com 영진닷컴": "영진", "영진정보연구소": "영진", "홍태성": "영진",
    "이노플리아": "영진", "웅진북센": "영진", "일마": "영진",
    "이기적": "영진", "이기적컴활": "영진", "이기적 컴활1급 필기기본서": "영진",
    "이기적 컴퓨터활용능력": "영진", "박윤정": "영진",
    "매스티안 R&D 센터": "매스티안", "매스티안 편집부": "매스티안",
    "창의사고력 수학 팩토 세트": "매스티안", "미메시스": "매스티안",
    "소마셈": "소마", "soma": "소마", "소마출판사": "소마", "소마사고력수학": "소마",
    "소마사고력수학 연구소": "소마", "soma(소마)": "소마",
    "씨투엠": "씨투엠에듀", "씨투엠에듀(C2M EDU)": "씨투엠에듀",
    "플라토 세트": "씨투엠에듀", "플라토": "씨투엠에듀", "수학독해 세트": "씨투엠에듀",
    "해람북스(구 북스홀릭)": "해람북스", "송설북": "해람북스", "해람북스기획팀": "해람북스",
    "해림북스": "해람북스", "방과후교육연구회": "해람북스", "기획팀": "해람북스",
    "NE능률": "능률교육", "엔이능률": "능률교육", "능률교": "능률교육",
    "신사고": "좋은책신사고", "홍범준, 신사고수학콘텐츠연구회": "좋은책신사고",
    "홍범준": "좋은책신사고", "홍범준 , 좋은책신사고 편집부": "좋은책신사고",
    "신사고초등콘텐츠연구회": "좋은책신사고", "신사고국어콘텐츠연구회": "좋은책신사고",
    "쎈": "좋은책신사고", "쎈B": "좋은책신사고", "쎈 공통수학": "좋은책신사고",
    "쎈 미적분": "좋은책신사고", "라이트쎈": "좋은책신사고", "일품": "좋은책신사고",
    "우공비": "좋은책신사고",
    "이지스에듀": "이지스퍼블리싱", "이지스에듀(이지스퍼블리싱)": "이지스퍼블리싱",
    "이지퍼블리싱": "이지스퍼블리싱", "이성용": "이지스퍼블리싱",
    "EBS한국교육방송공사": "EBS", "한국교육방송공사(EBSi)": "EBS",
    "한국교육방송공사(초등)": "EBS", "EBS교육방송": "EBS",
    "ebs": "EBS", "EBSI": "EBS", "EBS 수능완성": "EBS",
    "기출의 미래": "EBS", "수능특강": "한국교육방송공사",
    "수경": "수경출판사", "수경출판사(학습)": "수경출판사", "수경수학콘텐츠연구소": "수경출판사",
    "자이스토리": "수경출판사", "수력충전": "수경출판사",
    "이퓨쳐": "이퓨처",
    "마더텅 편집부": "마더텅", "마덩텅": "마더텅",
    "풍산자": "지학사", "지학사(학습)": "지학사",
    "비상": "비상교육", "VISANG교육": "비상교육", "비상ESN": "비상교육",
    "비상교육 편집부": "비상교육", "비상교육편집부": "비상교육",
    "오투": "비상교육", "개념+유형": "비상교육", "개념유형": "비상교육",
    "유형만렙": "비상교육", "유형만렙 중학 수학": "비상교육",
    "REXmedia(렉스미디어)": "렉스미디어", "REXmedia 렉스미디어": "렉스미디어",
    "렉스기획팀": "렉스미디어", "렉스디어": "렉스미디어",
    "기사북닷컴": "크라운", "가을책방": "길벗", "길벗출판사": "길벗",
    "환상감자": "길벗", "피피티프로": "길벗", "디렌드라신하": "길벗", "고경희": "길벗",
    "마주현(워킹노마드)": "길벗",
    "아소미디어(아카데미소프트)": "아카데미소프트", "아소미디어": "아카데미소프트",
    "아카데미소프트사": "아카데미소프트", "아케데미소프트": "아카데미소프트",
    "KIE 기획연구실": "아카데미소프트", "KIE 기획연구실 감수": "아카데미소프트",
    "KIE기획연구실감수": "아카데미소프트", "코딩이지": "아카데미소프트",
    "씨엔씨에듀": "아카데미소프트", "코딩아카데미": "아카데미소프트",
    "동아출판": "동아", "동아출판사": "동아", "동아출판편집부": "동아", "동아출판 수학팀": "동아",
    "히어로": "동아",
    "마린북스 교재개발팀": "마린북스",
    "류은희": "렉스미디어닷넷", "조준현": "렉스미디어닷넷", "김상민": "렉스미디어닷넷",
    "이투스에듀 수학개발팀": "이투스북", "고쟁이": "이투스북",
    "수학의 바이블개념ON": "이투스북", "북마트": "이투스북",
    "에듀원편집부": "에듀원", "에듀원 편집부": "에듀원", "에듀윈": "에듀원",
    "백발백중 100발 100중": "에듀원", "아이와함께": "에듀원", "브랜드없음": "에듀원",
    "(주)에듀플라자": "에듀플라자", "에듀플러스": "에듀플라자",
    "내신콘서트": "에듀플라자",
    "베스트교육(베스트콜렉션)": "베스트콜렉션", "베스트컬렉션": "베스트콜렉션",
    "베스트교육": "베스트콜렉션",
    "디딤돌교육(학습)": "디딤돌", "디딤돌 편집부": "디딤돌",
    "디딤돌교육 학습": "디딤돌", "디딤돌 초등수학 연구소": "디딤돌",
    "꿈을 담는 틀": "꿈을담는틀", "꿈틀": "꿈을담는틀",
    "미래엔": "미래엔에듀",
    "Bricks": "사회평론", "BRICKS READING": "사회평론",
    "Bricks Reading Nonfiction": "사회평론", "브릭스": "사회평론",
    "천재교육": "진학사", "천재": "진학사",
    "시대고시기획": "시대고시",
    "빅식스": "해람북스", "제이북스": "비상교육",
    "e-future": "이퓨처", "이퓨쳐(e-future)": "이퓨처",
    "에듀왕": "에듀원", "에듀왕(왕수학)": "에듀원",
    "아이베이비북": "해람북스",
    "일품 중등수학 2-2": "좋은책신사고",
    "완자 기출PICK 중학 과학": "비상교육", "완자 기출PICK 중학 사회": "비상교육",
    "개념원리 RPM 알피엠 확률과통계": "개념원리",
    "2026 마더텅 전국연합 학력평가 기출문제집 고1 한국사": "마더텅",
    "Full수록(풀수록) 전국연합 모의고사 국어영역 고1": "비상교육",
    "밀크북(milkbook)": "해람북스",
}


def _resolve_supply_rate(row, pub_rates):
    """공급율 결정: publishers 직접 > 브랜드 별칭 > books 출판사 > 기본 65%"""
    if pd.notna(row.get("_pub_rate")):
        return float(row["_pub_rate"])
    brand = str(row.get("출판사", ""))
    alias = _BRAND_ALIAS.get(brand)
    if alias and alias in pub_rates:
        return float(pub_rates[alias])
    book_pub = row.get("_book_pub")
    if pd.notna(book_pub) and book_pub:
        if book_pub in pub_rates:
            return float(pub_rates[book_pub])
        alias2 = _BRAND_ALIAS.get(book_pub)
        if alias2 and alias2 in pub_rates:
            return float(pub_rates[alias2])
    return 0.65


def _calc_margin(df, pub_rates):
    """순마진 계산 + 공급율/배송 표시 컬럼 추가"""
    df["_supply_rate"] = df.apply(lambda r: _resolve_supply_rate(r, pub_rates), axis=1)
    _lp = df["정가"].fillna(0).astype(int)
    _sp = df["판매가"].fillna(0).astype(int)
    _sr = df["_supply_rate"].astype(float)
    _supply = (_lp * _sr).astype(int)
    _fee = (_sp * COUPANG_FEE_RATE).astype(int)
    _margin = _sp - _supply - _fee
    _customer_fee = df["배송비"].fillna(0).astype(int)
    _ship_cost = (DEFAULT_SHIPPING_COST - _customer_fee).clip(lower=0)
    df["순마진"] = (_margin - _ship_cost).astype(int)
    df["공급율"] = (_sr * 100).round(0).astype(int).astype(str) + "%"
    df.drop(columns=["_supply_rate", "_pub_rate", "_book_pub"], inplace=True, errors="ignore")
    # 상태 한글
    _sl = {"active": "판매중", "paused": "판매중지", "pending": "대기", "sold_out": "품절", "rejected": "반려"}
    df["상태"] = df["상태"].map(_sl).fillna(df["상태"])
    # 배송 표시
    def _fmt_ship(row):
        t = str(row.get("배송유형", "") or "")
        c = int(row.get("배송비", 0) or 0)
        if t == "FREE":
            return "무료배송"
        if t == "CONDITIONAL_FREE":
            if c <= 0:
                return "조건부무료"
            sr_str = str(row.get("공급율", "65%") or "65%")
            sr_pct = int(sr_str.replace("%", "").strip() or "65")
            if sr_pct > 70:
                thr = "6만"
            elif sr_pct > 67:
                thr = "3만"
            elif sr_pct > 65:
                thr = "2.5만"
            else:
                thr = "2만"
            return f"조건부({c:,}원/{thr}↑무료)"
        if t == "NOT_FREE":
            return f"유료({c:,}원)"
        return t or "-"
    df["배송"] = df.apply(_fmt_ship, axis=1)
    return df


def _load_listings(account_id, status_filter="전체", search_q=""):
    """계정별 listings 로드"""
    where_parts = ["l.account_id = :acct_id"]
    params = {"acct_id": account_id}
    _filter_map = {"판매중": "active", "판매중지": "paused", "대기": "pending", "품절": "sold_out", "반려": "rejected"}
    if status_filter != "전체":
        where_parts.append("l.coupang_status = :status")
        params["status"] = _filter_map.get(status_filter, status_filter)
    if search_q:
        where_parts.append("(l.product_name LIKE :sq OR l.isbn LIKE :sq OR CAST(l.coupang_product_id AS TEXT) LIKE :sq)")
        params["sq"] = f"%{search_q}%"
    where_sql = " AND ".join(where_parts)
    return query_df(f"""
        SELECT COALESCE(l.product_name, '(미등록)') as 상품명,
               COALESCE(l.original_price, 0) as 정가,
               l.sale_price as 판매가,
               l.delivery_charge_type as 배송유형,
               COALESCE(l.delivery_charge, 0) as 배송비,
               COALESCE(l.stock_quantity, 10) as 재고,
               l.coupang_status as 상태,
               l.isbn as "ISBN",
               COALESCE(l.brand, '') as 출판사,
               COALESCE(CAST(l.coupang_product_id AS TEXT), '-') as "쿠팡ID",
               COALESCE(CAST(l.vendor_item_id AS TEXT), '') as "VID",
               l.synced_at as 동기화일,
               pub.supply_rate as _pub_rate,
               COALESCE(pub2.name, '') as _book_pub
        FROM listings l
        LEFT JOIN publishers pub ON l.brand = pub.name
        LEFT JOIN books b ON l.isbn = b.isbn
        LEFT JOIN publishers pub2 ON b.publisher_id = pub2.id
        WHERE {where_sql}
        ORDER BY l.synced_at DESC NULLS LAST
    """, params)


def render(selected_account, accounts_df, account_names):
    """상품조회 페이지 메인"""
    st.title("상품조회")

    tab1, tab2 = st.tabs(["전체 현황", "불일치/누락 관리"])

    with tab1:
        _render_dashboard(accounts_df, account_names)

    with tab2:
        _render_mismatch(accounts_df, account_names)


# ═══════════════════════════════════════════════════════════════
# Tab 1: 전체 현황 대시보드
# ═══════════════════════════════════════════════════════════════

def _render_dashboard(accounts_df, account_names):
    """Tab 1: 전체 대시보드 + 계정별 테이블"""

    # ── 전체 KPI ──
    _kpi = query_df("""
        SELECT
            COUNT(*) FILTER (WHERE coupang_status = 'active') as active_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'paused') as paused_cnt,
            COUNT(*) FILTER (WHERE coupang_status NOT IN ('active','paused')) as other_cnt,
            COUNT(*) as total_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'active' AND stock_quantity <= 3) as low_stock_cnt,
            COUNT(*) FILTER (WHERE coupang_status = 'active'
                              AND sale_price > 0 AND original_price > 0
                              AND sale_price > original_price) as price_over_cnt,
            COUNT(DISTINCT account_id) as acct_cnt
        FROM listings
    """)
    if not _kpi.empty:
        r = _kpi.iloc[0]
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("전체 판매중", f"{int(r['active_cnt']):,}개")
        c2.metric("판매중지", f"{int(r['paused_cnt']):,}개")
        c3.metric("기타", f"{int(r['other_cnt']):,}개")
        c4.metric("전체", f"{int(r['total_cnt']):,}개")
        c5.metric("재고 부족", f"{int(r['low_stock_cnt'])}건",
                  delta=f"-{int(r['low_stock_cnt'])}" if int(r['low_stock_cnt']) > 0 else None,
                  delta_color="inverse")
        c6.metric("정가초과", f"{int(r['price_over_cnt'])}건",
                  delta=f"⚠ {int(r['price_over_cnt'])}" if int(r['price_over_cnt']) > 0 else "정상",
                  delta_color="inverse" if int(r['price_over_cnt']) > 0 else "normal")

    # ── 계정별 요약 테이블 ──
    acct_sum = query_df("""
        SELECT a.id as account_id, a.account_name as 계정,
               COUNT(l.id) as 전체,
               SUM(CASE WHEN l.coupang_status = 'active' THEN 1 ELSE 0 END) as 판매중,
               SUM(CASE WHEN l.coupang_status = 'paused' THEN 1 ELSE 0 END) as 판매중지,
               SUM(CASE WHEN l.coupang_status NOT IN ('active','paused') THEN 1 ELSE 0 END) as 기타,
               SUM(CASE WHEN l.coupang_status = 'active' AND COALESCE(l.stock_quantity,0) <= 3 THEN 1 ELSE 0 END) as 재고부족,
               COALESCE(SUM(CASE WHEN l.coupang_status = 'active' THEN l.sale_price ELSE 0 END), 0) as 판매중_총액
        FROM accounts a
        LEFT JOIN listings l ON a.id = l.account_id
        WHERE a.is_active = true
        GROUP BY a.id, a.account_name ORDER BY a.account_name
    """)
    if not acct_sum.empty:
        _display_sum = acct_sum.drop(columns=["account_id"]).copy()
        _display_sum["판매중_총액"] = _display_sum["판매중_총액"].apply(lambda x: fmt_krw(x))
        st.dataframe(_display_sum, use_container_width=True, hide_index=True)

    st.divider()

    # ── 계정별 상품 테이블 (확장형) ──
    pub_rates = dict(query_df_cached("SELECT name, supply_rate FROM publishers").values.tolist())

    # 계정 선택 (기본: 전체 계정 순회)
    if accounts_df.empty:
        st.info("활성 계정이 없습니다.")
        return

    for _, acct_row in accounts_df.iterrows():
        acct_id = int(acct_row["id"])
        acct_name = acct_row["account_name"]
        _wing_client = create_wing_client(acct_row)

        with st.expander(f"📦 {acct_name}", expanded=False):
            # 필터
            _fc1, _fc2 = st.columns([1, 3])
            with _fc1:
                _st_filter = st.selectbox(
                    "상태", ["판매중", "전체", "판매중지", "대기", "품절", "반려"],
                    key=f"bw_st_{acct_name}"
                )
            with _fc2:
                _search = st.text_input("검색 (상품명/ISBN/SKU)", key=f"bw_q_{acct_name}")

            listings_df = _load_listings(acct_id, _st_filter, _search)

            if listings_df.empty:
                st.info("조건에 맞는 상품이 없습니다.")
                continue

            listings_df = _calc_margin(listings_df, pub_rates)
            _grid_cols = ["상품명", "정가", "판매가", "순마진", "공급율", "배송", "재고", "상태", "ISBN", "출판사", "쿠팡ID", "VID", "동기화일"]
            _grid_df = listings_df[_grid_cols]

            st.caption(f"총 {len(_grid_df):,}건  |  행 클릭 → 하단 상세보기")

            gb = GridOptionsBuilder.from_dataframe(_grid_df)
            gb.configure_selection(selection_mode="single", use_checkbox=False)
            gb.configure_column("상품명", minWidth=200)
            gb.configure_column("공급율", width=70)
            gb.configure_grid_options(domLayout="normal")
            grid_resp = AgGrid(
                _grid_df,
                gridOptions=gb.build(),
                update_on=["selectionChanged"],
                height=350,
                theme="streamlit",
                key=f"bw_grid_{acct_name}",
            )

            selected = grid_resp["selected_rows"]
            if selected is not None and len(selected) > 0:
                sel = selected.iloc[0] if hasattr(selected, "iloc") else pd.Series(selected[0])
                _render_detail(sel, acct_id, acct_name, _wing_client)


def _render_detail(sel, account_id, account_name, _wing_client):
    """선택 행 상세 카드 + 액션"""
    _sel_vid = sel.get("VID", "") or ""

    st.divider()
    pc1, pc2 = st.columns([1, 3])
    with pc1:
        _img_url = None
        try:
            _img_row = query_df(
                "SELECT images FROM listings WHERE account_id=:aid AND CAST(coupang_product_id AS TEXT)=:cid LIMIT 1",
                {"aid": account_id, "cid": str(sel.get("쿠팡ID", "") or "")}
            )
            if not _img_row.empty:
                _imgs = _img_row.iloc[0]["images"]
                if isinstance(_imgs, str) and _imgs.strip():
                    _imgs_list = _json.loads(_imgs) if _imgs.startswith("[") else []
                    if _imgs_list:
                        _first = _imgs_list[0]
                        _img_url = _first.get("url", _first) if isinstance(_first, dict) else _first
                elif isinstance(_imgs, list) and _imgs:
                    _first = _imgs[0]
                    _img_url = _first.get("url", _first) if isinstance(_first, dict) else _first
        except Exception:
            pass
        if _img_url and isinstance(_img_url, str) and _img_url.startswith("http"):
            st.image(_img_url, width=180)
        elif _img_url and isinstance(_img_url, dict):
            _real_url = _img_url.get("url") or _img_url.get("imageUrl") or _img_url.get("cdnPath") or ""
            if _real_url and isinstance(_real_url, str):
                st.image(_real_url, width=180)
        else:
            st.markdown('<div style="width:180px;height:240px;background:#f0f0f0;display:flex;align-items:center;justify-content:center;border-radius:8px;color:#bbb;font-size:48px;">📖</div>', unsafe_allow_html=True)
    with pc2:
        st.markdown(f"### {sel['상품명']}")
        dc1, dc2, dc3, dc4, dc5 = st.columns(5)
        dc1.metric("정가", f"{int(sel.get('정가', 0) or 0):,}원")
        dc2.metric("판매가", f"{int(sel.get('판매가', 0) or 0):,}원")
        dc3.metric("순마진", f"{int(sel.get('순마진', 0) or 0):,}원")
        dc4.metric("상태", sel.get("상태", "-"))
        dc5.metric("쿠팡ID", sel.get("쿠팡ID", "-") or "-")
        st.caption(f"ISBN: {sel.get('ISBN') or '-'}  |  VID: {_sel_vid or '-'}  |  동기화: {sel.get('동기화일') or '-'}")

    # ── 액션 탭 ──
    _has_api = bool(_sel_vid and _wing_client)
    _action_tabs = ["수정"] + (["실시간 조회", "판매 중지/재개"] if _has_api else [])
    _at = st.tabs(_action_tabs)

    # 수정 탭
    with _at[0]:
        sel_title = sel.get("상품명", "") or ""
        lid_row = query_df("""
            SELECT l.id, l.original_price FROM listings l
            WHERE l.account_id = :acct_id
              AND COALESCE(l.product_name, '') = :title
              AND COALESCE(l.isbn, '') = :isbn
            LIMIT 1
        """, {"acct_id": account_id, "title": sel_title, "isbn": sel.get("ISBN") or ""})
        if not lid_row.empty:
            lid = int(lid_row.iloc[0]["id"])
            _cur_orig_price = int(lid_row.iloc[0]["original_price"] or 0)
            with st.form(f"bw_edit_{account_name}_{lid}"):
                new_name = st.text_input("상품명", value=sel_title)
                le1, le2, le3 = st.columns(3)
                with le1:
                    new_sp = st.number_input("판매가", value=int(sel.get("판매가", 0) or 0), step=100)
                with le2:
                    new_orig = st.number_input("기준가격(정가)", value=_cur_orig_price, step=100)
                with le3:
                    status_opts = ["active", "paused", "pending", "rejected", "sold_out"]
                    _st_map_rev = {"판매중": "active", "판매중지": "paused", "대기": "pending", "품절": "sold_out", "반려": "rejected"}
                    _cur_st = _st_map_rev.get(sel.get("상태", ""), "active")
                    cur_idx = status_opts.index(_cur_st) if _cur_st in status_opts else 0
                    new_status = st.selectbox("상태", status_opts, index=cur_idx, key=f"bw_edit_st_{account_name}_{lid}")
                if st.form_submit_button("저장", type="primary"):
                    try:
                        run_sql("UPDATE listings SET product_name=:name, sale_price=:sp, original_price=:op, coupang_status=:st WHERE id=:id",
                                {"name": new_name, "sp": new_sp, "op": new_orig, "st": new_status, "id": lid})
                        if new_orig != _cur_orig_price and _sel_vid and _wing_client and new_orig > 0:
                            try:
                                _wing_client.update_original_price(int(_sel_vid), new_orig, dashboard_override=True)
                            except CoupangWingError as e:
                                st.warning(f"기준가격 API 반영 실패: {e.message}")
                        st.success("저장 완료")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"저장 실패: {e}")
        else:
            st.info("listings에서 해당 상품을 찾을 수 없습니다.")

    if _has_api:
        # 실시간 조회
        with _at[1]:
            if st.button("실시간 조회", key=f"bw_rt_{account_name}_{_sel_vid}"):
                try:
                    _inv_info = _wing_client.get_item_inventory(int(_sel_vid))
                    _inv_data = _inv_info.get("data", _inv_info)
                    _ri1, _ri2, _ri3, _ri4 = st.columns(4)
                    _ri1.metric("쿠팡 판매가", f"{_inv_data.get('salePrice', '-'):,}원" if isinstance(_inv_data.get('salePrice'), int) else str(_inv_data.get('salePrice', '-')))
                    _ri2.metric("기준가", f"{_inv_data.get('originalPrice', '-'):,}원" if isinstance(_inv_data.get('originalPrice'), int) else str(_inv_data.get('originalPrice', '-')))
                    _ri3.metric("재고", str(_inv_data.get('quantity', _inv_data.get('maximumBuyCount', '-'))))
                    _ri4.metric("판매상태", str(_inv_data.get('salesStatus', _inv_data.get('status', '-'))))
                    st.json(_inv_data)
                except CoupangWingError as e:
                    st.error(f"API 오류: {e.message}")
                except Exception as e:
                    st.error(f"조회 실패: {e}")

        # 판매 중지/재개
        with _at[2]:
            _sale_confirm = st.checkbox("작업을 확인합니다", key=f"bw_sc_{account_name}_{_sel_vid}")
            _sc1, _sc2 = st.columns(2)
            with _sc1:
                if st.button("판매 중지", type="secondary", disabled=not _sale_confirm, key=f"bw_stop_{account_name}_{_sel_vid}", use_container_width=True):
                    try:
                        _wing_client.stop_item_sale(int(_sel_vid), dashboard_override=True)
                        run_sql("UPDATE listings SET coupang_status='sold_out' WHERE account_id=:aid AND vendor_item_id=:vid",
                                {"aid": account_id, "vid": _sel_vid})
                        st.success("판매 중지 완료")
                        st.cache_data.clear()
                        st.rerun()
                    except CoupangWingError as e:
                        st.error(f"API 오류: {e.message}")
            with _sc2:
                if st.button("판매 재개", type="primary", disabled=not _sale_confirm, key=f"bw_resume_{account_name}_{_sel_vid}", use_container_width=True):
                    try:
                        _wing_client.resume_item_sale(int(_sel_vid))
                        run_sql("UPDATE listings SET coupang_status='active' WHERE account_id=:aid AND vendor_item_id=:vid",
                                {"aid": account_id, "vid": _sel_vid})
                        st.success("판매 재개 완료")
                        st.cache_data.clear()
                        st.rerun()
                    except CoupangWingError as e:
                        st.error(f"API 오류: {e.message}")


# ═══════════════════════════════════════════════════════════════
# Tab 2: 불일치/누락 관리
# ═══════════════════════════════════════════════════════════════

def _render_mismatch(accounts_df, account_names):
    """Tab 2: 노출-등록 불일치 + 옵션 누락"""

    st.subheader("불일치/누락 점검")

    # ── 섹션 1: 가격 불일치 (DB products 기준가 vs listings 판매가) ──
    st.markdown("#### 가격 불일치")
    st.caption("DB 기준가(products.sale_price)와 쿠팡 등록가(listings.sale_price)가 다른 상품")

    price_diff = query_df("""
        SELECT a.account_name as 계정,
               COALESCE(l.product_name, '(미등록)') as 상품명,
               p.sale_price as 기준가,
               l.sale_price as 등록가,
               (p.sale_price - l.sale_price) as 차이,
               COALESCE(CAST(l.vendor_item_id AS TEXT), '') as "VID",
               l.isbn as "ISBN",
               l.id as _lid, a.id as _aid
        FROM listings l
        JOIN products p ON l.product_id = p.id
        JOIN accounts a ON l.account_id = a.id
        WHERE a.is_active = true
          AND l.coupang_status = 'active'
          AND l.sale_price > 0 AND p.sale_price > 0
          AND l.sale_price != p.sale_price
        ORDER BY a.account_name, ABS(p.sale_price - l.sale_price) DESC
    """)

    if not price_diff.empty:
        st.warning(f"{len(price_diff)}건의 가격 불일치")
        _pd_display = price_diff[["계정", "상품명", "기준가", "등록가", "차이", "VID", "ISBN"]].copy()
        gb_pd = GridOptionsBuilder.from_dataframe(_pd_display)
        gb_pd.configure_selection(selection_mode="multiple", use_checkbox=True)
        gb_pd.configure_column("상품명", headerCheckboxSelection=True, minWidth=200)
        gb_pd.configure_grid_options(domLayout="normal")
        pd_grid = AgGrid(
            _pd_display,
            gridOptions=gb_pd.build(),
            update_on=["selectionChanged"],
            height=300,
            theme="streamlit",
            key="mm_price_grid",
        )
        pd_selected = pd_grid["selected_rows"]
        pd_sel_list = []
        if pd_selected is not None and len(pd_selected) > 0:
            _pdf = pd_selected if isinstance(pd_selected, pd.DataFrame) else pd.DataFrame(pd_selected)
            pd_sel_list = _pdf.to_dict("records")

        if pd_sel_list:
            _confirm = st.checkbox("선택 항목 가격을 기준가로 일괄 수정합니다", key="mm_pd_confirm")
            if st.button(f"선택 {len(pd_sel_list)}건 가격 수정", type="primary", disabled=not _confirm, key="mm_pd_fix"):
                _ok, _fail = 0, 0
                _prog = st.progress(0, text="가격 수정 중...")
                for _i, _r in enumerate(pd_sel_list):
                    _prog.progress((_i + 1) / len(pd_sel_list))
                    vid = str(_r.get("VID", ""))
                    if not vid:
                        _fail += 1
                        continue
                    # 원본 데이터에서 기준가 가져오기
                    _match = price_diff[price_diff["VID"] == vid]
                    _target = int(_match.iloc[0]["기준가"]) if not _match.empty else int(_r.get("기준가", 0))
                    _acct_name = _r.get("계정", "")
                    # 계정별 WING 클라이언트
                    _acct_mask = accounts_df["account_name"] == _acct_name
                    if not _acct_mask.any():
                        _fail += 1
                        continue
                    _acct_row = accounts_df[_acct_mask].iloc[0]
                    _client = create_wing_client(_acct_row)
                    if _client is None:
                        _fail += 1
                        continue
                    try:
                        _client.update_price(int(vid), _target, dashboard_override=True)
                        run_sql("UPDATE listings SET sale_price=:sp WHERE vendor_item_id=:vid AND account_id=:aid",
                                {"sp": _target, "vid": vid, "aid": int(_acct_row["id"])})
                        _ok += 1
                    except Exception as e:
                        _fail += 1
                        logger.warning(f"가격수정 실패 VID={vid}: {e}")
                _prog.progress(1.0, text="완료!")
                st.success(f"완료: 성공 {_ok}건, 실패 {_fail}건")
                st.cache_data.clear()
                st.rerun()
    else:
        st.success("가격 불일치 없음")

    st.divider()

    # ── 섹션 2: 필수 정보 누락 (VID, 판매가, 정가 등) ──
    st.markdown("#### 필수 정보 누락")
    st.caption("VID, 판매가, 정가, ISBN 등 필수 필드가 비어있는 상품")

    missing = query_df("""
        SELECT a.account_name as 계정,
               COALESCE(l.product_name, '(미등록)') as 상품명,
               l.isbn as "ISBN",
               COALESCE(CAST(l.coupang_product_id AS TEXT), '-') as "쿠팡ID",
               COALESCE(CAST(l.vendor_item_id AS TEXT), '') as "VID",
               l.sale_price as 판매가,
               COALESCE(l.original_price, 0) as 정가,
               l.coupang_status as 상태,
               CASE
                   WHEN l.vendor_item_id IS NULL OR l.vendor_item_id = 0 THEN 'VID 없음'
                   WHEN l.sale_price IS NULL OR l.sale_price = 0 THEN '판매가 없음'
                   WHEN l.original_price IS NULL OR l.original_price = 0 THEN '정가 없음'
                   WHEN l.isbn IS NULL OR l.isbn = '' THEN 'ISBN 없음'
                   WHEN l.product_name IS NULL OR l.product_name = '' THEN '상품명 없음'
               END as 누락항목,
               l.id as _lid, a.id as _aid
        FROM listings l
        JOIN accounts a ON l.account_id = a.id
        WHERE a.is_active = true
          AND l.coupang_status = 'active'
          AND (
              l.vendor_item_id IS NULL OR l.vendor_item_id = 0
              OR l.sale_price IS NULL OR l.sale_price = 0
              OR l.original_price IS NULL OR l.original_price = 0
              OR l.isbn IS NULL OR l.isbn = ''
              OR l.product_name IS NULL OR l.product_name = ''
          )
        ORDER BY a.account_name, l.product_name
    """)

    if not missing.empty:
        st.warning(f"{len(missing)}건의 필수 정보 누락")
        _ms_display = missing[["계정", "상품명", "누락항목", "판매가", "정가", "ISBN", "쿠팡ID", "VID", "상태"]].copy()
        _sl = {"active": "판매중", "paused": "판매중지", "pending": "대기", "sold_out": "품절", "rejected": "반려"}
        _ms_display["상태"] = _ms_display["상태"].map(_sl).fillna(_ms_display["상태"])

        gb_ms = GridOptionsBuilder.from_dataframe(_ms_display)
        gb_ms.configure_selection(selection_mode="single", use_checkbox=False)
        gb_ms.configure_column("상품명", minWidth=200)
        gb_ms.configure_grid_options(domLayout="normal")
        ms_grid = AgGrid(
            _ms_display,
            gridOptions=gb_ms.build(),
            update_on=["selectionChanged"],
            height=300,
            theme="streamlit",
            key="mm_missing_grid",
        )

        ms_selected = ms_grid["selected_rows"]
        if ms_selected is not None and len(ms_selected) > 0:
            ms_sel = ms_selected.iloc[0] if hasattr(ms_selected, "iloc") else pd.Series(ms_selected[0])
            _ms_match = missing[
                (missing["상품명"] == ms_sel.get("상품명", "")) &
                (missing["계정"] == ms_sel.get("계정", ""))
            ]
            if not _ms_match.empty:
                _lid = int(_ms_match.iloc[0]["_lid"])
                st.divider()
                st.markdown(f"**{ms_sel.get('계정', '')} — {ms_sel.get('상품명', '')}** (누락: {ms_sel.get('누락항목', '')})")
                with st.form(f"mm_fix_{_lid}"):
                    mf1, mf2, mf3 = st.columns(3)
                    with mf1:
                        fix_name = st.text_input("상품명", value=ms_sel.get("상품명", "") or "")
                    with mf2:
                        fix_sp = st.number_input("판매가", value=int(ms_sel.get("판매가", 0) or 0), step=100)
                    with mf3:
                        fix_op = st.number_input("정가", value=int(ms_sel.get("정가", 0) or 0), step=100)
                    mf4, mf5 = st.columns(2)
                    with mf4:
                        fix_isbn = st.text_input("ISBN", value=ms_sel.get("ISBN", "") or "")
                    with mf5:
                        fix_vid = st.text_input("VID", value=ms_sel.get("VID", "") or "")

                    if st.form_submit_button("저장", type="primary"):
                        try:
                            _update_parts = []
                            _params = {"id": _lid}
                            if fix_name:
                                _update_parts.append("product_name=:name")
                                _params["name"] = fix_name
                            if fix_sp > 0:
                                _update_parts.append("sale_price=:sp")
                                _params["sp"] = fix_sp
                            if fix_op > 0:
                                _update_parts.append("original_price=:op")
                                _params["op"] = fix_op
                            if fix_isbn:
                                _update_parts.append("isbn=:isbn")
                                _params["isbn"] = fix_isbn
                            if fix_vid:
                                _update_parts.append("vendor_item_id=:vid")
                                _params["vid"] = int(fix_vid) if fix_vid.isdigit() else fix_vid
                            if _update_parts:
                                run_sql(f"UPDATE listings SET {', '.join(_update_parts)} WHERE id=:id", _params)
                                st.success("저장 완료")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.warning("변경사항이 없습니다.")
                        except Exception as e:
                            st.error(f"저장 실패: {e}")
    else:
        st.success("필수 정보 누락 없음")

    st.divider()

    # ── 섹션 3: 정가초과 (판매가 > 정가) ──
    st.markdown("#### 정가 초과")
    st.caption("판매가가 정가(original_price)보다 높은 상품 — 도서정가제 위반 우려")

    over_price = query_df("""
        SELECT a.account_name as 계정,
               COALESCE(l.product_name, '(미등록)') as 상품명,
               l.original_price as 정가,
               l.sale_price as 판매가,
               (l.sale_price - l.original_price) as 초과액,
               COALESCE(CAST(l.vendor_item_id AS TEXT), '') as "VID",
               l.isbn as "ISBN",
               l.id as _lid, a.id as _aid
        FROM listings l
        JOIN accounts a ON l.account_id = a.id
        WHERE a.is_active = true
          AND l.coupang_status = 'active'
          AND l.sale_price > 0 AND l.original_price > 0
          AND l.sale_price > l.original_price
        ORDER BY (l.sale_price - l.original_price) DESC
    """)

    if not over_price.empty:
        st.warning(f"{len(over_price)}건의 정가 초과")
        _op_display = over_price[["계정", "상품명", "정가", "판매가", "초과액", "VID", "ISBN"]].copy()
        gb_op = GridOptionsBuilder.from_dataframe(_op_display)
        gb_op.configure_selection(selection_mode="multiple", use_checkbox=True)
        gb_op.configure_column("상품명", headerCheckboxSelection=True, minWidth=200)
        gb_op.configure_grid_options(domLayout="normal")
        op_grid = AgGrid(
            _op_display,
            gridOptions=gb_op.build(),
            update_on=["selectionChanged"],
            height=300,
            theme="streamlit",
            key="mm_over_grid",
        )
        op_selected = op_grid["selected_rows"]
        op_sel_list = []
        if op_selected is not None and len(op_selected) > 0:
            _opf = op_selected if isinstance(op_selected, pd.DataFrame) else pd.DataFrame(op_selected)
            op_sel_list = _opf.to_dict("records")

        if op_sel_list:
            _op_confirm = st.checkbox("선택 항목 판매가를 정가의 90%로 수정합니다", key="mm_op_confirm")
            if st.button(f"선택 {len(op_sel_list)}건 판매가 수정 (정가×0.9)", type="primary", disabled=not _op_confirm, key="mm_op_fix"):
                _ok, _fail = 0, 0
                _prog = st.progress(0, text="판매가 수정 중...")
                for _i, _r in enumerate(op_sel_list):
                    _prog.progress((_i + 1) / len(op_sel_list))
                    vid = str(_r.get("VID", ""))
                    if not vid:
                        _fail += 1
                        continue
                    _match = over_price[over_price["VID"] == vid]
                    _orig = int(_match.iloc[0]["정가"]) if not _match.empty else 0
                    if _orig <= 0:
                        _fail += 1
                        continue
                    _target = int(_orig * 0.9)
                    _acct_name = _r.get("계정", "")
                    _acct_mask = accounts_df["account_name"] == _acct_name
                    if not _acct_mask.any():
                        _fail += 1
                        continue
                    _acct_row = accounts_df[_acct_mask].iloc[0]
                    _client = create_wing_client(_acct_row)
                    if _client is None:
                        _fail += 1
                        continue
                    try:
                        _client.update_price(int(vid), _target, dashboard_override=True)
                        run_sql("UPDATE listings SET sale_price=:sp WHERE vendor_item_id=:vid AND account_id=:aid",
                                {"sp": _target, "vid": vid, "aid": int(_acct_row["id"])})
                        _ok += 1
                    except Exception as e:
                        _fail += 1
                        logger.warning(f"판매가수정 실패 VID={vid}: {e}")
                _prog.progress(1.0, text="완료!")
                st.success(f"완료: 성공 {_ok}건, 실패 {_fail}건")
                st.cache_data.clear()
                st.rerun()
    else:
        st.success("정가 초과 없음")
