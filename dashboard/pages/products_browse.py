"""상품조회 페이지 — 전체 대시보드 + 계정별 테이블 + 불일치/누락 관리"""
import io
import logging
import re
import time
import json as _json
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

from dashboard.utils import (
    query_df, query_df_cached, run_sql, create_wing_client,
    fmt_money_df, fmt_krw, CoupangWingError, engine,
)
from core.constants import COUPANG_FEE_RATE, DEFAULT_SHIPPING_COST
from sqlalchemy import text

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


def _run_wing_sync(accounts_df, quick=False, force=False, stale_hours=24):
    """WING API 동기화 실행 (Streamlit 진행 표시)"""
    from psycopg2.extras import execute_values
    from core.api.wing_client import CoupangWingClient

    isbn_pattern = re.compile(r'97[89]\d{10}')
    now = datetime.utcnow()

    # 활성 WING 계정만
    wing_accts = accounts_df[accounts_df["wing_api_enabled"] == True]
    if wing_accts.empty:
        st.warning("WING API가 활성화된 계정이 없습니다.")
        return

    total_result = {"total": 0, "new": 0, "updated": 0, "detail_synced": 0, "detail_error": 0}
    _log = st.container()
    _progress = st.progress(0, text="동기화 준비 중...")

    for acct_idx, (_, acct_row) in enumerate(wing_accts.iterrows()):
        acct_name = acct_row["account_name"]
        acct_id = int(acct_row["id"])
        _progress.progress(
            acct_idx / len(wing_accts),
            text=f"[{acct_idx+1}/{len(wing_accts)}] {acct_name} — Stage 1 목록 조회 중...",
        )

        client = create_wing_client(acct_row)
        if client is None:
            _log.caption(f"{acct_name}: WING 클라이언트 생성 실패, 건너뜀")
            continue

        # ── Stage 1: list_products ──
        try:
            products = client.list_products(max_per_page=100, max_pages=0)
        except CoupangWingError as e:
            _log.warning(f"{acct_name}: 목록 조회 실패 — {e.message}")
            continue

        total_result["total"] += len(products)

        # 기존 listings 로드
        existing_rows = query_df(
            "SELECT id, coupang_product_id, isbn, vendor_item_id FROM listings WHERE account_id = :aid",
            {"aid": acct_id},
        )
        by_pid = {}
        by_isbn = {}
        for _, erow in existing_rows.iterrows():
            d = {"id": erow["id"], "coupang_product_id": erow["coupang_product_id"],
                 "isbn": erow.get("isbn"), "vendor_item_id": erow.get("vendor_item_id")}
            if d["coupang_product_id"]:
                by_pid[int(d["coupang_product_id"])] = d
            if d["isbn"] and pd.notna(d["isbn"]):
                by_isbn[str(d["isbn"])] = d

        upsert_rows = []
        new_cnt, upd_cnt = 0, 0

        for pdata in products:
            spid = int(pdata.get("sellerProductId", 0))
            # ISBN 추출
            isbns = set()
            for item in pdata.get("items", []):
                for field in ["barcode", "externalVendorSku"]:
                    for m in isbn_pattern.finditer(str(item.get(field, ""))):
                        isbns.add(m.group())
                for tag in (item.get("searchTags") or []):
                    for m in isbn_pattern.finditer(str(tag)):
                        isbns.add(m.group())
                attrs = item.get("attributes", [])
                if isinstance(attrs, list):
                    for attr in attrs:
                        if attr.get("attributeTypeName") == "ISBN":
                            cleaned = re.sub(r'[^0-9]', '', str(attr.get("attributeValueName", "")))
                            if len(cleaned) == 13 and cleaned.startswith(("978", "979")):
                                isbns.add(cleaned)
            isbn_str = ",".join(sorted(isbns)) if isbns else None

            vid = None
            items = pdata.get("items", [])
            if items:
                vid = items[0].get("vendorItemId")
                if vid:
                    vid = int(vid)

            # 상태
            status_raw = pdata.get("statusName", pdata.get("status", ""))
            _smap = {"판매중": "active", "승인완료": "active", "APPROVE": "active",
                     "판매중지": "paused", "SUSPEND": "paused", "품절": "sold_out",
                     "SOLDOUT": "sold_out", "승인반려": "rejected", "삭제": "deleted", "승인대기": "pending"}
            coupang_status = _smap.get(status_raw, "pending")
            product_name = pdata.get("sellerProductName", "")

            sale_price = items[0].get("salePrice", 0) or 0 if items else 0
            original_price = items[0].get("originalPrice", 0) or 0 if items else 0

            existing = by_pid.get(spid)
            if not existing and isbn_str:
                existing = by_isbn.get(isbn_str)

            if existing:
                upd_cnt += 1
            else:
                new_cnt += 1

            upsert_rows.append((
                acct_id, spid, vid, isbn_str,
                coupang_status, sale_price, original_price, product_name,
                now, now, now,
            ))

        # 벌크 UPSERT
        if upsert_rows:
            raw_conn = engine.raw_connection()
            try:
                cur = raw_conn.cursor()
                sql = """
                    INSERT INTO listings (account_id, coupang_product_id, vendor_item_id, isbn,
                        coupang_status, sale_price, original_price, product_name,
                        synced_at, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT (account_id, coupang_product_id) DO UPDATE SET
                        coupang_status = EXCLUDED.coupang_status,
                        product_name = EXCLUDED.product_name,
                        vendor_item_id = COALESCE(EXCLUDED.vendor_item_id, listings.vendor_item_id),
                        sale_price = CASE WHEN EXCLUDED.sale_price > 0 THEN EXCLUDED.sale_price ELSE listings.sale_price END,
                        original_price = CASE WHEN EXCLUDED.original_price > 0 THEN EXCLUDED.original_price ELSE listings.original_price END,
                        isbn = COALESCE(listings.isbn, EXCLUDED.isbn),
                        synced_at = EXCLUDED.synced_at,
                        updated_at = NOW()
                """
                BATCH = 500
                for i in range(0, len(upsert_rows), BATCH):
                    execute_values(cur, sql, upsert_rows[i:i+BATCH], page_size=BATCH)
                raw_conn.commit()
                cur.close()
            except Exception as e:
                raw_conn.rollback()
                _log.error(f"{acct_name}: 벌크 UPSERT 실패 — {e}")
            finally:
                raw_conn.close()

        total_result["new"] += new_cnt
        total_result["updated"] += upd_cnt
        _log.caption(f"{acct_name}: {len(products)}개 조회, 신규 {new_cnt}, 업데이트 {upd_cnt}")

        # ── Stage 2: 상세 조회 (quick이 아닐 때) ──
        if not quick and upsert_rows:
            # 상세 조회 대상 선별 (detail_synced_at이 NULL이거나 stale)
            stale_cutoff = now - timedelta(hours=stale_hours)
            if force:
                detail_targets = query_df(
                    "SELECT id, coupang_product_id, vendor_item_id, detail_synced_at FROM listings WHERE account_id = :aid AND coupang_product_id IS NOT NULL",
                    {"aid": acct_id},
                )
            else:
                detail_targets = query_df(
                    "SELECT id, coupang_product_id, vendor_item_id, detail_synced_at FROM listings WHERE account_id = :aid AND coupang_product_id IS NOT NULL AND (detail_synced_at IS NULL OR detail_synced_at < :cutoff)",
                    {"aid": acct_id, "cutoff": stale_cutoff},
                )

            if not detail_targets.empty:
                _log.caption(f"{acct_name}: Stage 2 상세 조회 {len(detail_targets)}건...")
                _detail_ok, _detail_err = 0, 0

                for di, (_, drow) in enumerate(detail_targets.iterrows()):
                    if di % 10 == 0:
                        _progress.progress(
                            (acct_idx + (di / len(detail_targets))) / len(wing_accts),
                            text=f"[{acct_idx+1}/{len(wing_accts)}] {acct_name} — Stage 2 상세 {di+1}/{len(detail_targets)}",
                        )
                    pid = int(drow["coupang_product_id"])
                    try:
                        result = client.get_product(pid)
                        data = result.get("data", result) if isinstance(result, dict) else result
                        if not isinstance(data, dict):
                            continue

                        # 파싱
                        brand = data.get("brand", "") or ""
                        disp_cat = str(data.get("displayCategoryCode", "")) or ""
                        del_type = data.get("deliveryChargeType", "") or ""
                        del_charge = data.get("deliveryCharge")
                        free_ship = data.get("freeShipOverAmount")
                        ret_charge = data.get("returnCharge")
                        supply_price = None
                        orig_price = None
                        sale_price_d = None
                        isbn_d = None
                        publisher_d = None
                        d_items = data.get("items", [])
                        if d_items:
                            d_item = d_items[0]
                            supply_price = d_item.get("supplyPrice")
                            orig_price = d_item.get("originalPrice")
                            sale_price_d = d_item.get("salePrice")
                            for attr in (d_item.get("attributes") or []):
                                aname = attr.get("attributeTypeName", "")
                                aval = attr.get("attributeValueName", "")
                                if aname == "ISBN" and aval and "상세" not in aval:
                                    cleaned = re.sub(r'[^0-9]', '', aval)
                                    if len(cleaned) == 13:
                                        isbn_d = cleaned
                                elif aname == "출판사" and aval and "상세" not in aval:
                                    publisher_d = aval

                        # 재고/판매상태
                        vid_val = drow.get("vendor_item_id")
                        on_sale_status = None
                        stock_qty = None
                        if vid_val and pd.notna(vid_val) and int(vid_val) > 0:
                            try:
                                inv = client.get_item_inventory(int(vid_val))
                                inv_data = inv.get("data", inv) if isinstance(inv, dict) else {}
                                on_sale_status = "active" if inv_data.get("onSale", True) else "paused"
                                stock_qty = inv_data.get("amountInStock")
                            except Exception:
                                pass

                        # DB 업데이트
                        _parts = ["brand=:brand", "display_category_code=:dcc",
                                  "delivery_charge_type=:dct", "delivery_charge=:dc",
                                  "free_ship_over_amount=:fso", "return_charge=:rc",
                                  "raw_json=:rj", "detail_synced_at=:dsa"]
                        _p = {"id": int(drow["id"]), "brand": brand, "dcc": disp_cat,
                              "dct": del_type, "dc": del_charge, "fso": free_ship, "rc": ret_charge,
                              "rj": _json.dumps(data, ensure_ascii=False), "dsa": now}
                        if supply_price:
                            _parts.append("supply_price=:sup")
                            _p["sup"] = supply_price
                        if orig_price and orig_price > 0:
                            _parts.append("original_price=:op")
                            _p["op"] = orig_price
                        if sale_price_d and sale_price_d > 0:
                            _parts.append("sale_price=:sp")
                            _p["sp"] = sale_price_d
                        if isbn_d:
                            _parts.append("isbn=COALESCE(listings.isbn, :isbn)")
                            _p["isbn"] = isbn_d
                        if on_sale_status:
                            _parts.append("coupang_status=:cs")
                            _p["cs"] = on_sale_status
                        if stock_qty is not None:
                            _parts.append("stock_quantity=:sq")
                            _p["sq"] = stock_qty

                        run_sql(f"UPDATE listings SET {', '.join(_parts)} WHERE id=:id", _p)
                        _detail_ok += 1
                        time.sleep(0.08)

                    except CoupangWingError as e:
                        _detail_err += 1
                        if e.status_code == 429 or "RATE" in str(getattr(e, 'code', '')).upper():
                            time.sleep(1)
                    except Exception:
                        _detail_err += 1

                total_result["detail_synced"] += _detail_ok
                total_result["detail_error"] += _detail_err
                _log.caption(f"{acct_name}: Stage 2 완료 — 성공 {_detail_ok}, 실패 {_detail_err}")

    _progress.progress(1.0, text="동기화 완료!")

    # 결과 요약
    _log.success(
        f"동기화 완료: 총 {total_result['total']}개 조회, "
        f"신규 {total_result['new']}, 업데이트 {total_result['updated']}"
        + (f", 상세 {total_result['detail_synced']}건" if total_result['detail_synced'] else "")
        + (f", 상세실패 {total_result['detail_error']}건" if total_result['detail_error'] else "")
    )
    st.cache_data.clear()


def _render_all_products(pub_rates):
    """전체 상품 목록 (모든 계정, 검색+수정 가능)"""
    st.subheader("전체 상품")

    # 필터
    _fc1, _fc2, _fc3 = st.columns([1, 1, 3])
    with _fc1:
        _all_st = st.selectbox("상태", ["판매중", "전체", "판매중지", "대기", "품절", "반려"], key="bw_all_st")
    with _fc2:
        _pub_list = ["전체"] + sorted(
            query_df_cached("SELECT DISTINCT COALESCE(l.brand, '') as b FROM listings l WHERE l.brand IS NOT NULL AND l.brand != '' ORDER BY b")["b"].tolist()
        )
        _pub_filter = st.selectbox("출판사/브랜드", _pub_list, key="bw_all_pub")
    with _fc3:
        _all_q = st.text_input("검색 (상품명 / ISBN / 쿠팡ID)", key="bw_all_q")

    # 쿼리 조건
    where_parts = ["1=1"]
    params = {}
    _filter_map = {"판매중": "active", "판매중지": "paused", "대기": "pending", "품절": "sold_out", "반려": "rejected"}
    if _all_st != "전체":
        where_parts.append("l.coupang_status = :status")
        params["status"] = _filter_map.get(_all_st, _all_st)
    if _pub_filter != "전체":
        where_parts.append("l.brand = :brand")
        params["brand"] = _pub_filter
    if _all_q:
        where_parts.append("(l.product_name LIKE :sq OR l.isbn LIKE :sq OR CAST(l.coupang_product_id AS TEXT) LIKE :sq)")
        params["sq"] = f"%{_all_q}%"
    where_sql = " AND ".join(where_parts)

    all_df = query_df(f"""
        SELECT a.account_name as 계정,
               COALESCE(l.product_name, '(미등록)') as 상품명,
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
               l.account_id as _account_id,
               l.id as _lid,
               pub.supply_rate as _pub_rate,
               COALESCE(pub2.name, '') as _book_pub
        FROM listings l
        JOIN accounts a ON a.id = l.account_id
        LEFT JOIN publishers pub ON l.brand = pub.name
        LEFT JOIN books b ON l.isbn = b.isbn
        LEFT JOIN publishers pub2 ON b.publisher_id = pub2.id
        WHERE {where_sql}
        ORDER BY COALESCE(l.brand, 'zzz'), l.product_name, a.account_name
    """, params)

    if all_df.empty:
        st.info("조건에 맞는 상품이 없습니다.")
        return

    all_df = _calc_margin(all_df, pub_rates)

    # KPI
    _k1, _k2, _k3 = st.columns(3)
    _k1.metric("상품 수", f"{len(all_df):,}건")
    _k2.metric("출판사/브랜드", f"{all_df['출판사'].nunique()}개")
    _k3.metric("계정", f"{all_df['계정'].nunique()}개")

    # 그리드
    _grid_cols = ["계정", "상품명", "정가", "판매가", "순마진", "공급율", "배송", "재고", "상태", "ISBN", "출판사", "쿠팡ID", "VID"]
    _grid_df = all_df[_grid_cols]

    gb = GridOptionsBuilder.from_dataframe(_grid_df)
    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_column("상품명", minWidth=200)
    gb.configure_column("공급율", width=70)
    gb.configure_column("계정", width=100)
    gb.configure_grid_options(domLayout="normal")
    grid_resp = AgGrid(
        _grid_df,
        gridOptions=gb.build(),
        update_on=["selectionChanged"],
        height=500,
        theme="streamlit",
        key="bw_all_grid",
    )

    # 엑셀 다운로드
    _xl_buf = io.BytesIO()
    with pd.ExcelWriter(_xl_buf, engine="openpyxl") as writer:
        _grid_df.to_excel(writer, sheet_name="전체상품", index=False)
    _xl_buf.seek(0)
    st.download_button(
        f"엑셀 다운로드 ({len(_grid_df):,}건)",
        _xl_buf.getvalue(),
        file_name="쿠팡_전체상품.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="bw_all_xlsx_dl",
        type="primary",
    )

    # 행 선택 → WING 상세 조회
    selected = grid_resp["selected_rows"]
    if selected is not None and len(selected) > 0:
        sel = selected.iloc[0] if hasattr(selected, "iloc") else pd.Series(selected[0])
        _sel_cpid = str(sel.get("쿠팡ID", "") or "")
        _sel_vid = str(sel.get("VID", "") or "")
        _sel_acct = sel.get("계정", "")

        # 계정 정보 조회 → WING 클라이언트 생성
        _acct_row = query_df("SELECT * FROM accounts WHERE account_name = :name LIMIT 1", {"name": _sel_acct})
        if not _acct_row.empty:
            _acct = _acct_row.iloc[0]
            _acct_id = int(_acct["id"])
            _wc = create_wing_client(_acct)

            st.divider()
            st.markdown(f"#### {sel['상품명']}  (`{_sel_acct}`)")

            if _wc and _sel_cpid and _sel_cpid != "-":
                # 실시간 조회 버튼
                if st.button("WING 실시간 조회", key="bw_all_rt_btn", type="primary"):
                    try:
                        _prod = _wc.get_product(int(_sel_cpid))
                        _pd = _prod.get("data", _prod)
                        st.session_state["_bw_all_product_data"] = _pd
                        st.session_state["_bw_all_cpid"] = _sel_cpid
                        st.session_state["_bw_all_acct_id"] = _acct_id
                        st.session_state["_bw_all_vid"] = _sel_vid
                        # DB에 상세 저장
                        from dashboard.pages.products_list import _save_product_detail_to_db
                        _save_product_detail_to_db(_acct_id, int(_sel_cpid), _pd)
                    except CoupangWingError as e:
                        st.error(f"조회 실패: {e.message}")
                    except Exception as e:
                        st.error(f"조회 실패: {e}")

                # 세션에 저장된 데이터 표시
                _cached_pd = st.session_state.get("_bw_all_product_data")
                if _cached_pd and st.session_state.get("_bw_all_cpid") == _sel_cpid:
                    from dashboard.pages.products_list import _render_product_detail
                    _render_product_detail(
                        _cached_pd, _wc,
                        st.session_state.get("_bw_all_acct_id", _acct_id),
                        st.session_state.get("_bw_all_vid", _sel_vid),
                        key_prefix="bw_all_rt",
                    )
            else:
                st.info("WING API 연결 불가 (API 키 미설정 또는 쿠팡ID 없음)")


def render(selected_account, accounts_df, account_names):
    """상품조회 페이지 메인"""
    st.title("상품조회")

    tab1, tab_winner, tab2 = st.tabs(["전체 현황", "아이템위너", "불일치/누락 관리"])

    with tab1:
        _render_dashboard(accounts_df, account_names)

    with tab_winner:
        _render_item_winner(accounts_df, account_names)

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

    # ── WING 동기화 ──
    _sync_col1, _sync_col2, _sync_col3, _sync_col4 = st.columns([1.5, 1.5, 1, 6])
    with _sync_col1:
        _sync_quick = st.button("빠른 동기화 (목록만)", key="bw_sync_quick", type="primary", use_container_width=True)
    with _sync_col2:
        _sync_full = st.button("전체 동기화 (상세포함)", key="bw_sync_full", use_container_width=True)
    with _sync_col3:
        _sync_force = st.checkbox("강제", key="bw_sync_force", help="이미 동기화된 상품도 다시 조회")

    # 마지막 동기화 시간
    _last_sync = query_df("SELECT MAX(synced_at) as last FROM listings")
    if not _last_sync.empty and _last_sync.iloc[0]["last"]:
        with _sync_col4:
            st.caption(f"마지막 동기화: {_last_sync.iloc[0]['last']}")

    if _sync_quick:
        _run_wing_sync(accounts_df, quick=True)
    elif _sync_full:
        _run_wing_sync(accounts_df, quick=False, force=_sync_force, stale_hours=24)

    st.divider()

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

    # ── 전체 상품 목록 ──
    pub_rates = dict(query_df_cached("SELECT name, supply_rate FROM publishers").values.tolist())
    _render_all_products(pub_rates)

    st.divider()

    # ── 계정별 상품 테이블 (확장형) ──
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
        # dict이면 cdnPath에서 URL 추출
        if _img_url and isinstance(_img_url, dict):
            _img_url = _img_url.get("cdnPath") or _img_url.get("vendorPath") or _img_url.get("url") or _img_url.get("imageUrl") or ""
        # 상대경로면 쿠팡 CDN 프리픽스 추가
        if _img_url and isinstance(_img_url, str) and not _img_url.startswith("http"):
            _img_url = f"https://image6.coupangcdn.com/image/{_img_url}"
        if _img_url and isinstance(_img_url, str) and _img_url.startswith("http"):
            st.image(_img_url, width=180)
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
                    _inv_data = _inv_info.get("data") if isinstance(_inv_info.get("data"), dict) else _inv_info
                    _ri1, _ri2, _ri3 = st.columns(3)
                    _sale_p = _inv_data.get('salePrice')
                    _ri1.metric("쿠팡 판매가", f"{_sale_p:,}원" if isinstance(_sale_p, (int, float)) else str(_sale_p or '-'))
                    _stock = _inv_data.get('amountInStock', _inv_data.get('quantity', _inv_data.get('maximumBuyCount')))
                    _ri2.metric("재고", f"{_stock:,}" if isinstance(_stock, (int, float)) else str(_stock or '-'))
                    _on_sale = _inv_data.get('onSale', _inv_data.get('salesStatus', _inv_data.get('status')))
                    _ri3.metric("판매상태", "판매중" if _on_sale is True else "중지" if _on_sale is False else str(_on_sale or '-'))
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
# Tab: 아이템위너
# ═══════════════════════════════════════════════════════════════

def _parse_winner_excel(uploaded_file):
    """쿠팡 가격관리 엑셀 파싱 (header=1, 안내 행 스킵)"""
    df = pd.read_excel(uploaded_file, header=1, dtype=str)
    df.columns = df.columns.str.strip()
    # 빈 행 제거
    id_col = next((c for c in df.columns if "노출상품" in c or "상품ID" in c), df.columns[0])
    df = df[df[id_col].notna() & (df[id_col] != "")]
    return df


def _classify_winner(df):
    """위너 여부 판정 컬럼 추가"""
    # '아이템위너가 대비 내 판매가' = 0 이면 위너
    _diff_col = next((c for c in df.columns if "아이템위너가 대비" in c or "위너가 대비" in c), None)
    # '내상품 판매 점유율' = 100% 이면 위너
    _share_col = next((c for c in df.columns if "판매 점유율" in c or "점유율" in c), None)

    def _is_winner(row):
        # 방법 1: 가격 차이가 0
        if _diff_col:
            try:
                diff = int(str(row.get(_diff_col, "")).replace(",", "").strip() or "999")
                if diff == 0:
                    return "위너"
            except (ValueError, TypeError):
                pass
        # 방법 2: 점유율 100%
        if _share_col:
            share = str(row.get(_share_col, "")).strip()
            if share == "100%":
                return "위너"
        return "비위너"

    df = df.copy()
    df["위너여부"] = df.apply(_is_winner, axis=1)
    return df


def _render_item_winner(accounts_df, account_names):
    """아이템위너 탭: 계정별 엑셀 업로드 + 위너 분석"""

    st.subheader("아이템위너 현황")
    st.caption("WING → 가격관리 → 엑셀 다운로드 파일을 업로드하세요.")

    if "winner_data" not in st.session_state:
        st.session_state["winner_data"] = {}

    # ── 업로드 영역 ──
    _uc1, _uc2 = st.columns([2, 5])
    with _uc1:
        _sel_acct = st.selectbox("계정 선택", account_names, key="winner_acct")
    with _uc2:
        _uploaded = st.file_uploader(
            "엑셀 파일 (.xlsx)",
            type=["xlsx", "xls"],
            key="winner_upload",
        )

    if _uploaded and _sel_acct:
        try:
            _raw_df = _parse_winner_excel(_uploaded)
            _raw_df = _classify_winner(_raw_df)
            st.session_state["winner_data"][_sel_acct] = _raw_df
            st.success(f"{_sel_acct}: {len(_raw_df)}건 로드 완료")
        except Exception as e:
            st.error(f"엑셀 읽기 실패: {e}")

    _loaded = {k: v for k, v in st.session_state["winner_data"].items() if not v.empty}
    if not _loaded:
        st.info("엑셀 파일을 업로드하면 아이템위너 현황이 표시됩니다.")
        return

    st.divider()

    # ── 전체 계정 요약 ──
    _summary = []
    _total_all, _winner_all, _loser_all = 0, 0, 0
    _total_sales_all, _winner_sales_all = 0, 0
    for _acct, _df in _loaded.items():
        _w = len(_df[_df["위너여부"] == "위너"])
        _l = len(_df) - _w
        _rate = f"{_w / len(_df) * 100:.1f}%" if len(_df) > 0 else "-"
        # 매출 집계
        _sales_col = next((c for c in _df.columns if "나의 지난주 매출" in c or "지난주 매출" in c), None)
        _my_sales = 0
        _w_sales = 0
        if _sales_col:
            _s = _df[_sales_col].str.replace(",", "", regex=False).apply(pd.to_numeric, errors="coerce").fillna(0)
            _my_sales = int(_s.sum())
            _w_sales = int(_s[_df["위너여부"] == "위너"].sum())
        _summary.append({"계정": _acct, "전체": len(_df), "위너": _w, "비위너": _l,
                         "위너율": _rate, "총매출": f"{_my_sales:,}원", "위너매출": f"{_w_sales:,}원"})
        _total_all += len(_df)
        _winner_all += _w
        _loser_all += _l
        _total_sales_all += _my_sales
        _winner_sales_all += _w_sales

    # 전체 KPI
    _k1, _k2, _k3, _k4, _k5 = st.columns(5)
    _k1.metric("전체 상품", f"{_total_all:,}건")
    _k2.metric("위너", f"{_winner_all:,}건")
    _k3.metric("비위너", f"{_loser_all:,}건")
    _k4.metric("위너율", f"{_winner_all / _total_all * 100:.1f}%" if _total_all else "-")
    _k5.metric("위너 매출 비중", f"{_winner_sales_all / _total_sales_all * 100:.1f}%" if _total_sales_all else "-")

    if len(_summary) > 1:
        st.dataframe(pd.DataFrame(_summary), use_container_width=True, hide_index=True)

    st.divider()

    # ── 계정별 상세 ──
    for _acct, _df in _loaded.items():
        with st.expander(f"📦 {_acct} ({len(_df)}건)", expanded=len(_loaded) == 1):
            # 컬럼명 매핑 (단축)
            _COL = {}
            for c in _df.columns:
                if "노출상품" in c:
                    _COL["pid"] = c
                elif "옵션ID" in c and "옵션ID" not in _COL:
                    _COL["vid"] = c
                elif c == "상품명":
                    _COL["name"] = c
                elif "내판매가" in c:
                    _COL["my_price"] = c
                elif "아이템위너가 대비" in c:
                    _COL["diff"] = c
                elif "가격범위" in c:
                    _COL["range"] = c
                elif "쿠팡추천가" in c:
                    _COL["rec"] = c
                elif "시작/중지" in c:
                    _COL["status"] = c
                elif "나의 지난주 매출" in c:
                    _COL["my_sales"] = c
                elif "나의 지난주 판매개수" in c:
                    _COL["my_qty"] = c
                elif "판매 점유율" in c:
                    _COL["share"] = c
                elif "쿠팡전체매출" in c:
                    _COL["total_sales"] = c
                elif "쿠팡전체 판매개수" in c:
                    _COL["total_qty"] = c
                elif "예상매출" in c:
                    _COL["forecast"] = c
                elif "판매자 상품코드" in c:
                    _COL["isbn"] = c

            # 필터
            _fc1, _fc2, _fc3 = st.columns([1, 1, 3])
            with _fc1:
                _w_filter = st.selectbox("위너 필터", ["전체", "위너만", "비위너만"], key=f"wf_{_acct}")
            with _fc2:
                if "status" in _COL:
                    _st_vals = ["전체"] + sorted(_df[_COL["status"]].dropna().unique().tolist())
                    _st_filter = st.selectbox("시작/중지", _st_vals, key=f"wsf_{_acct}")
                else:
                    _st_filter = "전체"
            with _fc3:
                _w_search = st.text_input("검색 (상품명 / ISBN)", key=f"ws_{_acct}")

            _filtered = _df.copy()
            if _w_filter == "위너만":
                _filtered = _filtered[_filtered["위너여부"] == "위너"]
            elif _w_filter == "비위너만":
                _filtered = _filtered[_filtered["위너여부"] == "비위너"]
            if "status" in _COL and _st_filter != "전체":
                _filtered = _filtered[_filtered[_COL["status"]] == _st_filter]
            if _w_search:
                _mask = pd.Series(False, index=_filtered.index)
                if "name" in _COL:
                    _mask |= _filtered[_COL["name"]].str.contains(_w_search, case=False, na=False)
                if "isbn" in _COL:
                    _mask |= _filtered[_COL["isbn"]].str.contains(_w_search, case=False, na=False)
                if "pid" in _COL:
                    _mask |= _filtered[_COL["pid"]].str.contains(_w_search, case=False, na=False)
                _filtered = _filtered[_mask]

            if _filtered.empty:
                st.info("조건에 맞는 상품이 없습니다.")
                continue

            # KPI
            _w_cnt = len(_filtered[_filtered["위너여부"] == "위너"])
            _l_cnt = len(_filtered) - _w_cnt
            _rate = f"{_w_cnt / len(_filtered) * 100:.1f}%" if len(_filtered) > 0 else "-"

            _ki1, _ki2, _ki3, _ki4 = st.columns(4)
            _ki1.metric("표시 상품", f"{len(_filtered):,}건")
            _ki2.metric("위너", f"{_w_cnt:,}건")
            _ki3.metric("비위너", f"{_l_cnt:,}건")
            _ki4.metric("위너율", _rate)

            # 테이블 — 핵심 컬럼 선택
            _show_cols = ["위너여부"]
            for _key in ["name", "my_price", "diff", "rec", "share", "my_sales", "my_qty",
                         "total_sales", "total_qty", "forecast", "status", "pid", "vid", "isbn"]:
                if _key in _COL and _COL[_key] in _filtered.columns:
                    _show_cols.append(_COL[_key])
            _grid_df = _filtered[_show_cols].copy()

            gb = GridOptionsBuilder.from_dataframe(_grid_df)
            gb.configure_selection(selection_mode="single", use_checkbox=False)
            if "name" in _COL:
                gb.configure_column(_COL["name"], minWidth=250)
            gb.configure_column("위너여부", width=80)
            gb.configure_grid_options(domLayout="normal")
            _grid = AgGrid(
                _grid_df,
                gridOptions=gb.build(),
                update_on=["selectionChanged"],
                height=450,
                theme="streamlit",
                key=f"wg_{_acct}",
            )

            # 행 선택 → DB 매칭 + 상세
            _sel_rows = _grid["selected_rows"]
            if _sel_rows is not None and len(_sel_rows) > 0:
                _sel = _sel_rows.iloc[0] if hasattr(_sel_rows, "iloc") else pd.Series(_sel_rows[0])

                st.divider()
                _sel_name = str(_sel.get(_COL.get("name", ""), ""))[:60]
                _sel_winner = _sel.get("위너여부", "")
                _badge = "**위너**" if _sel_winner == "위너" else "비위너"
                st.markdown(f"#### {_sel_name}  ({_badge})")

                # 엑셀 데이터 상세
                _dc1, _dc2, _dc3, _dc4, _dc5 = st.columns(5)
                _my_p = str(_sel.get(_COL.get("my_price", ""), "-"))
                _rec_p = str(_sel.get(_COL.get("rec", ""), "-"))
                _diff_p = str(_sel.get(_COL.get("diff", ""), "-"))
                _share_v = str(_sel.get(_COL.get("share", ""), "-"))
                _forecast_v = str(_sel.get(_COL.get("forecast", ""), "-"))
                _dc1.metric("내 판매가", f"{_my_p}원")
                _dc2.metric("쿠팡추천가", f"{_rec_p}원")
                _dc3.metric("위너가 대비 차이", f"{_diff_p}원")
                _dc4.metric("판매 점유율", _share_v)
                _dc5.metric("28일 예상매출", f"{_forecast_v}원")

                # DB 매칭
                _match_df = pd.DataFrame()
                _vid_val = str(_sel.get(_COL.get("vid", ""), "")).strip()
                _pid_val = str(_sel.get(_COL.get("pid", ""), "")).strip()
                if _vid_val:
                    _match_df = query_df(
                        "SELECT l.product_name, l.sale_price, l.original_price, l.isbn, "
                        "l.coupang_status, l.brand, l.coupang_product_id "
                        "FROM listings l JOIN accounts a ON a.id = l.account_id "
                        "WHERE CAST(l.vendor_item_id AS TEXT) = :vid AND a.account_name = :acct LIMIT 1",
                        {"vid": _vid_val, "acct": _acct},
                    )
                if _match_df.empty and _pid_val:
                    _match_df = query_df(
                        "SELECT l.product_name, l.sale_price, l.original_price, l.isbn, "
                        "l.coupang_status, l.brand, l.coupang_product_id "
                        "FROM listings l JOIN accounts a ON a.id = l.account_id "
                        "WHERE CAST(l.coupang_product_id AS TEXT) = :cpid AND a.account_name = :acct LIMIT 1",
                        {"cpid": _pid_val, "acct": _acct},
                    )

                if not _match_df.empty:
                    _m = _match_df.iloc[0]
                    _mc1, _mc2, _mc3, _mc4 = st.columns(4)
                    _mc1.metric("DB 판매가", f"{int(_m.get('sale_price') or 0):,}원")
                    _mc2.metric("DB 정가", f"{int(_m.get('original_price') or 0):,}원")
                    _mc3.metric("DB 상태", str(_m.get("coupang_status", "-")))
                    _mc4.metric("출판사", str(_m.get("brand", "-"))[:15])

            # 엑셀 다운로드
            _xl_buf = io.BytesIO()
            with pd.ExcelWriter(_xl_buf, engine="openpyxl") as writer:
                _filtered[_show_cols].to_excel(writer, sheet_name="아이템위너", index=False)
            _xl_buf.seek(0)
            st.download_button(
                f"엑셀 다운로드 ({len(_filtered):,}건)",
                _xl_buf.getvalue(),
                file_name=f"아이템위너_{_acct}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"wd_{_acct}",
            )


# ═══════════════════════════════════════════════════════════════
# Tab 3: 불일치/누락 관리
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

    # 누락 항목 컬럼 생성 (여러 항목 동시 누락 가능)
    if not missing.empty:
        def _get_missing_items(row):
            items = []
            vid = row.get("VID", "")
            if not vid or vid == "0":
                items.append("VID")
            if not row.get("판매가") or int(row.get("판매가", 0) or 0) == 0:
                items.append("판매가")
            if not row.get("정가") or int(row.get("정가", 0) or 0) == 0:
                items.append("정가")
            isbn = row.get("ISBN", "")
            if not isbn or (isinstance(isbn, float) and pd.isna(isbn)):
                items.append("ISBN")
            name = row.get("상품명", "")
            if not name or name == "(미등록)":
                items.append("상품명")
            return ", ".join(items) if items else "-"
        missing["누락항목"] = missing.apply(_get_missing_items, axis=1)

    if not missing.empty:
        # ── 누락 유형별 집계 ──
        _vid_cnt = len(missing[missing["누락항목"].str.contains("VID")])
        _sp_cnt = len(missing[missing["누락항목"].str.contains("판매가")])
        _op_cnt = len(missing[missing["누락항목"].str.contains("정가")])
        _isbn_cnt = len(missing[missing["누락항목"].str.contains("ISBN")])
        _name_cnt = len(missing[missing["누락항목"].str.contains("상품명")])
        _has_cpid = len(missing[missing["쿠팡ID"] != "-"])

        mk1, mk2, mk3, mk4, mk5, mk6 = st.columns(6)
        mk1.metric("전체 누락", f"{len(missing)}건")
        mk2.metric("VID 없음", f"{_vid_cnt}건")
        mk3.metric("판매가 없음", f"{_sp_cnt}건")
        mk4.metric("정가 없음", f"{_op_cnt}건")
        mk5.metric("ISBN 없음", f"{_isbn_cnt}건")
        mk6.metric("쿠팡ID 있음", f"{_has_cpid}건", help="WING API로 자동 채우기 가능")

        # ═══════════════════════════════════════════════
        # 일괄 자동 채우기
        # ═══════════════════════════════════════════════
        st.markdown("##### 일괄 자동 채우기")
        st.caption("쿠팡ID가 있는 상품은 WING API에서 VID/ISBN/가격/상품명을 자동으로 가져옵니다. ISBN만 없는 상품은 books 테이블에서 제목 매칭으로 채웁니다.")

        _af1, _af2, _af3 = st.columns([1, 1, 1])
        with _af1:
            _auto_scope = st.radio(
                "범위", ["전체", "VID 없음만", "ISBN 없음만"],
                horizontal=True, key="mm_auto_scope"
            )
        with _af2:
            _auto_api_sync = st.checkbox(
                "WING API 가격 수정 (판매가/정가)",
                value=False, key="mm_auto_api_sync",
                help="체크하면 DB뿐 아니라 쿠팡에도 가격 반영"
            )
        with _af3:
            _auto_api_put = st.checkbox(
                "WING API 상품 수정 (상품명/ISBN)",
                value=False, key="mm_auto_api_put",
                help="체크하면 상품명·ISBN 변경을 쿠팡에 반영 (재승인 필요)"
            )

        _btn_auto = st.button(
            f"자동 채우기 실행 ({_has_cpid}건 API 조회)",
            type="primary", key="mm_btn_autofill",
            disabled=(_has_cpid == 0 and _isbn_cnt == 0),
        )

        if _btn_auto:
            _isbn_re = re.compile(r'97[89]\d{10}')
            _prog = st.progress(0, text="준비 중...")
            _result_box = st.container()
            _ok, _fail, _skip, _api_ok = 0, 0, 0, 0
            _details = []  # 결과 로그

            # 범위 필터링
            _targets = missing.copy()
            if _auto_scope == "VID 없음만":
                _targets = _targets[_targets["누락항목"].str.contains("VID")]
            elif _auto_scope == "ISBN 없음만":
                _targets = _targets[_targets["누락항목"].str.contains("ISBN")]

            total = len(_targets)
            if total == 0:
                st.info("대상 상품이 없습니다.")
            else:
                # WING 클라이언트 캐시 (계정별)
                _client_cache = {}

                def _get_client(acct_name):
                    if acct_name in _client_cache:
                        return _client_cache[acct_name]
                    _m = accounts_df["account_name"] == acct_name
                    if not _m.any():
                        _client_cache[acct_name] = None
                        return None
                    c = create_wing_client(accounts_df[_m].iloc[0])
                    _client_cache[acct_name] = c
                    return c

                for _i, (_, row) in enumerate(_targets.iterrows()):
                    _prog.progress((_i + 1) / total, text=f"[{_i+1}/{total}] {str(row.get('상품명', ''))[:30]}...")

                    lid = int(row["_lid"])
                    acct_name = row["계정"]
                    cpid = str(row.get("쿠팡ID", "-") or "-")
                    cur_vid = str(row.get("VID", "") or "")
                    cur_isbn = str(row.get("ISBN", "") or "")
                    cur_sp = int(row.get("판매가", 0) or 0)
                    cur_op = int(row.get("정가", 0) or 0)
                    cur_name = str(row.get("상품명", "") or "")

                    updates = {}  # DB 업데이트할 필드
                    _api_detail_data = None  # update_product용 원본 데이터

                    # ── 전략 1: WING API get_product() ──
                    if cpid and cpid != "-":
                        client = _get_client(acct_name)
                        if client:
                            try:
                                detail = client.get_product(int(cpid))
                                data = detail.get("data", {})
                                if isinstance(data, dict):
                                    _api_detail_data = data  # PUT용 보관
                                    items = data.get("items", [])
                                    if items:
                                        item = items[0]

                                        # VID
                                        api_vid = item.get("vendorItemId")
                                        if api_vid and (not cur_vid or cur_vid == "0"):
                                            updates["vendor_item_id"] = int(api_vid)

                                        # ISBN (barcode → externalVendorSku → searchTags)
                                        if not cur_isbn or (isinstance(cur_isbn, float) and pd.isna(cur_isbn)):
                                            _isbn_found = ""
                                            for field in ["barcode", "externalVendorSku"]:
                                                m = _isbn_re.search(str(item.get(field, "")))
                                                if m:
                                                    _isbn_found = m.group()
                                                    break
                                            if not _isbn_found:
                                                for tag in (item.get("searchTags") or []):
                                                    m = _isbn_re.search(str(tag))
                                                    if m:
                                                        _isbn_found = m.group()
                                                        break
                                            if _isbn_found:
                                                updates["isbn"] = _isbn_found

                                        # 판매가
                                        api_sp = item.get("salePrice")
                                        if api_sp and isinstance(api_sp, (int, float)) and int(api_sp) > 0 and cur_sp == 0:
                                            updates["sale_price"] = int(api_sp)

                                        # 정가 (originalPrice)
                                        api_op = item.get("originalPrice")
                                        if api_op and isinstance(api_op, (int, float)) and int(api_op) > 0 and cur_op == 0:
                                            updates["original_price"] = int(api_op)

                                        # 상품명
                                        api_name = data.get("sellerProductName", "")
                                        if api_name and (not cur_name or cur_name == "(미등록)"):
                                            updates["product_name"] = api_name

                                time.sleep(0.12)  # rate limit
                            except CoupangWingError as e:
                                _details.append({"계정": acct_name, "상품명": cur_name[:30], "결과": f"API 오류: {e.message[:40]}"})
                                _fail += 1
                                continue
                            except Exception as e:
                                _details.append({"계정": acct_name, "상품명": cur_name[:30], "결과": f"오류: {str(e)[:40]}"})
                                _fail += 1
                                continue

                    # ── 전략 2: books 테이블 제목 매칭 (ISBN만 없을 때) ──
                    if "isbn" not in updates and (not cur_isbn or (isinstance(cur_isbn, float) and pd.isna(cur_isbn))):
                        _clean_name = cur_name
                        if _clean_name and _clean_name != "(미등록)" and len(_clean_name) >= 5:
                            _clean = re.sub(r'\[[^\]]*\]', '', _clean_name)
                            _clean = re.sub(r'\([^)]*\)', '', _clean)
                            _clean = re.sub(r'\d{4}년?', '', _clean)
                            _clean = re.sub(r'세트\d*', '', _clean)
                            _clean = ' '.join(_clean.split()).strip().lower()
                            if len(_clean) >= 5:
                                _kw = _clean[:40]
                                _book_match = query_df_cached(
                                    "SELECT isbn FROM books WHERE LOWER(title) LIKE :kw AND isbn IS NOT NULL LIMIT 1",
                                    {"kw": f"%{_kw}%"}
                                )
                                if not _book_match.empty:
                                    updates["isbn"] = str(_book_match.iloc[0]["isbn"])

                    # ── DB 업데이트 ──
                    if updates:
                        try:
                            _set_parts = []
                            _params = {"id": lid}
                            for k, v in updates.items():
                                _set_parts.append(f"{k}=:{k}")
                                _params[k] = v
                            run_sql(f"UPDATE listings SET {', '.join(_set_parts)} WHERE id=:id", _params)

                            # ── WING API 가격 수정 (옵션) ──
                            if _auto_api_sync:
                                _vid_for_api = updates.get("vendor_item_id") or (int(cur_vid) if cur_vid and cur_vid.isdigit() else 0)
                                if _vid_for_api and _vid_for_api > 0:
                                    _c = _get_client(acct_name)
                                    if _c:
                                        _new_sp = updates.get("sale_price")
                                        _new_op = updates.get("original_price")
                                        if _new_sp and _new_sp > 0:
                                            try:
                                                _c.update_price(int(_vid_for_api), _new_sp, dashboard_override=True)
                                                _api_ok += 1
                                            except Exception:
                                                pass
                                        if _new_op and _new_op > 0:
                                            try:
                                                _c.update_original_price(int(_vid_for_api), _new_op, dashboard_override=True)
                                                _api_ok += 1
                                            except Exception:
                                                pass

                            # ── WING API 상품 수정 — 상품명/ISBN (PUT, 재승인 필요) ──
                            if _auto_api_put and _api_detail_data and cpid and cpid != "-":
                                _need_put = False
                                import copy as _copy
                                _put_body = _copy.deepcopy(_api_detail_data)

                                # 상품명 변경
                                _new_name = updates.get("product_name")
                                if _new_name:
                                    _put_body["sellerProductName"] = _new_name
                                    for _it in _put_body.get("items", []):
                                        _it["itemName"] = _new_name
                                    _need_put = True

                                # ISBN → barcode 변경
                                _new_isbn = updates.get("isbn")
                                if _new_isbn:
                                    for _it in _put_body.get("items", []):
                                        _it["barcode"] = _new_isbn
                                        _it["externalVendorSku"] = _new_isbn
                                    _need_put = True

                                if _need_put:
                                    # 읽기전용 필드 제거
                                    for _rk in [
                                        "productId", "categoryId", "trackingId",
                                        "displayProductName", "generalProductName",
                                        "mdId", "mdName", "statusName", "status",
                                        "contributorType", "requested",
                                        "requiredDocuments", "extraInfoMessage",
                                        "roleCode", "multiShippingInfos", "multiReturnInfos",
                                    ]:
                                        _put_body.pop(_rk, None)
                                    for _it in _put_body.get("items", []):
                                        for _rk in [
                                            "supplyPrice", "saleAgentCommission",
                                            "isAutoGenerated", "freePriceType",
                                            "bestPriceGuaranteed3P",
                                        ]:
                                            _it.pop(_rk, None)

                                    _c = _get_client(acct_name)
                                    if _c:
                                        try:
                                            _resp = _c.update_product(int(cpid), _put_body)
                                            _rcode = _resp.get("code", "")
                                            if _rcode == "ERROR":
                                                _details.append({"계정": acct_name, "상품명": cur_name[:30],
                                                                 "결과": f"PUT 실패: {_resp.get('message', '')[:40]}"})
                                            else:
                                                _api_ok += 1
                                            time.sleep(0.3)
                                        except CoupangWingError as e:
                                            _details.append({"계정": acct_name, "상품명": cur_name[:30],
                                                             "결과": f"PUT 오류: {e.message[:40]}"})
                                        except Exception as e:
                                            _details.append({"계정": acct_name, "상품명": cur_name[:30],
                                                             "결과": f"PUT 예외: {str(e)[:40]}"})

                            _ok += 1
                            _filled = ", ".join(f"{k}={v}" for k, v in updates.items())
                            _details.append({"계정": acct_name, "상품명": cur_name[:30], "결과": f"채움: {_filled[:60]}"})
                        except Exception as e:
                            _fail += 1
                            _details.append({"계정": acct_name, "상품명": cur_name[:30], "결과": f"DB 오류: {str(e)[:40]}"})
                    else:
                        _skip += 1

                _prog.progress(1.0, text="완료!")

                with _result_box:
                    st.success(f"자동 채우기 완료: 성공 {_ok}건, 실패 {_fail}건, 변경없음 {_skip}건"
                               + (f", API 수정 {_api_ok}건" if _api_ok > 0 else ""))
                    if _details:
                        _detail_df = pd.DataFrame(_details)
                        # 채운 것과 실패한 것 분리
                        _filled_df = _detail_df[_detail_df["결과"].str.startswith("채움")]
                        _error_df = _detail_df[~_detail_df["결과"].str.startswith("채움") & (_detail_df["결과"] != "")]
                        if not _filled_df.empty:
                            with st.expander(f"채움 상세 ({len(_filled_df)}건)"):
                                st.dataframe(_filled_df, use_container_width=True, hide_index=True)
                        if not _error_df.empty:
                            with st.expander(f"오류 상세 ({len(_error_df)}건)"):
                                st.dataframe(_error_df, use_container_width=True, hide_index=True)

                st.cache_data.clear()
                # rerun 하지 않음 — 결과를 먼저 확인할 수 있도록
                st.info("새로고침하면 최신 누락 목록을 볼 수 있습니다.")

        st.divider()

        # ── 누락 목록 테이블 + 단건 수정 ──
        st.markdown("##### 누락 목록")
        _ms_display = missing[["계정", "상품명", "누락항목", "판매가", "정가", "ISBN", "쿠팡ID", "VID", "상태"]].copy()
        _sl = {"active": "판매중", "paused": "판매중지", "pending": "대기", "sold_out": "품절", "rejected": "반려"}
        _ms_display["상태"] = _ms_display["상태"].map(_sl).fillna(_ms_display["상태"])

        gb_ms = GridOptionsBuilder.from_dataframe(_ms_display)
        gb_ms.configure_selection(selection_mode="single", use_checkbox=False)
        gb_ms.configure_column("상품명", minWidth=200)
        gb_ms.configure_column("누락항목", minWidth=120)
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
                _acct_name = ms_sel.get("계정", "")
                _cpid = str(ms_sel.get("쿠팡ID", "-") or "-")

                st.divider()
                st.markdown(f"**{_acct_name} — {ms_sel.get('상품명', '')}** (누락: {ms_sel.get('누락항목', '')})")

                # ── WING API 조회 버튼 ──
                if _cpid and _cpid != "-":
                    if st.button("WING API에서 정보 가져오기", key=f"mm_fetch_{_lid}"):
                        _c = create_wing_client(accounts_df[accounts_df["account_name"] == _acct_name].iloc[0])
                        if _c:
                            try:
                                _detail = _c.get_product(int(_cpid))
                                _data = _detail.get("data", {})
                                _items = _data.get("items", []) if isinstance(_data, dict) else []
                                if _items:
                                    _it = _items[0]
                                    _fetched = {
                                        "VID": _it.get("vendorItemId", ""),
                                        "판매가": _it.get("salePrice", 0),
                                        "정가": _it.get("originalPrice", 0),
                                        "상품명": _data.get("sellerProductName", ""),
                                    }
                                    # ISBN 추출
                                    _isbn_re2 = re.compile(r'97[89]\d{10}')
                                    for _f in ["barcode", "externalVendorSku"]:
                                        _m = _isbn_re2.search(str(_it.get(_f, "")))
                                        if _m:
                                            _fetched["ISBN"] = _m.group()
                                            break
                                    if "ISBN" not in _fetched:
                                        for _tag in (_it.get("searchTags") or []):
                                            _m = _isbn_re2.search(str(_tag))
                                            if _m:
                                                _fetched["ISBN"] = _m.group()
                                                break
                                    st.session_state[f"mm_fetched_{_lid}"] = _fetched
                                    st.success(f"API 조회 성공: {_fetched}")
                                else:
                                    st.warning("상품에 items 정보가 없습니다.")
                            except CoupangWingError as e:
                                st.error(f"API 오류: {e.message}")
                            except Exception as e:
                                st.error(f"조회 실패: {e}")

                # 기존 값 또는 API에서 가져온 값으로 폼 초기화
                _fetched = st.session_state.get(f"mm_fetched_{_lid}", {})

                with st.form(f"mm_fix_{_lid}"):
                    mf1, mf2, mf3 = st.columns(3)
                    with mf1:
                        fix_name = st.text_input("상품명", value=_fetched.get("상품명") or ms_sel.get("상품명", "") or "")
                    with mf2:
                        fix_sp = st.number_input("판매가", value=int(_fetched.get("판매가") or ms_sel.get("판매가", 0) or 0), step=100)
                    with mf3:
                        fix_op = st.number_input("정가", value=int(_fetched.get("정가") or ms_sel.get("정가", 0) or 0), step=100)
                    mf4, mf5 = st.columns(2)
                    with mf4:
                        fix_isbn = st.text_input("ISBN", value=str(_fetched.get("ISBN", "") or ms_sel.get("ISBN", "") or ""))
                    with mf5:
                        fix_vid = st.text_input("VID", value=str(_fetched.get("VID", "") or ms_sel.get("VID", "") or ""))

                    _fix_api = st.checkbox("WING API 가격 반영", value=bool(_fetched), key=f"mm_fix_api_{_lid}")
                    _fix_put = st.checkbox("WING API 상품명/ISBN 반영 (재승인 필요)", value=False, key=f"mm_fix_put_{_lid}")

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
                            if fix_vid and fix_vid.isdigit():
                                _update_parts.append("vendor_item_id=:vid")
                                _params["vid"] = int(fix_vid)
                            if _update_parts:
                                run_sql(f"UPDATE listings SET {', '.join(_update_parts)} WHERE id=:id", _params)

                                _c = create_wing_client(accounts_df[accounts_df["account_name"] == _acct_name].iloc[0]) if (_fix_api or _fix_put) else None
                                _api_msgs = []

                                # WING API 가격 반영
                                if _fix_api and _c:
                                    _api_vid = int(fix_vid) if fix_vid and fix_vid.isdigit() else 0
                                    if _api_vid > 0:
                                        if fix_sp > 0:
                                            try:
                                                _c.update_price(_api_vid, fix_sp, dashboard_override=True)
                                                _api_msgs.append("판매가 OK")
                                            except CoupangWingError as e:
                                                _api_msgs.append(f"판매가 실패: {e.message[:30]}")
                                        if fix_op > 0:
                                            try:
                                                _c.update_original_price(_api_vid, fix_op, dashboard_override=True)
                                                _api_msgs.append("정가 OK")
                                            except CoupangWingError as e:
                                                _api_msgs.append(f"정가 실패: {e.message[:30]}")

                                # WING API 상품명/ISBN 반영 (PUT)
                                if _fix_put and _c and _cpid and _cpid != "-":
                                    try:
                                        import copy as _copy
                                        _detail = _c.get_product(int(_cpid))
                                        _put_body = _copy.deepcopy(_detail.get("data", {}))
                                        _changed = False
                                        if fix_name and isinstance(_put_body, dict):
                                            _put_body["sellerProductName"] = fix_name
                                            for _it in _put_body.get("items", []):
                                                _it["itemName"] = fix_name
                                            _changed = True
                                        if fix_isbn and isinstance(_put_body, dict):
                                            for _it in _put_body.get("items", []):
                                                _it["barcode"] = fix_isbn
                                                _it["externalVendorSku"] = fix_isbn
                                            _changed = True
                                        if _changed:
                                            for _rk in ["productId", "categoryId", "trackingId",
                                                        "displayProductName", "generalProductName",
                                                        "mdId", "mdName", "statusName", "status",
                                                        "contributorType", "requested",
                                                        "requiredDocuments", "extraInfoMessage",
                                                        "roleCode", "multiShippingInfos", "multiReturnInfos"]:
                                                _put_body.pop(_rk, None)
                                            for _it in _put_body.get("items", []):
                                                for _rk in ["supplyPrice", "saleAgentCommission",
                                                            "isAutoGenerated", "freePriceType",
                                                            "bestPriceGuaranteed3P"]:
                                                    _it.pop(_rk, None)
                                            _resp = _c.update_product(int(_cpid), _put_body)
                                            if _resp.get("code") == "ERROR":
                                                _api_msgs.append(f"PUT 실패: {_resp.get('message', '')[:30]}")
                                            else:
                                                _api_msgs.append("상품명/ISBN PUT OK (재승인 대기)")
                                    except CoupangWingError as e:
                                        _api_msgs.append(f"PUT 오류: {e.message[:30]}")
                                    except Exception as e:
                                        _api_msgs.append(f"PUT 예외: {str(e)[:30]}")

                                if _api_msgs:
                                    st.info(f"API: {', '.join(_api_msgs)}")

                                st.success("저장 완료")
                                st.session_state.pop(f"mm_fetched_{_lid}", None)
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
