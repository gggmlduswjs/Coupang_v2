"""
ISBN 채우기 Step 2: books 테이블 매칭 + 알라딘 API
실행: python scripts/fill_isbn_step2.py
"""
import sys
import re
import time
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from sqlalchemy import text
from dashboard.utils import engine

isbn_re = re.compile(r'97[89]\d{10}')


def clean_name(name: str, remove_year: bool = True) -> str:
    t = re.sub(r'\[[^\]]*\]', '', name)
    t = re.sub(r'\([^)]*\)', '', t)
    if remove_year:
        t = re.sub(r'\d{4}년?', '', t)
    t = re.sub(r'세트\d*|전\s*\d+권', '', t)
    t = re.sub(r'\s*[+&]\s*', ' ', t)
    for w in ['사은품', '선물', '증정', '포함', '무료배송', '슝슝오늘출발']:
        t = t.replace(w, '')
    return ' '.join(t.split()).strip()


def extract_year(name: str):
    m = re.search(r'(20\d{2})년?', name)
    if m:
        y = int(m.group(1))
        return y if 2020 <= y <= 2030 else None
    return None


# ── Step 1: books 테이블 매칭 ──────────────────────────
print("=" * 50)
print("Pass 1: books 테이블 제목 매칭")
print("=" * 50, flush=True)

with engine.connect() as conn:
    null_rows = conn.execute(text("""
        SELECT l.id, l.account_id, l.product_name
        FROM listings l
        WHERE l.isbn IS NULL AND l.product_name IS NOT NULL AND l.product_name <> ''
        ORDER BY l.id
    """)).fetchall()
    print(f"대상: {len(null_rows)}건", flush=True)

    books_filled = 0
    for i, row in enumerate(null_rows):
        lid, aid, pname = row
        clean = clean_name(pname).lower()
        if len(clean) < 5:
            continue

        keyword = clean[:40]
        matches = conn.execute(text("""
            SELECT isbn FROM books
            WHERE LOWER(title) LIKE :kw AND isbn IS NOT NULL
            LIMIT 2
        """), {"kw": f"%{keyword}%"}).fetchall()

        if len(matches) == 1:  # 2개 이상이면 애매 → 스킵
            isbn_val = matches[0][0]
            dup = conn.execute(text(
                "SELECT 1 FROM listings WHERE account_id=:aid AND isbn=:isbn AND id <> :lid"
            ), {"aid": aid, "isbn": isbn_val, "lid": lid}).first()
            if not dup:
                conn.execute(text("UPDATE listings SET isbn=:isbn WHERE id=:lid"),
                             {"isbn": isbn_val, "lid": lid})
                books_filled += 1

        if (i + 1) % 2000 == 0:
            conn.commit()
            print(f"  [{i+1}/{len(null_rows)}] books 매칭 완료: {books_filled}건", flush=True)

    conn.commit()
    print(f"Pass 1 완료: {books_filled}건 업데이트\n", flush=True)


# ── Step 2: 알라딘 API ─────────────────────────────────
print("=" * 50)
print("Pass 2: 알라딘 API 검색")
print("=" * 50, flush=True)

ttb_key = os.getenv("ALADIN_TTB_KEY", "")
if not ttb_key:
    print("ALADIN_TTB_KEY 없음. 종료.")
    sys.exit(0)

try:
    from core.api.aladin_client import AladinAPICrawler
    crawler = AladinAPICrawler(ttb_key)
except Exception as e:
    print(f"알라딘 클라이언트 로드 실패: {e}")
    sys.exit(1)

with engine.connect() as conn:
    null_rows = conn.execute(text("""
        SELECT l.id, l.account_id, l.product_name
        FROM listings l
        WHERE l.isbn IS NULL AND l.product_name IS NOT NULL AND l.product_name <> ''
        ORDER BY l.id
    """)).fetchall()
    print(f"대상: {len(null_rows)}건 (0.5초/건 예상 {len(null_rows)//2//60}분)", flush=True)

    aladin_filled = 0
    aladin_fail = 0
    COMMIT_EVERY = 100

    for i, row in enumerate(null_rows):
        lid, aid, pname = row
        year = extract_year(pname)
        clean = clean_name(pname, remove_year=(year is None))
        keyword = ' '.join(clean.split()[:8])
        if not keyword or len(keyword) < 4:
            aladin_fail += 1
            continue

        sort = "PublishTime" if year else "Accuracy"
        try:
            results = crawler.search_by_keyword(keyword=keyword, max_results=5, sort=sort)
        except Exception:
            aladin_fail += 1
            time.sleep(1)
            continue

        isbn_val = None
        if results:
            if year:
                for item in results:
                    if str(year) in item.get("pubDate", ""):
                        isbn_val = item.get("isbn13") or item.get("isbn")
                        break
            if not isbn_val:
                isbn_val = results[0].get("isbn13") or results[0].get("isbn")

        if isbn_val and isbn_re.match(str(isbn_val)):
            dup = conn.execute(text(
                "SELECT 1 FROM listings WHERE account_id=:aid AND isbn=:isbn AND id <> :lid"
            ), {"aid": aid, "isbn": str(isbn_val), "lid": lid}).first()
            if not dup:
                conn.execute(text("UPDATE listings SET isbn=:isbn WHERE id=:lid"),
                             {"isbn": str(isbn_val), "lid": lid})
                aladin_filled += 1
            else:
                aladin_fail += 1
        else:
            aladin_fail += 1

        if (i + 1) % COMMIT_EVERY == 0:
            conn.commit()
            pct = (i + 1) / len(null_rows) * 100
            print(f"  [{i+1}/{len(null_rows)}] {pct:.1f}% | 성공 {aladin_filled} / 실패 {aladin_fail}", flush=True)

        time.sleep(0.5)

    conn.commit()
    print(f"Pass 2 완료: {aladin_filled}건 업데이트\n", flush=True)

    # ── 최종 커버리지 ──
    stats = conn.execute(text("""
        SELECT a.account_name, COUNT(*) as total,
               SUM(CASE WHEN l.isbn IS NOT NULL THEN 1 ELSE 0 END) as has_isbn
        FROM listings l
        JOIN accounts a ON l.account_id = a.id
        WHERE a.is_active = true
        GROUP BY a.account_name ORDER BY a.account_name
    """)).fetchall()

    print("=== 최종 ISBN 커버리지 ===")
    total_all = total_has = 0
    for name, total, has in stats:
        pct = (has / total * 100) if total > 0 else 0
        print(f"  {name}: {has}/{total} ({pct:.1f}%)")
        total_all += total
        total_has += has
    print(f"  전체: {total_has}/{total_all} ({total_has/total_all*100:.1f}%)")
