"""
캐시 JSON에서 ISBN 추출 → listings 배치 업데이트
실행: python scripts/fill_isbn_from_cache.py
"""
import glob
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from sqlalchemy import text
from dashboard.utils import engine

isbn_re = re.compile(r'97[89]\d{10}')

account_files = glob.glob(r'C:\Users\MSI\Desktop\*\data\products_*.json')
print(f"캐시 파일 {len(account_files)}개 발견", flush=True)

# ── Step 1: 캐시 JSON 전체 파싱 → {coupang_product_id: isbn, vendor_item_id: isbn}
cpid_to_isbn = {}  # str(sellerProductId) → isbn
vid_to_isbn  = {}  # str(vendorItemId)    → isbn

for fpath in account_files:
    with open(fpath, encoding='utf-8') as f:
        data = json.load(f)
    account_name = data.get('account', Path(fpath).stem)
    products = data.get('products', {})
    print(f"[{account_name}] {len(products)}개 파싱...", flush=True)

    for spid, prod in products.items():
        for it in prod.get('items', []):
            barcode = str(it.get('barcode', '') or '')
            m = isbn_re.search(barcode)
            if m:
                isbn_val = m.group()
                cpid_to_isbn[str(spid)] = isbn_val
                vid = str(it.get('vendorItemId', '') or '')
                if vid:
                    vid_to_isbn[vid] = isbn_val
                break  # 첫 ISBN이면 충분

print(f"\n캐시에서 ISBN 보유: cpid기준 {len(cpid_to_isbn)}개 / vid기준 {len(vid_to_isbn)}개", flush=True)

# ── Step 2: DB에서 ISBN NULL 리스팅 전체 조회 (1회)
with engine.connect() as conn:
    null_rows = conn.execute(text("""
        SELECT l.id, l.account_id, l.coupang_product_id, l.vendor_item_id
        FROM listings l
        WHERE l.isbn IS NULL
          AND l.coupang_product_id IS NOT NULL
    """)).fetchall()
    print(f"ISBN NULL 리스팅: {len(null_rows)}건", flush=True)

    # ── Step 3: 매칭 → 업데이트 목록 생성
    updates = []  # [(listing_id, isbn)]
    for row in null_rows:
        lid, aid, cpid, vid = row
        isbn = cpid_to_isbn.get(str(cpid) if cpid else '') or \
               vid_to_isbn.get(str(vid) if vid else '')
        if isbn:
            updates.append((lid, isbn))

    print(f"매칭 성공: {len(updates)}건 → 배치 업데이트 시작", flush=True)

    # ── Step 4: 배치 UPDATE (500건씩 커밋)
    BATCH = 500
    updated = 0
    for i in range(0, len(updates), BATCH):
        batch = updates[i:i+BATCH]
        for lid, isbn in batch:
            conn.execute(text("UPDATE listings SET isbn=:isbn WHERE id=:lid"),
                         {"isbn": isbn, "lid": lid})
        conn.commit()
        updated += len(batch)
        print(f"  [{updated}/{len(updates)}] 커밋 완료", flush=True)

    print(f"\n업데이트 완료: {updated}건", flush=True)

    # ── Step 5: 최종 커버리지
    stats = conn.execute(text("""
        SELECT a.account_name,
               COUNT(*) as total,
               SUM(CASE WHEN l.isbn IS NOT NULL THEN 1 ELSE 0 END) as has_isbn
        FROM listings l
        JOIN accounts a ON l.account_id = a.id
        WHERE a.is_active = true
        GROUP BY a.account_name
        ORDER BY a.account_name
    """)).fetchall()

    print("\n=== ISBN 커버리지 (업데이트 후) ===")
    for name, total, has in stats:
        pct = (has / total * 100) if total > 0 else 0
        print(f"  {name}: {has}/{total} ({pct:.1f}%)")
