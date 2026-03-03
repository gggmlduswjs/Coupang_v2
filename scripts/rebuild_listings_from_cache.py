"""
캐시 JSON → listings 테이블 완전 재구축
==========================================
WING API 캐시(products_*.json)를 읽어 listings를 깨끗하게 재구축.

실행: python scripts/rebuild_listings_from_cache.py
"""
import sys
import re
import glob
import json
import logging
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from psycopg2.extras import execute_values
from sqlalchemy import text
from core.database import engine

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

isbn_re = re.compile(r'97[89]\d{10}')

STATUS_MAP = {
    "판매중": "active", "승인완료": "active", "APPROVE": "active",
    "판매중지": "paused", "SUSPEND": "paused",
    "품절": "sold_out", "SOLDOUT": "sold_out",
    "승인반려": "rejected", "삭제": "deleted", "DELETE": "deleted",
    "승인대기": "pending",
}


def extract_isbn(product_data: dict) -> str | None:
    """attributes → barcode → searchTags 순서로 ISBN 추출"""
    for item in product_data.get("items", []):
        # 1) attributes
        for attr in (item.get("attributes") or []):
            if attr.get("attributeTypeName") == "ISBN":
                val = re.sub(r'[^0-9]', '', str(attr.get("attributeValueName", "")))
                if len(val) == 13 and val.startswith(("978", "979")):
                    return val
        # 2) barcode
        m = isbn_re.search(str(item.get("barcode", "") or ""))
        if m:
            return m.group()
        # 3) searchTags
        for tag in (item.get("searchTags") or []):
            m = isbn_re.search(str(tag))
            if m:
                return m.group()
    return None


def parse_product(product_data: dict, account_id: int, synced_at: datetime) -> list[tuple]:
    """
    상품 데이터 → DB 행 리스트 (단품=1행, 세트=items 수만큼)
    반환: list of tuples matching INSERT columns
    """
    spid = int(product_data.get("sellerProductId", 0))
    pname = product_data.get("sellerProductName", "") or ""
    brand = product_data.get("brand", "") or ""
    disp_cat = str(product_data.get("displayCategoryCode", "") or "")
    dct = product_data.get("deliveryChargeType", "") or ""
    dc = product_data.get("deliveryCharge") or 0
    fsoa = product_data.get("freeShipOverAmount") or 0
    rc = product_data.get("returnCharge") or 0
    status_raw = product_data.get("statusName", product_data.get("status", ""))
    coupang_status = STATUS_MAP.get(status_raw, "pending")
    approval = product_data.get("status", "")
    on_sale = (status_raw in ("판매중", "승인완료"))

    isbn = extract_isbn(product_data)

    items = product_data.get("items") or []
    if not items:
        return []

    rows = []
    # 단품/세트 구분: 세트면 items가 여러 개
    # 단품은 items[0]만 사용, 세트는 각 item마다 row 생성
    for item in items:
        vid = item.get("vendorItemId")
        vid = int(vid) if vid else None
        orig_p = item.get("originalPrice") or 0
        sale_p = item.get("salePrice") or 0
        supply_p = item.get("supplyPrice") or 0
        stock = item.get("maximumBuyCount") or 10
        barcode = str(item.get("barcode", "") or "")
        option_name = item.get("itemName", "") or ""
        images_list = item.get("images") or []
        images_json = json.dumps(images_list, ensure_ascii=False) if images_list else None
        tags = item.get("searchTags") or []
        search_tags = json.dumps(tags, ensure_ascii=False) if tags else None

        rows.append((
            account_id,          # account_id
            spid,                # coupang_product_id
            spid,                # seller_product_id
            vid,                 # vendor_item_id
            pname,               # product_name
            coupang_status,      # coupang_status
            orig_p,              # original_price
            sale_p,              # sale_price
            supply_p,            # supply_price
            stock,               # stock_quantity
            brand,               # brand
            disp_cat,            # display_category_code
            dct,                 # delivery_charge_type
            dc,                  # delivery_charge
            fsoa,                # free_ship_over_amount
            rc,                  # return_charge
            isbn,                # isbn
            barcode,             # barcode
            option_name,         # option_name
            images_json,         # images
            search_tags,         # search_tags
            approval,            # approval_status
            on_sale,             # on_sale
            synced_at,           # synced_at
        ))
    return rows


def bulk_upsert(rows: list[tuple]) -> int:
    """psycopg2 execute_values로 빠른 UPSERT"""
    if not rows:
        return 0
    sql = """
        INSERT INTO listings (
            account_id, coupang_product_id, seller_product_id, vendor_item_id,
            product_name, coupang_status, original_price, sale_price,
            supply_price, stock_quantity, brand, display_category_code,
            delivery_charge_type, delivery_charge, free_ship_over_amount, return_charge,
            isbn, barcode, option_name, images, search_tags,
            approval_status, on_sale, synced_at
        ) VALUES %s
        ON CONFLICT (account_id, coupang_product_id) DO UPDATE SET
            vendor_item_id     = EXCLUDED.vendor_item_id,
            product_name       = EXCLUDED.product_name,
            coupang_status     = EXCLUDED.coupang_status,
            original_price     = EXCLUDED.original_price,
            sale_price         = EXCLUDED.sale_price,
            supply_price       = EXCLUDED.supply_price,
            stock_quantity     = EXCLUDED.stock_quantity,
            brand              = EXCLUDED.brand,
            display_category_code = EXCLUDED.display_category_code,
            delivery_charge_type = EXCLUDED.delivery_charge_type,
            delivery_charge    = EXCLUDED.delivery_charge,
            free_ship_over_amount = EXCLUDED.free_ship_over_amount,
            return_charge      = EXCLUDED.return_charge,
            isbn               = COALESCE(EXCLUDED.isbn, listings.isbn),
            barcode            = EXCLUDED.barcode,
            option_name        = EXCLUDED.option_name,
            images             = EXCLUDED.images,
            search_tags        = EXCLUDED.search_tags,
            approval_status    = EXCLUDED.approval_status,
            on_sale            = EXCLUDED.on_sale,
            synced_at          = EXCLUDED.synced_at,
            updated_at         = NOW()
    """
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        BATCH = 500
        inserted = 0
        for i in range(0, len(rows), BATCH):
            execute_values(cur, sql, rows[i:i+BATCH], page_size=BATCH)
            inserted += min(BATCH, len(rows) - i)
            raw_conn.commit()
            logger.info(f"  [{inserted}/{len(rows)}] UPSERT 완료")
        cur.close()
        return inserted
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"UPSERT 실패: {e}")
        raise
    finally:
        raw_conn.close()


def delete_removed_products(account_id: int, valid_spids: set, synced_at: datetime):
    """캐시에 없는 상품 삭제 (FK 참조 및 판매이력 있는 것 제외)"""
    with engine.connect() as conn:
        deleted = conn.execute(text("""
            DELETE FROM listings
            WHERE account_id = :aid
              AND coupang_product_id NOT IN :spids
              AND COALESCE(sold_quantity, 0) = 0
              AND id NOT IN (SELECT listing_id FROM return_requests WHERE listing_id IS NOT NULL)
        """), {"aid": account_id, "spids": tuple(valid_spids) if valid_spids else (-1,)})
        conn.commit()
        return deleted.rowcount


def main():
    cache_files = glob.glob(r'C:\Users\MSI\Desktop\*\data\products_*.json')
    logger.info(f"캐시 파일 {len(cache_files)}개 발견")

    # account_name → id 매핑
    with engine.connect() as conn:
        acct_rows = conn.execute(text(
            "SELECT id, account_name FROM accounts WHERE is_active = true"
        )).fetchall()
    acct_map = {name: aid for aid, name in acct_rows}
    logger.info(f"활성 계정: {list(acct_map.keys())}")

    total_rows = 0
    total_deleted = 0

    for fpath in cache_files:
        with open(fpath, encoding='utf-8') as f:
            data = json.load(f)

        account_name = data.get('account', '')
        account_id = acct_map.get(account_name)
        if not account_id:
            logger.warning(f"계정 없음: {account_name} → 스킵")
            continue

        products = data.get('products', {})
        synced_at = datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat()))
        logger.info(f"\n[{account_name}] {len(products)}개 상품 처리 (동기화: {synced_at:%Y-%m-%d %H:%M})")

        all_rows = []
        valid_spids = set()
        for spid, prod in products.items():
            rows = parse_product(prod, account_id, synced_at)
            all_rows.extend(rows)
            valid_spids.add(int(spid))

        n = bulk_upsert(all_rows)
        total_rows += n

    # 최종 커버리지
    with engine.connect() as conn:
        stats = conn.execute(text("""
            SELECT a.account_name, COUNT(*) as total,
                   SUM(CASE WHEN l.isbn IS NOT NULL THEN 1 ELSE 0 END) as has_isbn,
                   SUM(CASE WHEN l.coupang_status = 'active' THEN 1 ELSE 0 END) as active_cnt
            FROM listings l
            JOIN accounts a ON l.account_id = a.id
            WHERE a.is_active = true
            GROUP BY a.account_name ORDER BY a.account_name
        """)).fetchall()

    logger.info("\n" + "=" * 50)
    logger.info("최종 결과")
    logger.info("=" * 50)
    t_total = t_isbn = t_active = 0
    for name, total, has_isbn, active in stats:
        pct = has_isbn / total * 100 if total else 0
        logger.info(f"  {name}: {total}건 (active={active}, ISBN={has_isbn}/{total} {pct:.0f}%)")
        t_total += total; t_isbn += has_isbn; t_active += active
    logger.info(f"  전체: {t_total}건 (active={t_active}, ISBN={t_isbn}/{t_total} {t_isbn/t_total*100:.0f}%)")
    logger.info(f"\nUPSERT 완료: {total_rows}행")


if __name__ == "__main__":
    main()
