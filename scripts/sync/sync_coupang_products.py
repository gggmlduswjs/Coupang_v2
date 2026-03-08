"""
쿠팡 WING API 상품 동기화 (2단계)
==================================
5개 계정의 기존 등록 상품을 API로 조회 → listings 테이블에 동기화

Stage 1: list_products(100개씩) → sellerProductId 목록 + 기본정보
Stage 2: get_product(id) × N개 → 전체 상세 JSON → DB 저장

사용법:
    python scripts/sync_coupang_products.py                    # 전체 5계정 동기화 (증분 상세)
    python scripts/sync_coupang_products.py --account 007-bm   # 특정 계정만
    python scripts/sync_coupang_products.py --quick             # 목록만 (Stage 1만)
    python scripts/sync_coupang_products.py --force             # 전체 상세 강제 재조회
    python scripts/sync_coupang_products.py --stale-hours 48    # 48시간 지난 것만 재조회
    python scripts/sync_coupang_products.py --max-pages 5       # 최대 5페이지만
    python scripts/sync_coupang_products.py --dry-run            # DB 저장 없이 조회만
"""
import sys
import os
import re
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

# 프로젝트 루트 설정
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from dotenv import load_dotenv
load_dotenv()

from psycopg2.extras import execute_values
from sqlalchemy import text

from core.database import SessionLocal, init_db, engine
from core.models.account import Account
from core.models.listing import Listing
from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.constants import WING_ACCOUNT_ENV_MAP
from core.obsidian_logger import ObsidianLogger

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def _extract_isbns(product_data: dict) -> list:
    """
    쿠팡 상품 데이터에서 ISBN 복수 추출

    Returns:
        ISBN 문자열 리스트 (정렬됨)
    """
    isbn_pattern = re.compile(r'97[89]\d{10}')
    found = set()
    items = product_data.get("items", [])

    for item in items:
        # 1) items[].attributes에서 ISBN 추출 (가장 정확)
        attributes = item.get("attributes", [])
        if isinstance(attributes, list):
            for attr in attributes:
                attr_name = attr.get("attributeTypeName", "")
                attr_value = attr.get("attributeValueName", "")
                if attr_name == "ISBN" and attr_value and "상세" not in attr_value:
                    cleaned = re.sub(r'[^0-9]', '', attr_value)
                    if len(cleaned) == 13 and cleaned.startswith(("978", "979")):
                        found.add(cleaned)

        # 2) items의 barcode에서 검색
        barcode = str(item.get("barcode", ""))
        for match in isbn_pattern.finditer(barcode):
            found.add(match.group())

        # 3) items의 searchTags에서 검색
        search_tags = item.get("searchTags", [])
        if isinstance(search_tags, list):
            for tag in search_tags:
                for match in isbn_pattern.finditer(str(tag)):
                    found.add(match.group())

        # 4) vendorItemName
        vendor_name = str(item.get("vendorItemName", ""))
        for match in isbn_pattern.finditer(vendor_name):
            found.add(match.group())

    # 5) sellerProductName에서 검색
    product_name = str(product_data.get("sellerProductName", ""))
    for match in isbn_pattern.finditer(product_name):
        found.add(match.group())

    return sorted(found)


def _get_vendor_item_id(product_data: dict) -> int:
    """상품 데이터에서 vendorItemId 추출 (BigInteger)"""
    items = product_data.get("items", [])
    if items:
        vid = items[0].get("vendorItemId")
        if vid:
            return int(vid)
    return None


def _get_product_status(product_data: dict) -> str:
    """상품 상태를 listings 테이블 포맷으로 변환"""
    status = product_data.get("statusName", product_data.get("status", ""))
    status_map = {
        "판매중": "active",
        "승인완료": "active",
        "APPROVE": "active",
        "판매중지": "paused",
        "SUSPEND": "paused",
        "품절": "sold_out",
        "SOLDOUT": "sold_out",
        "승인반려": "rejected",
        "삭제": "deleted",
        "DELETE": "deleted",
        "승인대기": "pending",
    }
    return status_map.get(status, "pending")


def _parse_detail_fields(detail_data: dict) -> dict:
    """상세 API 응답에서 DB 필드를 파싱"""
    result = {}

    # brand (루트 레벨)
    result["brand"] = detail_data.get("brand", "") or ""

    # displayCategoryCode
    result["display_category_code"] = str(detail_data.get("displayCategoryCode", "")) or ""

    # deliveryChargeType (배송비 유형)
    result["delivery_charge_type"] = detail_data.get("deliveryChargeType", "") or ""

    # 배송/반품 가격 (루트 레벨)
    result["delivery_charge"] = detail_data.get("deliveryCharge", None)
    result["free_ship_over_amount"] = detail_data.get("freeShipOverAmount", None)
    result["return_charge"] = detail_data.get("returnCharge", None)

    # items[0]에서 추출
    items = detail_data.get("items", [])
    if items:
        item = items[0]
        result["supply_price"] = item.get("supplyPrice", None)
        result["original_price"] = item.get("originalPrice", None)
        result["sale_price"] = item.get("salePrice", None)

        # attributes에서 ISBN, 출판사 추출
        result["isbn"] = None
        result["publisher"] = None
        attributes = item.get("attributes", [])
        if isinstance(attributes, list):
            for attr in attributes:
                attr_name = attr.get("attributeTypeName", "")
                attr_value = attr.get("attributeValueName", "")
                if attr_name == "ISBN" and attr_value and "상세" not in attr_value:
                    cleaned = re.sub(r'[^0-9]', '', attr_value)
                    if len(cleaned) == 13:
                        result["isbn"] = cleaned
                elif attr_name == "출판사" and attr_value and "상세" not in attr_value:
                    result["publisher"] = attr_value
    else:
        result["supply_price"] = None
        result["original_price"] = None
        result["sale_price"] = None
        result["isbn"] = None
        result["publisher"] = None

    return result


def create_wing_client(account: Account) -> CoupangWingClient:
    """Account 모델에서 WING API 클라이언트 생성"""
    if not account.has_wing_api:
        raise ValueError(f"{account.account_name}: WING API 정보가 없습니다")

    return CoupangWingClient(
        vendor_id=account.vendor_id,
        access_key=account.wing_access_key,
        secret_key=account.wing_secret_key,
    )


def _safe_commit(db):
    """DB 커밋"""
    db.commit()


def _bulk_upsert_listings(rows):
    """벌크 UPSERT (execute_values + ON CONFLICT DO UPDATE)"""
    if not rows:
        return
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
        for i in range(0, len(rows), BATCH):
            batch = rows[i:i + BATCH]
            execute_values(cur, sql, batch, page_size=BATCH)
        raw_conn.commit()
        cur.close()
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"벌크 UPSERT 실패: {e}")
        raise
    finally:
        raw_conn.close()


def _bulk_update_listings_by_id(rows):
    """ISBN fallback 매칭된 listings 벌크 UPDATE (id 기반)"""
    if not rows:
        return
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        sql = """
            UPDATE listings SET
                coupang_product_id = v.cpid::bigint,
                coupang_status = v.status,
                product_name = v.pname,
                vendor_item_id = COALESCE(v.vid::bigint, listings.vendor_item_id),
                sale_price = CASE WHEN v.sprice::int > 0 THEN v.sprice::int ELSE listings.sale_price END,
                original_price = CASE WHEN v.oprice::int > 0 THEN v.oprice::int ELSE listings.original_price END,
                isbn = COALESCE(listings.isbn, v.isbn),
                synced_at = v.synced::timestamp,
                updated_at = NOW()
            FROM (VALUES %s) AS v(lid, cpid, status, pname, vid, sprice, oprice, isbn, synced)
            WHERE listings.id = v.lid::int
        """
        execute_values(cur, sql, rows, page_size=500)
        raw_conn.commit()
        cur.close()
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"벌크 UPDATE 실패: {e}")
        raise
    finally:
        raw_conn.close()


def _fetch_product_detail(client: CoupangWingClient, seller_product_id: int) -> dict:
    """상품 상세 조회 (1회 재시도 포함)"""
    try:
        result = client.get_product(seller_product_id)
        # 응답에서 data 키 확인
        if isinstance(result, dict) and "data" in result:
            return result["data"] if isinstance(result["data"], dict) else result
        return result
    except CoupangWingError as e:
        # Rate limit이면 1초 대기 후 재시도
        if e.status_code == 429 or "RATE" in str(e.code).upper():
            logger.warning(f"    Rate limit, 1초 대기 후 재시도: {seller_product_id}")
            time.sleep(1)
            try:
                result = client.get_product(seller_product_id)
                if isinstance(result, dict) and "data" in result:
                    return result["data"] if isinstance(result["data"], dict) else result
                return result
            except CoupangWingError:
                pass
        raise


def sync_account_products(
    db, account: Account, max_pages: int = 0, dry_run: bool = False,
    quick: bool = False, force: bool = False, stale_hours: int = 24,
) -> dict:
    """
    단일 계정의 상품을 WING API로 조회하여 DB에 동기화

    Returns:
        {"total", "new", "updated", "isbn_found", "isbn_missing", "detail_synced", "detail_skipped", "detail_error"}
    """
    result = {
        "total": 0, "new": 0, "updated": 0,
        "isbn_found": 0, "isbn_missing": 0,
        "detail_synced": 0, "detail_skipped": 0, "detail_error": 0,
    }

    try:
        client = create_wing_client(account)
    except ValueError as e:
        logger.error(str(e))
        return result

    logger.info(f"\n{'='*50}")
    logger.info(f"동기화: {account.account_name} (vendor_id={account.vendor_id})")
    logger.info(f"{'='*50}")

    # ── Stage 1: 상품 목록 조회 ──
    try:
        products = client.list_products(max_per_page=100, max_pages=max_pages)
    except CoupangWingError as e:
        logger.error(f"  상품 목록 조회 실패: {e}")
        return result

    result["total"] = len(products)
    logger.info(f"  [Stage 1] 총 {len(products)}개 상품 조회됨")

    if dry_run:
        for product_data in products:
            isbns = _extract_isbns(product_data)
            if isbns:
                result["isbn_found"] += 1
            else:
                result["isbn_missing"] += 1
        logger.info(f"  [DRY-RUN] ISBN 추출: {result['isbn_found']}개 성공, {result['isbn_missing']}개 실패")
        return result

    # 기존 listings를 경량 dict로 로드 (ORM 객체 아닌 필수 컬럼만)
    existing_rows = db.execute(text(
        "SELECT id, coupang_product_id, isbn, vendor_item_id "
        "FROM listings WHERE account_id = :aid"
    ), {"aid": account.id}).fetchall()

    by_pid = {}   # coupang_product_id(int) → dict
    by_isbn = {}  # isbn → dict
    for row in existing_rows:
        d = {"id": row[0], "coupang_product_id": row[1], "isbn": row[2], "vendor_item_id": row[3]}
        if d["coupang_product_id"]:
            by_pid[int(d["coupang_product_id"])] = d
        if d["isbn"]:
            by_isbn[d["isbn"]] = d

    logger.info(f"  기존 DB listings: {len(existing_rows)}개 (product_id:{len(by_pid)}, isbn:{len(by_isbn)})")

    now = datetime.utcnow()
    upsert_by_pid = {}    # pid → tuple (중복 시 마지막 값 유지)
    isbn_update_rows = [] # ISBN fallback 매칭 → UPDATE by id

    for product_data in products:
        seller_product_id = int(product_data.get("sellerProductId", 0))
        isbns = _extract_isbns(product_data)
        isbn_str = ",".join(isbns) if isbns else None
        vendor_item_id = _get_vendor_item_id(product_data)
        coupang_status = _get_product_status(product_data)
        product_name = product_data.get("sellerProductName", "")

        if isbns:
            result["isbn_found"] += 1
        else:
            result["isbn_missing"] += 1

        # dict 룩업 (DB 쿼리 없음)
        existing = by_pid.get(seller_product_id)
        isbn_fallback = False
        if not existing and isbn_str:
            existing = by_isbn.get(isbn_str)
            isbn_fallback = bool(existing)

        # 가격 추출
        items = product_data.get("items", [])
        sale_price = 0
        original_price = 0
        if items:
            sale_price = items[0].get("salePrice", 0) or 0
            original_price = items[0].get("originalPrice", 0) or 0

        if existing and isbn_fallback:
            # ISBN fallback: 기존 행의 coupang_product_id가 다름 → id 기반 UPDATE
            isbn_update_rows.append((
                existing["id"], seller_product_id, coupang_status, product_name,
                vendor_item_id, sale_price, original_price, isbn_str, now,
            ))
            result["updated"] += 1
            by_pid[seller_product_id] = {**existing, "coupang_product_id": seller_product_id}
        else:
            # 신규 + 기존(pid 매칭) 모두 UPSERT로 처리
            if existing:
                result["updated"] += 1
            else:
                result["new"] += 1
                d = {"id": None, "coupang_product_id": seller_product_id,
                     "isbn": isbn_str, "vendor_item_id": vendor_item_id}
                by_pid[seller_product_id] = d
                if isbn_str:
                    by_isbn[isbn_str] = d

            upsert_by_pid[seller_product_id] = (
                account.id, seller_product_id, vendor_item_id, isbn_str,
                coupang_status, sale_price, original_price, product_name,
                now, now, now,  # synced_at, created_at, updated_at
            )

    # 벌크 UPSERT (INSERT + ON CONFLICT DO UPDATE)
    upsert_rows = list(upsert_by_pid.values())
    t0 = time.time()
    _bulk_upsert_listings(upsert_rows)

    # ISBN fallback 매칭 벌크 UPDATE
    _bulk_update_listings_by_id(isbn_update_rows)

    elapsed = time.time() - t0
    logger.info(f"  [Stage 1] 벌크 완료 ({elapsed:.1f}초): upsert {len(upsert_rows)}건, isbn-update {len(isbn_update_rows)}건")
    logger.info(f"  [Stage 1] 신규 {result['new']}개, 업데이트 {result['updated']}개")
    logger.info(f"  ISBN 추출: 성공 {result['isbn_found']}개, 실패 {result['isbn_missing']}개")

    # ── Stage 2: 상품 상세 조회 ──
    if quick:
        logger.info(f"  [Stage 2] --quick 모드: 상세 조회 생략")
        return result

    # Stage 2용 ORM 객체 재로드 (벌크 커밋 이후)
    db.expire_all()
    stage2_listings = db.query(Listing).filter(
        Listing.account_id == account.id
    ).all()
    by_product_id = {int(l.coupang_product_id): l
                     for l in stage2_listings if l.coupang_product_id}

    # 상세 조회 대상 선별
    stale_cutoff = now - timedelta(hours=stale_hours)
    detail_targets = []

    all_listings = {pid: lst for pid, lst in by_product_id.items() if pid}

    for pid, lst in all_listings.items():
        if force:
            detail_targets.append((pid, lst))
        elif lst.detail_synced_at is None:
            detail_targets.append((pid, lst))
        elif lst.detail_synced_at < stale_cutoff:
            detail_targets.append((pid, lst))

    if not detail_targets:
        logger.info(f"  [Stage 2] 상세 조회 대상 없음 (모두 최신)")
        return result

    logger.info(f"  [Stage 2] 상세 조회 대상: {len(detail_targets)}개")

    for i, (pid, lst) in enumerate(detail_targets, 1):
        try:
            detail_data = _fetch_product_detail(client, pid)

            # 파싱된 필드 업데이트
            parsed = _parse_detail_fields(detail_data)
            lst.brand = parsed["brand"]
            lst.display_category_code = parsed["display_category_code"]
            lst.delivery_charge_type = parsed["delivery_charge_type"]
            lst.supply_price = parsed["supply_price"]
            lst.delivery_charge = parsed["delivery_charge"]
            lst.free_ship_over_amount = parsed["free_ship_over_amount"]
            lst.return_charge = parsed["return_charge"]

            # ISBN (attributes에서 추출한 것을 기존 isbn에 병합)
            if parsed["isbn"] and not lst.isbn:
                lst.isbn = parsed["isbn"]

            # 가격 업데이트
            if parsed["original_price"] and parsed["original_price"] > 0:
                lst.original_price = parsed["original_price"]
            if parsed["sale_price"] and parsed["sale_price"] > 0:
                lst.sale_price = parsed["sale_price"]

            # onSale 상태 조회 (vendor_item_id가 있고, deleted가 아닌 경우만)
            vid = lst.vendor_item_id
            if vid and lst.coupang_status not in ("deleted", "rejected"):
                try:
                    inv_resp = client.get_item_inventory(int(vid))
                    inv_data = inv_resp.get("data", inv_resp) if isinstance(inv_resp, dict) else {}
                    on_sale = inv_data.get("onSale")
                    if on_sale is not None:  # 명시적으로 있을 때만 상태 변경
                        lst.coupang_status = "active" if on_sale else "paused"
                    # 재고도 업데이트
                    stock = inv_data.get("amountInStock")
                    if stock is not None:
                        lst.stock_quantity = stock
                except Exception:
                    pass  # 실패해도 상세 동기화는 계속

            # raw_json + 파싱 필드 저장
            lst.raw_json = json.dumps(detail_data, ensure_ascii=False)
            lst.detail_synced_at = now

            # search_tags, images, purchase_options, search_options
            items = detail_data.get("items", [])
            if items:
                first_item = items[0]
                tags = first_item.get("searchTags", [])
                lst.search_tags = ", ".join(str(t) for t in tags) if tags else ""

                img_list = []
                for img in first_item.get("images", []):
                    url = img.get("cdnPath", img.get("vendorPath", ""))
                    if url:
                        img_list.append(url)
                lst.images = img_list

                purchase_opts = []
                search_opts = []
                for attr in first_item.get("attributes", []):
                    entry = {"type": attr.get("attributeTypeName", ""), "value": attr.get("attributeValueName", "")}
                    if attr.get("exposed") == "EXPOSED":
                        purchase_opts.append(entry)
                    else:
                        search_opts.append(entry)
                lst.purchase_options = purchase_opts
                lst.search_options = search_opts

            result["detail_synced"] += 1

            # 50건마다 중간 커밋 + 진행 로그
            if i % 50 == 0:
                _safe_commit(db)
                logger.info(f"    상세 진행: {i}/{len(detail_targets)} ({result['detail_synced']}성공, {result['detail_error']}실패)")

        except CoupangWingError as e:
            result["detail_error"] += 1
            logger.warning(f"    상세 조회 실패 [{pid}]: {e}")
        except Exception as e:
            result["detail_error"] += 1
            logger.warning(f"    상세 조회 오류 [{pid}]: {type(e).__name__}: {e}")

    # 최종 커밋
    _safe_commit(db)

    logger.info(f"  [Stage 2] 완료: 성공 {result['detail_synced']}개, 실패 {result['detail_error']}개, 스킵 {len(all_listings) - len(detail_targets)}개")

    return result


def _backfill_price_fields(db):
    """기존 raw_json에서 가격 필드 백필 (supply_price 등이 NULL인 레코드만)"""
    listings = db.query(Listing).filter(
        Listing.raw_json.isnot(None),
        Listing.supply_price.is_(None),
    ).all()

    if not listings:
        return 0

    count = 0
    for lst in listings:
        try:
            data = json.loads(lst.raw_json)
            parsed = _parse_detail_fields(data)
            lst.supply_price = parsed["supply_price"]
            lst.delivery_charge = parsed["delivery_charge"]
            lst.free_ship_over_amount = parsed["free_ship_over_amount"]
            lst.return_charge = parsed["return_charge"]
            count += 1
        except Exception:
            continue

    if count:
        _safe_commit(db)
        logger.info(f"  가격 필드 백필: {count}개 업데이트")

    return count


def populate_wing_credentials(db):
    """
    .env에서 WING API 크레덴셜을 읽어 Account 레코드에 저장
    (이미 값이 있으면 건너뜀)
    """
    updated = 0

    for account_name, env_prefix in WING_ACCOUNT_ENV_MAP.items():
        vendor_id = os.getenv(f"{env_prefix}_VENDOR_ID")
        access_key = os.getenv(f"{env_prefix}_ACCESS_KEY")
        secret_key = os.getenv(f"{env_prefix}_SECRET_KEY")

        if not all([vendor_id, access_key, secret_key]):
            continue

        account = db.query(Account).filter(
            Account.account_name == account_name
        ).first()

        if not account:
            logger.warning(f"계정 없음: {account_name} (DB에 먼저 등록 필요)")
            continue

        if account.has_wing_api:
            continue

        account.vendor_id = vendor_id
        account.wing_access_key = access_key
        account.wing_secret_key = secret_key
        account.wing_api_enabled = True
        updated += 1

    db.commit()
    if updated:
        logger.info(f"WING API 크레덴셜 {updated}개 계정에 등록")
    return updated


def run_sync(account_names=None, max_pages=0, dry_run=False,
             quick=False, force=False, stale_hours=24):
    """전체 동기화 실행"""
    obs = ObsidianLogger()

    mode_str = "quick(목록만)" if quick else ("force(전체 상세)" if force else f"증분(stale>{stale_hours}h)")

    print("\n" + "=" * 60)
    print("  쿠팡 WING API 상품 동기화 (2단계)")
    print(f"  모드: {mode_str}")
    print(f"  시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    init_db()
    db = SessionLocal()

    try:
        # WING API 크레덴셜 채우기
        populate_wing_credentials(db)

        # 기존 raw_json에서 새 가격 필드 백필
        _backfill_price_fields(db)

        # 동기화 대상 계정 조회
        query = db.query(Account).filter(
            Account.is_active == True,
            Account.wing_api_enabled == True,
        )

        if account_names:
            query = query.filter(Account.account_name.in_(account_names))

        accounts = query.all()

        if not accounts:
            print("\n  WING API가 활성화된 계정이 없습니다.")
            print("  .env에 COUPANG_*_VENDOR_ID, _ACCESS_KEY, _SECRET_KEY를 확인하세요.")
            return

        print(f"\n  동기화 대상: {len(accounts)}개 계정")
        for acc in accounts:
            print(f"    - {acc.account_name} (vendor_id={acc.vendor_id})")

        # 계정별 동기화
        total_result = {
            "total": 0, "new": 0, "updated": 0,
            "isbn_found": 0, "isbn_missing": 0,
            "detail_synced": 0, "detail_skipped": 0, "detail_error": 0,
        }

        for account in accounts:
            result = sync_account_products(
                db, account, max_pages=max_pages, dry_run=dry_run,
                quick=quick, force=force, stale_hours=stale_hours,
            )

            for key in total_result:
                total_result[key] += result[key]

        # 결과 요약
        total_listings = db.query(Listing).count()
        detail_filled = db.query(Listing).filter(Listing.raw_json.isnot(None)).count()

        print("\n" + "=" * 60)
        print("  동기화 결과")
        print("=" * 60)
        print(f"  [Stage 1] 총 조회: {total_result['total']}개")
        print(f"  [Stage 1] 신규: {total_result['new']}개 / 업데이트: {total_result['updated']}개")
        print(f"  [Stage 1] ISBN 성공: {total_result['isbn_found']}개 / 실패: {total_result['isbn_missing']}개")
        if not quick:
            print(f"  [Stage 2] 상세 성공: {total_result['detail_synced']}개 / 실패: {total_result['detail_error']}개")
        print(f"  DB 총 Listings: {total_listings}개 (상세 보유: {detail_filled}개)")
        print("=" * 60)

        # Obsidian 로그
        detail_line = ""
        if not quick:
            detail_line = f"\n| 상세 성공 | {total_result['detail_synced']}개 |\n| 상세 실패 | {total_result['detail_error']}개 |"

        obs.log_to_daily(f"""**WING API 상품 동기화 완료** ({mode_str})

| 항목 | 수량 |
|------|------|
| 조회 | {total_result['total']}개 |
| 신규 | {total_result['new']}개 |
| 업데이트 | {total_result['updated']}개 |
| ISBN 성공 | {total_result['isbn_found']}개 |
| ISBN 실패 | {total_result['isbn_missing']}개 |{detail_line}
| DB Listings | {total_listings}개 |
| 상세 보유 | {detail_filled}개 |""", "WING API 동기화")

    except Exception as e:
        logger.error(f"동기화 오류: {e}", exc_info=True)
        obs.log_to_daily(f"**동기화 오류:** `{type(e).__name__}: {e}`", "WING API 동기화 실패")
        db.rollback()
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="쿠팡 WING API 상품 동기화 (2단계)")
    parser.add_argument(
        "--account", nargs="+",
        help="특정 계정만 동기화 (예: --account 007-bm 007-book)"
    )
    parser.add_argument(
        "--max-pages", type=int, default=0,
        help="계정당 최대 페이지 수 (기본: 0=무제한)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="DB 저장 없이 조회만"
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="목록만 동기화 (Stage 1만, 상세 조회 생략)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="모든 상품 상세 강제 재조회"
    )
    parser.add_argument(
        "--stale-hours", type=int, default=24,
        help="상세 재조회 기준 시간 (기본: 24시간)"
    )

    args = parser.parse_args()

    run_sync(
        account_names=args.account,
        max_pages=args.max_pages,
        dry_run=args.dry_run,
        quick=args.quick,
        force=args.force,
        stale_hours=args.stale_hours,
    )


if __name__ == "__main__":
    main()
