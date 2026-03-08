"""
쿠팡 상품에 ISBN 일괄 등록
===========================
DB에 ISBN이 있지만 쿠팡 상품 attributes에 ISBN이 없는 상품을 찾아
update_product API로 ISBN을 추가하고 승인요청까지 자동 처리.

사용법:
    python scripts/fill_isbn_to_coupang.py                    # 전체 계정
    python scripts/fill_isbn_to_coupang.py --account 007-bm   # 특정 계정
    python scripts/fill_isbn_to_coupang.py --dry-run           # 조회만 (수정 안 함)
    python scripts/fill_isbn_to_coupang.py --limit 10          # 최대 10개만
"""
import sys
import os
import json
import time
import argparse
import logging
import re
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 설정
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from core.database import SessionLocal, init_db, engine
from core.models.account import Account
from core.models.listing import Listing
from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.constants import WING_ACCOUNT_ENV_MAP

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def _create_wing_client(account: Account) -> CoupangWingClient:
    """Account 모델에서 WING API 클라이언트 생성"""
    return CoupangWingClient(
        vendor_id=account.vendor_id,
        access_key=account.wing_access_key,
        secret_key=account.wing_secret_key,
    )


def _extract_isbn_from_attributes(attributes: list) -> str:
    """attributes 배열에서 ISBN 값 추출"""
    for attr in attributes:
        if attr.get("attributeTypeName") == "ISBN":
            val = attr.get("attributeValueName", "")
            cleaned = re.sub(r'[^0-9]', '', val)
            if len(cleaned) == 13 and cleaned.startswith(("978", "979")):
                return cleaned
    return ""


def _find_isbn_attribute_index(attributes: list) -> int:
    """attributes 배열에서 ISBN 항목의 인덱스 반환 (-1이면 없음)"""
    for i, attr in enumerate(attributes):
        if attr.get("attributeTypeName") == "ISBN":
            return i
    return -1


def fill_isbn_for_account(db, account: Account, dry_run=False, limit=0):
    """계정의 ISBN 누락 상품을 찾아 쿠팡에 업데이트"""
    try:
        client = _create_wing_client(account)
    except Exception as e:
        logger.error(f"{account.account_name}: WING 클라이언트 생성 실패: {e}")
        return {"total": 0, "updated": 0, "skipped": 0, "error": 0}

    # DB에 ISBN이 있고, raw_json이 있는 listings 조회
    query = db.query(Listing).filter(
        Listing.account_id == account.id,
        Listing.isbn.isnot(None),
        Listing.isbn != '',
        Listing.raw_json.isnot(None),
    )
    listings = query.all()

    result = {"total": 0, "updated": 0, "skipped": 0, "error": 0}
    targets = []

    for lst in listings:
        # 단일 ISBN만 처리 (세트물 제외)
        isbn = lst.isbn.strip()
        if ',' in isbn:
            continue
        if not (len(isbn) == 13 and isbn.startswith(("978", "979"))):
            continue

        # raw_json에서 현재 쿠팡 상품의 ISBN 확인
        try:
            prod_data = json.loads(lst.raw_json)
        except Exception:
            continue

        items = prod_data.get("items", [])
        if not items:
            continue

        first_item = items[0]
        attrs = first_item.get("attributes", [])
        existing_isbn = _extract_isbn_from_attributes(attrs)

        if existing_isbn == isbn:
            # 이미 일치 → 스킵
            continue

        targets.append({
            "listing": lst,
            "isbn": isbn,
            "spid": int(lst.coupang_product_id),
            "prod_data": prod_data,
            "existing_isbn": existing_isbn,
        })

    result["total"] = len(targets)
    logger.info(f"\n{'='*50}")
    logger.info(f"{account.account_name}: ISBN 누락/불일치 {len(targets)}건 발견")
    logger.info(f"{'='*50}")

    if limit > 0:
        targets = targets[:limit]
        logger.info(f"  --limit {limit}: 처음 {len(targets)}건만 처리")

    for i, t in enumerate(targets, 1):
        spid = t["spid"]
        isbn = t["isbn"]
        existing = t["existing_isbn"]

        action = "추가" if not existing else f"수정({existing}→{isbn})"
        logger.info(f"  [{i}/{len(targets)}] SPID={spid} ISBN {action}")

        if dry_run:
            result["skipped"] += 1
            continue

        try:
            # 1) 최신 상품 데이터 가져오기 (raw_json이 stale할 수 있으므로)
            prod_resp = client.get_product(spid)
            prod_data = prod_resp.get("data", prod_resp)

            items = prod_data.get("items", [])
            if not items:
                logger.warning(f"    items 없음, 스킵")
                result["error"] += 1
                continue

            # 2) 모든 item의 attributes에 ISBN 추가/수정
            for item in items:
                attrs = item.get("attributes", [])
                idx = _find_isbn_attribute_index(attrs)

                isbn_attr = {
                    "attributeTypeName": "ISBN",
                    "attributeValueName": isbn,
                    "exposed": "NONE",
                    "editable": "Y",
                }

                if idx >= 0:
                    attrs[idx]["attributeValueName"] = isbn
                else:
                    attrs.append(isbn_attr)

                item["attributes"] = attrs

            # 3) update_product 호출 (requested=true로 승인요청 포함)
            prod_data["items"] = items
            prod_data["requested"] = True

            client.update_product(spid, prod_data)
            result["updated"] += 1
            logger.info(f"    완료 (승인요청 포함)")

            # Rate limit 방지
            time.sleep(0.3)

        except CoupangWingError as e:
            result["error"] += 1
            logger.error(f"    API 오류: {e.message}")
            if "RATE" in str(e.code).upper() or e.status_code == 429:
                logger.info("    Rate limit, 2초 대기")
                time.sleep(2)
        except Exception as e:
            result["error"] += 1
            logger.error(f"    오류: {type(e).__name__}: {e}")

    return result


def run(account_names=None, dry_run=False, limit=0):
    """전체 실행"""
    print("\n" + "=" * 60)
    print("  쿠팡 상품 ISBN 일괄 등록")
    print(f"  모드: {'DRY-RUN (조회만)' if dry_run else '실행'}")
    if limit:
        print(f"  제한: 계정당 최대 {limit}건")
    print(f"  시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    init_db()
    db = SessionLocal()

    try:
        query = db.query(Account).filter(
            Account.is_active == True,
            Account.wing_api_enabled == True,
        )
        if account_names:
            query = query.filter(Account.account_name.in_(account_names))

        accounts = query.all()
        if not accounts:
            print("\n  활성 계정이 없습니다.")
            return

        print(f"\n  대상 계정: {len(accounts)}개")
        for acc in accounts:
            print(f"    - {acc.account_name}")

        total = {"total": 0, "updated": 0, "skipped": 0, "error": 0}
        for account in accounts:
            r = fill_isbn_for_account(db, account, dry_run=dry_run, limit=limit)
            for k in total:
                total[k] += r[k]

        print("\n" + "=" * 60)
        print("  결과")
        print("=" * 60)
        print(f"  ISBN 누락/불일치: {total['total']}건")
        print(f"  업데이트 성공: {total['updated']}건")
        print(f"  스킵 (dry-run): {total['skipped']}건")
        print(f"  실패: {total['error']}건")
        print("=" * 60)

    except Exception as e:
        logger.error(f"오류: {e}", exc_info=True)
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="쿠팡 상품 ISBN 일괄 등록")
    parser.add_argument("--account", nargs="+", help="특정 계정만")
    parser.add_argument("--dry-run", action="store_true", help="조회만 (수정 안 함)")
    parser.add_argument("--limit", type=int, default=0, help="계정당 최대 처리 건수")

    args = parser.parse_args()
    run(account_names=args.account, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
