"""노출 모니터링 — 셀러 상품의 쿠팡 검색 노출 확인"""

import re
from difflib import SequenceMatcher

from core.config import AnalysisConfig
from core.database import CoupangDB
from core.models import ExposureLog
from analysis.collector import collect_keyword


def cmd_exposure(args, config: AnalysisConfig):
    """노출 모니터링 명령어 핸들러"""
    if not hasattr(args, "exposure_action") or not args.exposure_action:
        print("\n사용법: python main.py exposure {check|batch|report} ...")
        return

    if args.exposure_action == "check":
        check_exposure(args.account, args.keyword, args.pages, config)
    elif args.exposure_action == "batch":
        batch_check(args.account, args.top, config)
    elif args.exposure_action == "report":
        exposure_report(args.account, config)


def check_exposure(account_code: str, keyword: str, pages: int = 3,
                   config: AnalysisConfig = None) -> list[dict]:
    """단일 키워드로 계정 상품 노출 체크.

    1) 키워드로 쿠팡 검색 → DB에 수집
    2) 수집된 결과와 계정 재고 상품을 3단계 매칭
    3) 매칭 결과를 exposure_logs에 저장

    Returns: 매칭된 상품 리스트
    """
    config = config or AnalysisConfig()
    db = CoupangDB(config)

    try:
        # 계정 확인
        account = db.get_account_by_code(account_code)
        if not account:
            print(f"\n계정을 찾을 수 없습니다: {account_code}")
            return []

        # 재고 상품 로드
        inv_products = db.list_inventory(account.id, limit=10000)
        if not inv_products:
            print(f"\n{account_code}에 등록된 상품이 없습니다.")
            return []

        print(f"\n[노출 체크] 계정: {account_code}, 키워드: '{keyword}', 페이지: {pages}")
        print(f"  재고 상품: {len(inv_products)}개")

        # 쿠팡 검색 수집 (기존 collect_keyword 재사용)
        print(f"\n  쿠팡 검색 수집 중...")
        count = collect_keyword(keyword, max_pages=pages, config=config)
        if count == 0:
            print("  검색 결과가 없습니다.")
            return []

        # 수집된 검색 결과 로드
        search_df = db.get_analysis_dataframe(keyword)
        if search_df.empty:
            print("  수집된 데이터를 로드할 수 없습니다.")
            return []

        print(f"  검색 결과: {len(search_df)}개 상품")

        # 3단계 매칭
        matches = _match_products(inv_products, search_df)

        # 결과 저장 및 출력
        print(f"\n  [매칭 결과] {len(matches)}개 발견")
        if matches:
            print(f"\n  {'순위':>5} {'매칭방식':<15} {'셀러상품ID':<15} {'상품명'}")
            print(f"  {'-' * 65}")

        for m in matches:
            log = ExposureLog(
                inventory_product_id=m["inv_id"],
                account_id=account.id,
                keyword=keyword,
                found=True,
                exposure_rank=m["rank"],
                page=(m["rank"] - 1) // 36 + 1,
                matched_by=m["matched_by"],
            )
            db.insert_exposure_log(log)
            name = m["product_name"][:40] if m["product_name"] else ""
            print(f"  {m['rank']:>5} {m['matched_by']:<15} {m['seller_product_id']:<15} {name}")

        # 미노출 상품도 로그
        matched_inv_ids = {m["inv_id"] for m in matches}
        for inv_p in inv_products:
            if inv_p.id not in matched_inv_ids:
                log = ExposureLog(
                    inventory_product_id=inv_p.id,
                    account_id=account.id,
                    keyword=keyword,
                    found=False,
                )
                db.insert_exposure_log(log)

        if not matches:
            print("  이 키워드로 노출되는 상품이 없습니다.")

        return matches

    finally:
        db.close()


def _match_products(inv_products, search_df) -> list[dict]:
    """3단계 매칭: product_id → vendor_item_id → 상품명 유사도"""
    matches = []
    matched_inv_ids = set()

    # 인벤토리 인덱스 구축
    inv_by_wing_pid = {}
    inv_by_seller_pid = {}
    for p in inv_products:
        if p.wing_product_id:
            inv_by_wing_pid[str(p.wing_product_id)] = p
        if p.seller_product_id:
            inv_by_seller_pid[str(p.seller_product_id)] = p

    for _, row in search_df.iterrows():
        search_pid = str(row.get("product_id", ""))
        search_vid = str(row.get("vendor_item_id", ""))
        search_name = str(row.get("product_name", ""))
        rank = int(row.get("exposure_order", 0))

        # 1단계: product_id 매칭
        if search_pid and search_pid in inv_by_wing_pid:
            inv_p = inv_by_wing_pid[search_pid]
            if inv_p.id not in matched_inv_ids:
                matches.append({
                    "inv_id": inv_p.id,
                    "seller_product_id": inv_p.seller_product_id,
                    "product_name": inv_p.product_name,
                    "rank": rank,
                    "matched_by": "product_id",
                })
                matched_inv_ids.add(inv_p.id)
                continue

        # 2단계: vendor_item_id 매칭
        if search_vid and search_vid in inv_by_seller_pid:
            inv_p = inv_by_seller_pid[search_vid]
            if inv_p.id not in matched_inv_ids:
                matches.append({
                    "inv_id": inv_p.id,
                    "seller_product_id": inv_p.seller_product_id,
                    "product_name": inv_p.product_name,
                    "rank": rank,
                    "matched_by": "vendor_item_id",
                })
                matched_inv_ids.add(inv_p.id)
                continue

        # 3단계: 상품명 유사도 (0.85 이상)
        if search_name:
            best_score = 0
            best_inv = None
            for inv_p in inv_products:
                if inv_p.id in matched_inv_ids:
                    continue
                if not inv_p.product_name:
                    continue
                score = SequenceMatcher(None, search_name, inv_p.product_name).ratio()
                if score > best_score:
                    best_score = score
                    best_inv = inv_p

            if best_score >= 0.85 and best_inv:
                matches.append({
                    "inv_id": best_inv.id,
                    "seller_product_id": best_inv.seller_product_id,
                    "product_name": best_inv.product_name,
                    "rank": rank,
                    "matched_by": f"name({best_score:.0%})",
                })
                matched_inv_ids.add(best_inv.id)

    matches.sort(key=lambda x: x["rank"])
    return matches


def batch_check(account_code: str, top_n: int = 50,
                config: AnalysisConfig = None):
    """상위 N개 상품의 상품명을 키워드로 검색하여 노출 체크"""
    config = config or AnalysisConfig()
    db = CoupangDB(config)

    try:
        account = db.get_account_by_code(account_code)
        if not account:
            print(f"\n계정을 찾을 수 없습니다: {account_code}")
            return

        products = db.list_inventory(account.id, status="판매중", limit=top_n)
        if not products:
            print(f"\n{account_code}에 판매중 상품이 없습니다.")
            return

        print(f"\n[배치 노출 체크] 계정: {account_code}, 대상: {len(products)}개")
        print("  각 상품의 핵심 키워드로 검색하여 노출 여부를 확인합니다.\n")

        found_count = 0
        for i, p in enumerate(products, 1):
            # 상품명에서 핵심 키워드 추출 (첫 2~3어절)
            keyword = _extract_keyword(p.product_name)
            if not keyword:
                continue

            print(f"  [{i}/{len(products)}] '{keyword}' <- {p.product_name[:30]}...")

            # 간단 체크 (1페이지만)
            results = check_exposure(account_code, keyword, pages=1, config=config)
            if results:
                found_count += 1

        print(f"\n  [결과] {found_count}/{len(products)} 노출 확인")

    finally:
        db.close()


def _extract_keyword(product_name: str) -> str:
    """상품명에서 핵심 키워드 추출 (처음 2~3어절)"""
    if not product_name:
        return ""
    # 괄호, 특수문자 제거
    cleaned = re.sub(r'[\[\](){}【】\-_/|]', ' ', product_name)
    words = cleaned.split()
    if len(words) >= 2:
        return " ".join(words[:2])
    return words[0] if words else ""


def exposure_report(account_code: str, config: AnalysisConfig = None):
    """계정의 노출 리포트 출력"""
    config = config or AnalysisConfig()
    db = CoupangDB(config)

    try:
        account = db.get_account_by_code(account_code)
        if not account:
            print(f"\n계정을 찾을 수 없습니다: {account_code}")
            return

        logs = db.get_exposure_logs_by_account(account.id, limit=500)
        if not logs:
            print(f"\n{account_code}의 노출 기록이 없습니다.")
            print("  python main.py exposure check -a {account_code} -k '키워드' 로 체크하세요.")
            return

        SEP = "=" * 70
        print(f"\n{SEP}")
        print(f"  노출 리포트: {account_code} ({account.account_name})")
        print(f"{SEP}")

        # 요약
        summary = db.get_exposure_summary(account.id)
        total = summary["total_checks"]
        found = summary["found"]
        rate = found / total * 100 if total > 0 else 0

        print(f"\n  총 체크: {total}")
        print(f"  노출됨: {found} ({rate:.1f}%)")
        print(f"  미노출: {summary['not_found']}")

        # 키워드별 통계
        keyword_stats = {}
        for log in logs:
            kw = log.keyword
            if kw not in keyword_stats:
                keyword_stats[kw] = {"found": 0, "not_found": 0, "best_rank": None}
            if log.found:
                keyword_stats[kw]["found"] += 1
                if log.exposure_rank:
                    curr_best = keyword_stats[kw]["best_rank"]
                    if curr_best is None or log.exposure_rank < curr_best:
                        keyword_stats[kw]["best_rank"] = log.exposure_rank
            else:
                keyword_stats[kw]["not_found"] += 1

        if keyword_stats:
            print(f"\n  [키워드별 노출]")
            print(f"  {'키워드':<25} {'노출':>5} {'미노출':>5} {'최고순위':>8}")
            print(f"  {'-' * 50}")
            for kw, stats in sorted(keyword_stats.items()):
                best = str(stats["best_rank"]) if stats["best_rank"] else "-"
                print(f"  {kw:<25} {stats['found']:>5} {stats['not_found']:>5} {best:>8}")

        # 최근 노출된 상품
        found_logs = [l for l in logs if l.found]
        if found_logs:
            print(f"\n  [최근 노출된 상품] (상위 10개)")
            print(f"  {'순위':>5} {'키워드':<20} {'매칭방식':<15} {'체크일'}")
            print(f"  {'-' * 55}")
            for log in found_logs[:10]:
                rank = str(log.exposure_rank) if log.exposure_rank else "-"
                print(f"  {rank:>5} {log.keyword:<20} {log.matched_by:<15} {log.checked_at[:10]}")

        print(f"\n{SEP}")

    finally:
        db.close()
