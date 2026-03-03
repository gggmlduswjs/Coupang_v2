"""쿠팡 검색 알고리즘 분석 도구 - CLI 진입점"""

import argparse
import os
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from core.config import AnalysisConfig
from analysis.collector import import_html_file, collect_keyword, enrich_products
from analysis.analyzer import run_full_analysis, print_analysis_report
from analysis.reporter import generate_report
from operations.backup import create_backup, list_backups, restore_backup
from operations.inventory import import_wing_excel
from core.database import SessionLocal
from core.models import Account, InventoryProduct


def cmd_collect(args, config):
    """Playwright 자동 수집"""
    print(f"\n[수집] 키워드: '{args.keyword}', 페이지: {args.pages}")
    count = collect_keyword(args.keyword, max_pages=args.pages, config=config)
    if count > 0 and args.enrich:
        print(f"\n[보강] 상위 {config.enrich_top_n}개 상세페이지 수집...")
        enrich_products(args.keyword, top_n=config.enrich_top_n, config=config)
    print(f"\n완료: {count}개 상품 수집")


def cmd_import(args, config):
    """HTML 파일 임포트"""
    print(f"\n[임포트] 파일: {args.file}")
    keyword = args.keyword or ""
    count = import_html_file(args.file, keyword=keyword, config=config)
    print(f"\n완료: {count}개 상품 임포트")


def cmd_analyze(args, config):
    """분석 실행"""
    print(f"\n[분석] 키워드: '{args.keyword}'")
    analysis = run_full_analysis(args.keyword, config=config)
    if analysis:
        print_analysis_report(analysis)
    return analysis


def cmd_report(args, config):
    """Excel 리포트 생성"""
    print(f"\n[리포트] 키워드: '{args.keyword}'")
    analysis = run_full_analysis(args.keyword, config=config)
    if analysis:
        filepath = generate_report(analysis, config=config)
        if filepath:
            print(f"\n리포트 생성 완료: {filepath}")


def cmd_strategy(args, config):
    """전략 제안"""
    print(f"\n[전략] 키워드: '{args.keyword}'")
    analysis = run_full_analysis(args.keyword, config=config)
    if not analysis:
        return

    strat = analysis.get("strategy", {})
    SEP = "=" * 65

    print(f"\n{SEP}")
    print(f"  전략 제안: '{args.keyword}'")
    print(f"{SEP}")

    if strat.get("pricing"):
        print(f"\n  [가격 전략]")
        for k, v in strat["pricing"].items():
            print(f"    {k}: {v}")

    if strat.get("keyword"):
        print(f"\n  [키워드 전략]")
        for k, v in strat["keyword"].items():
            if isinstance(v, list):
                v = ", ".join(v)
            if v:
                print(f"    {k}: {v}")

    if strat.get("review"):
        print(f"\n  [리뷰 전략]")
        for k, v in strat["review"].items():
            print(f"    {k}: {v}")

    if strat.get("delivery"):
        print(f"\n  [배송 전략]")
        for k, v in strat["delivery"].items():
            print(f"    {k}: {v}")

    if strat.get("actions"):
        print(f"\n  [액션 체크리스트]")
        for a in strat["actions"]:
            print(f"    {a['우선순위']}. {a['항목']}")
            print(f"       {a['설명']}")

    ci = strat.get("competition_index", 0)
    level = "매우 높음" if ci >= 80 else "높음" if ci >= 60 else "보통" if ci >= 40 else "낮음"
    print(f"\n  경쟁 강도: {ci}/100 ({level})")
    print(f"{SEP}")


def cmd_full(args, config):
    """전체 파이프라인: 수집 → 분석 → 리포트 → 전략"""
    print(f"\n[전체 실행] 키워드: '{args.keyword}'")

    # 수집
    if args.file:
        print(f"\n{'='*40} 1. 임포트 {'='*40}")
        import_html_file(args.file, keyword=args.keyword, config=config)
    else:
        print(f"\n{'='*40} 1. 수집 {'='*40}")
        collect_keyword(args.keyword, max_pages=args.pages, config=config)

    # 분석
    print(f"\n{'='*40} 2. 분석 {'='*40}")
    analysis = run_full_analysis(args.keyword, config=config)
    if not analysis:
        print("분석 실패. 데이터를 확인해주세요.")
        return

    print_analysis_report(analysis)

    # 리포트
    print(f"\n{'='*40} 3. 리포트 {'='*40}")
    filepath = generate_report(analysis, config=config)

    # 전략
    print(f"\n{'='*40} 4. 전략 {'='*40}")
    strat = analysis.get("strategy", {})
    if strat.get("actions"):
        for a in strat["actions"]:
            print(f"  {a['우선순위']}. {a['항목']}: {a['설명']}")

    print(f"\n{'='*65}")
    print(f"  전체 파이프라인 완료!")
    if filepath:
        print(f"  리포트: {filepath}")
    print(f"{'='*65}")


def cmd_account(args, config):
    """계정 관리"""
    db = SessionLocal()
    try:
        if args.account_action == "list":
            accounts = db.query(Account).order_by(Account.account_code).all()
            if not accounts:
                print("\n등록된 계정이 없습니다.")
                return
            print(f"\n{'코드':<12} {'이름':<15} {'상태':<10} {'벤더ID':<12} {'메모'}")
            print("-" * 65)
            for a in accounts:
                print(f"{a.account_code:<12} {a.account_name:<15} {a.status:<10} {a.vendor_id or '':<12} {a.memo or ''}")

        elif args.account_action == "add":
            existing = db.query(Account).filter(Account.account_code == args.code).first()
            if existing:
                print(f"\n이미 존재하는 계정: {args.code}")
                return
            account = Account(
                account_code=args.code,
                account_name=args.name or args.code,
                status=args.status or "활성",
                vendor_id=args.vendor_id or "",
                memo=args.memo or "",
            )
            db.add(account)
            db.commit()
            print(f"\n계정 추가 완료: {args.code} ({account.account_name})")

        elif args.account_action == "status":
            account = db.query(Account).filter(Account.account_code == args.code).first()
            if not account:
                print(f"\n계정을 찾을 수 없습니다: {args.code}")
                return
            old_status = account.status
            account.status = args.new_status
            db.commit()
            print(f"\n{args.code} 상태 변경: {old_status} → {args.new_status}")
    finally:
        db.close()


def cmd_inv(args, config):
    """재고 관리"""
    if args.inv_action == "import":
        import os
        if not os.path.exists(args.file):
            print(f"\n파일을 찾을 수 없습니다: {args.file}")
            return
        # 임포트 전 자동 백업
        print("\n[자동 백업] 임포트 전 DB 백업...")
        create_backup(config)
        print(f"\n[임포트] 계정: {args.account}, 파일: {args.file}")
        import_wing_excel(args.file, args.account, config)

    elif args.inv_action == "list":
        from sqlalchemy import func
        db = SessionLocal()
        try:
            account = db.query(Account).filter(Account.account_code == args.account).first()
            if not account:
                print(f"\n계정을 찾을 수 없습니다: {args.account}")
                return
            query = db.query(InventoryProduct).filter(InventoryProduct.account_id == account.id)
            if args.status:
                query = query.filter(InventoryProduct.status == args.status)
            total = query.count()
            products = query.limit(args.limit).all()
            status_counts = dict(
                db.query(InventoryProduct.status, func.count(InventoryProduct.id))
                .filter(InventoryProduct.account_id == account.id)
                .group_by(InventoryProduct.status)
                .all()
            )

            print(f"\n[{args.account}] 재고 목록 (총 {total}개)")
            if status_counts:
                parts = [f"{s}: {c}" for s, c in status_counts.items()]
                print(f"  상태별: {', '.join(parts)}")
            print(f"\n{'ID':<8} {'셀러상품ID':<15} {'상품명':<40} {'판매가':>10} {'상태':<8}")
            print("-" * 85)
            for p in products:
                name = p.product_name[:38] if p.product_name else ""
                price = f"{p.sale_price:,}" if p.sale_price else "-"
                print(f"{p.id:<8} {p.seller_product_id:<15} {name:<40} {price:>10} {p.status:<8}")
            if len(products) >= args.limit:
                print(f"\n  ... 최대 {args.limit}개 표시 (--limit으로 조절)")
        finally:
            db.close()

    elif args.inv_action == "search":
        db = SessionLocal()
        try:
            results = db.query(InventoryProduct).filter(
                InventoryProduct.product_name.like(f"%{args.query}%")
            ).limit(100).all()
            if not results:
                print(f"\n'{args.query}' 검색 결과 없음")
                return
            print(f"\n검색 결과: {len(results)}개")
            print(f"\n{'ID':<8} {'계정ID':<8} {'셀러상품ID':<15} {'상품명':<40} {'판매가':>10}")
            print("-" * 85)
            for p in results:
                name = p.product_name[:38] if p.product_name else ""
                price = f"{p.sale_price:,}" if p.sale_price else "-"
                print(f"{p.id:<8} {p.account_id:<8} {p.seller_product_id:<15} {name:<40} {price:>10}")
        finally:
            db.close()


def cmd_backup(args, config):
    """DB 백업/복원"""
    if args.list_backups:
        backups = list_backups(config)
        if not backups:
            print("\n백업이 없습니다.")
            return
        print(f"\n{'파일명':<35} {'크기(MB)':>10} {'생성일시'}")
        print("-" * 70)
        for b in backups:
            print(f"{b['filename']:<35} {b['size_mb']:>10} {b['modified'][:19]}")
    elif args.restore:
        print(f"\n[복원] 대상: {args.restore}")
        restore_backup(args.restore, config)
    else:
        print("\n[백업] DB 백업 생성 중...")
        path = create_backup(config)
        if path:
            print(f"  완료: {path}")


def cmd_xray(args, config):
    """쿠팡 시스템 역공학 분석"""
    from analysis.reverse_engineer import run_reverse_engineering

    print(f"\n[역공학] 키워드: '{args.keyword}'")
    results = run_reverse_engineering(args.keyword, config=config)
    if not results:
        return

    # JSON 저장 옵션
    if args.output:
        import json
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n  결과 저장: {args.output}")


def cmd_ad(args, config):
    """광고 추천 리포트"""
    from operations.ad_report import generate_ad_report

    src = getattr(args, "source", "db")
    label = "API 실시간" if src == "api" else "DB 재고"
    print(f"\n[광고추천] 계정: {args.account} ({label})")
    filepath = generate_ad_report(args.account, config=config, output=args.output, source=src)
    if filepath:
        print(f"\n  리포트: {filepath}")


def cmd_upload(args, config):
    """업로드 엑셀 파일 관리 + API 상품 관리"""
    from operations.upload_excel import (
        find_upload_files, filter_products, update_filename_count,
        search_products, compare_accounts, get_stats,
        find_duplicates, validate_upload, fix_dates,
        fill_required_fields, optimize_tags, audit_seo, auto_fill,
    )

    base = config.upload_excel_dir
    folder = getattr(args, "dir", "") or ""
    action = args.upload_action

    if not action:
        print("\nupload 하위 명령을 지정하세요:")
        print("  Excel: list, filter, search, compare, stats, dup, validate, fix-dates, fill, auto-fill, optimize, audit")
        print("  API:   register, update-tags, status, delete, sync, gap")
        return

    if action == "list":
        files = find_upload_files(base, folder)
        if not files:
            print("\n파일이 없습니다.")
            return
        print(f"\n{'폴더':<35} {'계정':<12} {'상품수':>6}  파일명")
        print("-" * 100)
        total = 0
        for f in files:
            cnt = f["count"] if f["count"] >= 0 else "ERR"
            print(f"{f['folder']:<35} {f['account']:<12} {cnt:>6}  {f['filename']}")
            if isinstance(f["count"], int) and f["count"] > 0:
                total += f["count"]
        print(f"\n  총 {len(files)}개 파일, {total:,}개 상품")

    elif action == "filter":
        keywords = [k.strip() for k in args.query.split(",") if k.strip()]
        if not keywords:
            print("\n키워드를 지정하세요 (-q)")
            return
        files = find_upload_files(base, folder, count=False)
        total_deleted = 0
        for f in files:
            cnt, matched = filter_products(f["path"], keywords, dry_run=args.dry_run)
            if cnt > 0:
                mode = "[미리보기]" if args.dry_run else "[삭제]"
                print(f"\n  {mode} {f['filename']}: {cnt}건")
                for m in matched[:5]:
                    print(f"    Row {m['row']}: {m['name'][:60]} (키워드: {m['keyword']})")
                if cnt > 5:
                    print(f"    ... 외 {cnt - 5}건")
                if not args.dry_run:
                    new_path = update_filename_count(f["path"])
                    if new_path != f["path"]:
                        print(f"    → 파일명 변경: {os.path.basename(new_path)}")
                total_deleted += cnt
        if total_deleted == 0:
            print("\n  매칭되는 상품이 없습니다.")
        else:
            mode = "미리보기" if args.dry_run else "삭제"
            print(f"\n  총 {total_deleted}건 {mode} 완료")

    elif action == "search":
        results = search_products(base, args.query, folder)
        if not results:
            print(f"\n'{args.query}' 검색 결과 없음")
            return
        print(f"\n검색 결과: {len(results)}건")
        print(f"\n{'계정':<12} {'가격':>10} {'상품명':<50} 파일")
        print("-" * 100)
        for r in results[:50]:
            price = f"{r['price']:,}" if r.get("price") else "-"
            name = r["name"][:48]
            print(f"{r['account']:<12} {price:>10} {name:<50} {r['file']}")
        if len(results) > 50:
            print(f"\n  ... 외 {len(results) - 50}건 (총 {len(results)}건)")

    elif action == "compare":
        if not folder:
            print("\n폴더를 지정하세요 (-d)")
            return
        result = compare_accounts(base, folder, args.account_a, args.account_b)
        print(f"\n[비교] {args.account_a} vs {args.account_b} (폴더: {folder})")
        print(f"  {args.account_a}: {result['count_a']}개")
        print(f"  {args.account_b}: {result['count_b']}개")
        print(f"  공통: {len(result['common'])}개")
        if result["only_a"]:
            print(f"\n  [{args.account_a}에만 있는 상품] ({len(result['only_a'])}개)")
            for name in result["only_a"][:10]:
                print(f"    - {name[:70]}")
            if len(result["only_a"]) > 10:
                print(f"    ... 외 {len(result['only_a']) - 10}개")
        if result["only_b"]:
            print(f"\n  [{args.account_b}에만 있는 상품] ({len(result['only_b'])}개)")
            for name in result["only_b"][:10]:
                print(f"    - {name[:70]}")
            if len(result["only_b"]) > 10:
                print(f"    ... 외 {len(result['only_b']) - 10}개")

    elif action == "stats":
        stats = get_stats(base, folder)
        print(f"\n[통계] 총 {stats['total']:,}개 상품 ({stats['files']}개 파일)")
        if stats["by_folder"]:
            print(f"\n  [폴더별]")
            for k, v in stats["by_folder"].items():
                print(f"    {k}: {v:,}개")
        if stats["by_account"]:
            print(f"\n  [계정별]")
            for k, v in stats["by_account"].items():
                print(f"    {k}: {v:,}개")
        if stats["price_stats"]:
            ps = stats["price_stats"]
            print(f"\n  [가격 분포]")
            print(f"    최저: {ps['min']:,}원  최고: {ps['max']:,}원")
            print(f"    평균: {ps['avg']:,}원  중앙값: {ps['median']:,}원")
        if stats["categories"]:
            print(f"\n  [카테고리 TOP 10]")
            for k, v in list(stats["categories"].items())[:10]:
                print(f"    {k}: {v:,}개")

    elif action == "dup":
        filepath = getattr(args, "file", "") or ""
        if filepath:
            dups = find_duplicates(filepath=filepath)
        else:
            dups = find_duplicates(base_dir=base, folder=folder)
        if not dups:
            print("\n중복 상품 없음")
            return
        print(f"\n중복 상품: {len(dups)}건")
        for d in dups[:20]:
            print(f"\n  [{d['count']}회] {d['name'][:70]}")
            for loc in d["locations"]:
                print(f"    - {loc['file']} (Row {loc['row']})")
        if len(dups) > 20:
            print(f"\n  ... 외 {len(dups) - 20}건")

    elif action == "validate":
        filepath = getattr(args, "file", "") or ""
        targets = []
        if filepath:
            targets = [filepath]
        else:
            files = find_upload_files(base, folder, count=False)
            targets = [f["path"] for f in files]

        total_issues = 0
        for fpath in targets:
            issues = validate_upload(fpath)
            if issues:
                fname = os.path.basename(fpath)
                print(f"\n  [{fname}] {len(issues)}건 문제")
                for iss in issues[:10]:
                    print(f"    Row {iss['row']}: {iss['field']} - {iss['issue']}")
                if len(issues) > 10:
                    print(f"    ... 외 {len(issues) - 10}건")
                total_issues += len(issues)
        if total_issues == 0:
            print("\n  검증 통과 - 문제 없음")
        else:
            print(f"\n  총 {total_issues}건 문제 발견")

    elif action == "fix-dates":
        filepath = getattr(args, "file", "") or ""
        if not filepath:
            print("\n파일을 지정하세요 (-f)")
            return
        fixed = fix_dates(filepath)
        print(f"\n  {fixed}건 날짜 형식 수정 완료")

    elif action == "fill":
        filepath = getattr(args, "file", "") or ""
        targets = []
        if filepath:
            targets = [{"path": filepath, "filename": os.path.basename(filepath)}]
        else:
            targets = find_upload_files(base, folder, count=False)

        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[채우기]"
        total_filled = 0
        for t in targets:
            fpath = t["path"]
            try:
                result = fill_required_fields(fpath, dry_run=dry)
            except Exception as e:
                print(f"\n  [오류] {t['filename']}: {e}")
                continue
            if result["total"] > 0:
                print(f"\n  {mode} {t['filename']}:")
                if result["discount_ref"]:
                    print(f"    할인율기준가: {result['discount_ref']}건")
                if result["stock"]:
                    print(f"    재고수량: {result['stock']}건")
                if result["lead_time"]:
                    print(f"    출고리드타임: {result['lead_time']}건")
                total_filled += result["total"]

        if total_filled == 0:
            print("\n  모든 필수 필드가 채워져 있습니다.")
        else:
            print(f"\n  총 {total_filled}건 {mode} 완료")

    elif action == "optimize":
        filepath = getattr(args, "file", "") or ""
        targets = []
        if filepath:
            targets = [{"path": filepath, "filename": os.path.basename(filepath)}]
        else:
            targets = find_upload_files(base, folder, count=False)

        if not targets:
            print("\n  대상 파일이 없습니다.")
            return

        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[적용]"
        total_updated = 0
        for t in targets:
            fpath = t["path"]
            try:
                result = optimize_tags(fpath, dry_run=dry)
            except Exception as e:
                print(f"\n  [오류] {t['filename']}: {e}")
                continue
            print(f"\n  {mode} {t['filename']}:")
            print(f"    상품: {result['total']}개")
            print(f"    태그 변경: {result['updated']}개")
            print(f"    평균 태그: {result['avg_before']}개 → {result['avg_after']}개")
            total_updated += result["updated"]

        if total_updated == 0:
            print("\n  변경 사항 없음 — 이미 최적화되어 있습니다.")
        else:
            print(f"\n  총 {total_updated}건 {mode} 완료")

    elif action == "audit":
        filepath = getattr(args, "file", "") or ""
        targets = []
        if filepath:
            targets = [filepath]
        else:
            files = find_upload_files(base, folder, count=False)
            targets = [f["path"] for f in files]

        if not targets:
            print("\n  대상 파일이 없습니다.")
            return

        total_issues = 0
        for fpath in targets:
            try:
                issues = audit_seo(fpath)
            except Exception as e:
                print(f"\n  [오류] {os.path.basename(fpath)}: {e}")
                continue
            if issues:
                fname = os.path.basename(fpath)
                # 이슈 유형별 집계
                by_type: dict[str, int] = {}
                for iss in issues:
                    key = iss["issue"].split(":")[0] if ":" in iss["issue"] else iss["issue"]
                    by_type[key] = by_type.get(key, 0) + 1

                print(f"\n  [{fname}] SEO 이슈 {len(issues)}건")
                for itype, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
                    print(f"    {itype}: {cnt}건")
                print()
                for iss in issues[:15]:
                    print(f"    Row {iss['row']}: [{iss['field']}] {iss['issue']}")
                    print(f"           → {iss['suggestion']}")
                if len(issues) > 15:
                    print(f"    ... 외 {len(issues) - 15}건")
                total_issues += len(issues)
        if total_issues == 0:
            print("\n  SEO 감사 통과 — 이슈 없음")
        else:
            print(f"\n  총 {total_issues}건 SEO 이슈 발견")

    elif action == "auto-fill":
        filepath = getattr(args, "file", "") or ""
        targets = []
        if filepath:
            targets = [{"path": filepath, "filename": os.path.basename(filepath)}]
        else:
            targets = find_upload_files(base, folder, count=False)

        if not targets:
            print("\n  대상 파일이 없습니다.")
            return

        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[자동완성]"

        for t in targets:
            fpath = t["path"]
            try:
                r = auto_fill(fpath, dry_run=dry)
            except Exception as e:
                print(f"\n  [오류] {t['filename']}: {e}")
                continue

            print(f"\n  {mode} {t['filename']}:")
            fill = r.get("fill", {})
            if fill.get("total", 0) > 0:
                print(f"    필수필드: 할인율기준가 {fill.get('discount_ref', 0)}, 재고 {fill.get('stock', 0)}, 리드타임 {fill.get('lead_time', 0)}")
            if r.get("dates", 0) > 0:
                print(f"    날짜수정: {r['dates']}건")
            tags = r.get("tags", {})
            if tags.get("updated", 0) > 0:
                print(f"    태그최적화: {tags['updated']}건 (평균 {tags.get('avg_before', 0)} → {tags.get('avg_after', 0)}개)")
            if r.get("status", 0) > 0:
                print(f"    상품상태: {r['status']}건 → '판매중'")
            if r.get("start_date", 0) > 0:
                print(f"    판매시작일: {r['start_date']}건 → 오늘")
            if r.get("template"):
                print(f"    템플릿 구조 보정됨")
            if all(v in (0, False, {}) for v in [fill.get("total", 0), r.get("dates", 0),
                                                  tags.get("updated", 0), r.get("status", 0),
                                                  r.get("start_date", 0)]):
                print(f"    이미 모든 필드가 채워져 있습니다.")

    # ── API 명령 ─────────────────────────────────────────

    elif action == "register":
        from operations.product_api import register_from_excel

        account = getattr(args, "account", "")
        filepath = getattr(args, "file", "")
        if not account or not filepath:
            print("\n  계정(-a)과 파일(-f)을 지정하세요.")
            return

        dry = getattr(args, "dry_run", False)
        test = getattr(args, "test", 0)
        mode = "[미리보기]" if dry else "[등록]"
        print(f"\n{mode} 계정: {account}, 파일: {os.path.basename(filepath)}")

        try:
            result = register_from_excel(filepath, account, dry_run=dry, test_limit=test)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n  결과: 등록 {result['created']}, 건너뜀 {result['skipped']}, 오류 {result['error']}")
        if result["errors"]:
            print(f"\n  [오류 목록]")
            for err in result["errors"][:10]:
                print(f"    - {err}")
            if len(result["errors"]) > 10:
                print(f"    ... 외 {len(result['errors']) - 10}건")

    elif action == "update-tags":
        from operations.product_api import update_search_tags

        account = getattr(args, "account", "")
        if not account:
            print("\n  계정(-a)을 지정하세요.")
            return

        dry = getattr(args, "dry_run", False)
        test = getattr(args, "test", 0)
        mode = "[미리보기]" if dry else "[업데이트]"
        print(f"\n{mode} 계정: {account} — 판매중 상품 검색어 태그 업데이트")

        try:
            result = update_search_tags(account, dry_run=dry, test_limit=test)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n  결과: 전체 {result['total']}, 변경 {result['changed']}, "
              f"변경없음 {result['skipped']}, 오류 {result['error']}")
        if result.get("warning"):
            print(f"\n  {result['warning']}")

    elif action == "status":
        from operations.product_api import check_status, STATUS_MAP

        account = getattr(args, "account", "")
        if not account:
            print("\n  계정(-a)을 지정하세요.")
            return

        spid_str = getattr(args, "id", "") or ""
        spids = [s.strip() for s in spid_str.split(",") if s.strip()] if spid_str else None

        try:
            results = check_status(account, spids)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if spids:
            print(f"\n[{account}] 상품 상태 조회:")
            for r in results:
                print(f"  {r['sellerProductId']}: {r['status_kr']}  {r['name'][:50]}")
        else:
            # 전체 상태별 집계
            from collections import Counter
            status_cnt = Counter(r["status"] for r in results)
            print(f"\n[{account}] 전체 상품: {len(results)}개")
            for st, cnt in status_cnt.most_common():
                kr = STATUS_MAP.get(st, st)
                print(f"  {kr}: {cnt}개")

            # 상위 5개 표시
            if results:
                print(f"\n  최근 등록:")
                for r in results[:5]:
                    price = f"{r.get('salePrice', 0):,}원" if r.get('salePrice') else "-"
                    print(f"    [{r['status_kr']}] {r['name'][:50]}  {price}")
                if len(results) > 5:
                    print(f"    ... 외 {len(results) - 5}개")

    elif action == "delete":
        from operations.product_api import delete_products

        account = getattr(args, "account", "")
        spid_str = getattr(args, "id", "") or ""
        if not account or not spid_str:
            print("\n  계정(-a)과 상품ID(--id)를 지정하세요.")
            return

        spids = [s.strip() for s in spid_str.split(",") if s.strip()]
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[삭제]"
        print(f"\n{mode} 계정: {account}, 대상: {len(spids)}개")

        try:
            result = delete_products(account, spids, dry_run=dry)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n  결과: 삭제 {result['deleted']}, 오류 {result['error']}")
        if result["errors"]:
            for err in result["errors"]:
                print(f"    - {err}")

    elif action == "sync":
        from operations.product_api import sync_status

        account = getattr(args, "account", "")
        if not account:
            print("\n  계정(-a)을 지정하세요.")
            return

        try:
            result = sync_status(account, base, folder)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n[{account}] Excel ↔ API 동기화:")
        print(f"  API 상품: {result['api_total']}개")
        print(f"  Excel 상품: {result['excel_total']}개")
        print(f"  등록됨: {result['registered']}개")
        print(f"  미등록: {result['not_registered']}개")

        # 미등록 상품 5개 표시
        not_reg = [d for d in result["details"] if not d["registered"]]
        if not_reg:
            print(f"\n  [미등록 상품] (총 {len(not_reg)}개)")
            for d in not_reg[:10]:
                price = f"{d['price']:,}원" if d.get("price") else "-"
                print(f"    - {d['name'][:55]}  {price}  ({d['file']})")
            if len(not_reg) > 10:
                print(f"    ... 외 {len(not_reg) - 10}개")

    elif action == "gap":
        from operations.product_api import fill_gap

        account_from = getattr(args, "account_from", "") or getattr(args, "account_a", "")
        account_to = getattr(args, "account_to", "") or getattr(args, "account_b", "")
        if not account_from or not account_to or not folder:
            print("\n  두 계정(--from, --to)과 폴더(-d)를 지정하세요.")
            print("  예: upload gap -d 폴더 --from 007-ez --to 002-bm")
            return

        dry = getattr(args, "dry_run", False)
        test = getattr(args, "test", 0)
        mode = "[미리보기]" if dry else "[갭 채우기]"
        print(f"\n{mode} {account_from} → {account_to} (폴더: {folder})")

        try:
            result = fill_gap(account_from, account_to, base, folder,
                              dry_run=dry, test_limit=test)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n  갭 상품: {result['gap']}개")
        print(f"  등록: {result['created']}, 건너뜀: {result['skipped']}, 오류: {result['error']}")
        if result["errors"]:
            print(f"\n  [오류 목록]")
            for err in result["errors"][:5]:
                print(f"    - {err}")


def cmd_catalog(args, config):
    """카탈로그 매칭 (반자동화)"""
    from operations.catalog_matcher import (
        prepare_catalog_worksheet, batch_match,
        review_matches, generate_catalog_report,
    )

    if not args.catalog_action:
        print("\ncatalog 하위 명령을 지정하세요: prepare, match, review, report")
        return

    if args.catalog_action == "prepare":
        prepare_catalog_worksheet(args.account, config=config)

    elif args.catalog_action == "match":
        batch_match(args.account, config=config, limit=args.limit)

    elif args.catalog_action == "review":
        review_matches(args.account, config=config)

    elif args.catalog_action == "report":
        generate_catalog_report(args.account, config=config)


def cmd_product(args, config):
    """개별 상품 관리"""
    from operations.product_manager import (
        list_products, get_product_detail, search_products,
        update_product_name, update_product_tags, update_product_price,
        update_product_field, register_product, delete_product,
        stop_sale, resume_sale, get_change_history,
        STATUS_MAP,
    )

    action = args.product_action
    if not action:
        print("\nproduct 하위 명령을 지정하세요:")
        print("  조회: list, detail, search, history")
        print("  수정: update, update-name, update-tags, update-price")
        print("  관리: register, delete, stop, resume")
        return

    if action == "list":
        account = args.account
        try:
            products = list_products(
                account, status=args.status,
                search=args.search, limit=args.limit,
            )
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if not products:
            print(f"\n[{account}] 상품 없음")
            return

        print(f"\n[{account}] 상품 목록 ({len(products)}개)")
        print(f"\n{'SPID':<16} {'상태':<16} {'판매가':>10}  상품명")
        print("-" * 85)
        for p in products:
            price = f"{p['salePrice']:,}" if p.get("salePrice") else "-"
            name = p["name"][:45]
            print(f"{p['sellerProductId']:<16} {p['status_kr']:<16} {price:>10}  {name}")
        if len(products) >= args.limit:
            print(f"\n  ... 최대 {args.limit}개 표시 (--limit으로 조절)")

    elif action == "detail":
        try:
            data = get_product_detail(args.account, args.id)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n[상품 상세] SPID: {args.id}")
        print(f"  상품명: {data.get('sellerProductName', '')}")
        print(f"  상태: {data.get('status_kr', '')} ({data.get('status', '')})")
        print(f"  브랜드: {data.get('brand', '')}")
        print(f"  제조사: {data.get('manufacturer', '')}")
        print(f"  카테고리: {data.get('displayCategoryCode', '')}")

        items = data.get("items", [])
        for i, item in enumerate(items):
            print(f"\n  [아이템 {i+1}] vendorItemId: {item.get('vendorItemId', '')}")
            print(f"    판매가: {item.get('salePrice', 0):,}원")
            print(f"    정가: {item.get('originalPrice', 0):,}원")
            tags = item.get("searchTags", [])
            print(f"    검색어({len(tags)}): {'/'.join(tags[:10])}")
            if len(tags) > 10:
                print(f"           ... 외 {len(tags) - 10}개")
            print(f"    최대구매수량: {item.get('maximumBuyCount', 0)}")
            print(f"    출고리드타임: {item.get('outboundShippingTimeDay', 0)}일")

    elif action == "search":
        try:
            products = search_products(args.account, args.query, limit=args.limit)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if not products:
            print(f"\n[{args.account}] '{args.query}' 검색 결과 없음")
            return

        print(f"\n[{args.account}] '{args.query}' 검색 결과: {len(products)}개")
        print(f"\n{'SPID':<16} {'판매가':>10}  상품명")
        print("-" * 75)
        for p in products:
            price = f"{p['salePrice']:,}" if p.get("salePrice") else "-"
            print(f"{p['sellerProductId']:<16} {price:>10}  {p['name'][:50]}")

    elif action == "update":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[수정]"
        print(f"\n{mode} {args.account} / SPID {args.id} / {args.field} = {args.value}")

        try:
            result = update_product_field(
                args.account, args.id, args.field, args.value, dry_run=dry,
            )
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  전략: {result['strategy']}")
            print(f"  변경: {result['before']} → {result['after']}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "update-name":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[상품명 수정]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = update_product_name(
                args.account, args.id, args.name, dry_run=dry,
            )
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  변경: {result['before'][:60]}")
            print(f"     → {result['after'][:60]}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "update-tags":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[태그 수정]"
        tags = [t.strip() for t in args.tags.split("/") if t.strip()]
        merge = getattr(args, "merge", False)
        print(f"\n{mode} {args.account} / SPID {args.id} ({'병합' if merge else '교체'})")

        try:
            result = update_product_tags(
                args.account, args.id, tags, merge=merge, dry_run=dry,
            )
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            before_str = "/".join(result["before"][:5])
            after_str = "/".join(result["after"][:5])
            print(f"  기존({len(result['before'])}): {before_str}...")
            print(f"  변경({len(result['after'])}): {after_str}...")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "update-price":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[가격 수정]"
        price = getattr(args, "price", 0) or 0
        orig_price = getattr(args, "original_price", 0) or 0
        print(f"\n{mode} {args.account} / SPID {args.id}")

        if not price and not orig_price:
            print("  --price 또는 --original-price를 지정하세요.")
            return

        try:
            result = update_product_price(
                args.account, args.id, price=price,
                original_price=orig_price, dry_run=dry,
            )
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["changes"]:
            for ch in result["changes"]:
                print(f"  {ch['field']}: {ch['before']:,} → {ch['after']:,}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "register":
        dry = getattr(args, "dry_run", False)
        json_path = getattr(args, "json", "")
        mode = "[미리보기]" if dry else "[등록]"
        print(f"\n{mode} {args.account} / JSON: {json_path}")

        if not os.path.exists(json_path):
            print(f"  파일을 찾을 수 없습니다: {json_path}")
            return

        try:
            result = register_product(args.account, json_path, dry_run=dry)
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            if result.get("seller_product_id"):
                print(f"  등록 완료! SPID: {result['seller_product_id']}")
        if result.get("warning"):
            print(f"  {result['warning']}")

    elif action == "delete":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[삭제]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = delete_product(args.account, args.id, dry_run=dry)
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  삭제{'(미리보기)' if dry else ''}: {result['name'][:60]}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "stop":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[판매중지]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = stop_sale(args.account, args.id, dry_run=dry)
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  {result['items_stopped']}개 아이템 판매중지")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "resume":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[판매재개]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = resume_sale(args.account, args.id, dry_run=dry)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  {result['items_resumed']}개 아이템 판매재개")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "history":
        spid = getattr(args, "id", "") or ""
        print(f"\n[변경이력] {args.account}" + (f" / SPID {spid}" if spid else ""))

        try:
            hist = get_change_history(args.account, spid)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        # API 이력
        api_hist = hist.get("api_history", [])
        if api_hist:
            print(f"\n  [API 상태변경 이력] ({len(api_hist)}건)")
            for h in api_hist[:10]:
                print(f"    {h}")

        # 로컬 이력
        local_hist = hist.get("local_history", [])
        if local_hist:
            print(f"\n  [로컬 변경 이력] ({len(local_hist)}건)")
            print(f"  {'시간':<20} {'작업':<15} {'필드':<20} {'결과':<10}")
            print("  " + "-" * 70)
            for h in local_hist:
                t = h.get("changed_at", "")[:19]
                act = h.get("action", "")
                field = h.get("field", "")
                code = h.get("result_code", "")
                print(f"  {t:<20} {act:<15} {field:<20} {code:<10}")
                before = h.get("before_value", "")
                after = h.get("after_value", "")
                if before or after:
                    print(f"    {before[:40]} → {after[:40]}")
        elif not api_hist:
            print("\n  변경 이력 없음")


def main():
    parser = argparse.ArgumentParser(
        description="쿠팡 검색 알고리즘 분석 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python main.py collect --keyword "한끝" --pages 3     # Playwright 자동 수집
  python main.py import --file search.py                # HTML 파일 임포트
  python main.py analyze --keyword "한끝"                # 분석 실행
  python main.py report --keyword "한끝"                 # Excel 리포트
  python main.py strategy --keyword "한끝"               # 전략 제안
  python main.py full --keyword "한끝"                   # 전부 일괄 실행

  python main.py account list                           # 계정 목록
  python main.py account add -c 007-ez -n "007-EZ"      # 계정 추가
  python main.py account status -c 007-bm -s 비활성      # 상태 변경

  python main.py inv import -a 007-ez -f export.xlsx    # Wing Excel 임포트
  python main.py inv list -a 007-ez                     # 재고 목록
  python main.py inv search -q "검색어"                   # 전체 검색

  python main.py backup                                 # DB 백업
  python main.py backup --list                          # 백업 목록
  python main.py backup --restore latest                # 최신 백업 복원

  python main.py dashboard                              # 전체 대시보드
  python main.py dashboard -a 007-ez                    # 계정별 대시보드

  python main.py exposure check -a 007-ez -k "키워드"    # 노출 체크
  python main.py exposure batch -a 007-ez --top 50      # 상위 N개 배치 체크
  python main.py exposure report -a 007-ez              # 노출 리포트

  python main.py xray -k "키워드"                        # 역공학 분석
  python main.py xray -k "키워드" -o result.json         # 결과 JSON 저장

  python main.py ad -a 007-ez                            # 광고 추천 Excel 리포트
  python main.py ad -a 007-ez -o my_report.xlsx          # 저장 경로 지정

  python main.py catalog prepare -a 007-ez              # 수동 매칭용 Excel 생성
  python main.py catalog match -a 007-ez               # 후보 검색 (기본 50개)
  python main.py catalog match -a 007-ez --limit 30    # 30개만 검색
  python main.py catalog review -a 007-ez              # 후보 결과 확인
  python main.py catalog report -a 007-ez              # 매칭 체크리스트 Excel

  # ── Excel 관리 ──
  python main.py upload list                                    # 전체 파일 목록
  python main.py upload list -d "참고서_베스트셀러_업로드_v2"      # 폴더 지정
  python main.py upload filter -q "독서평설,자습서" -d 폴더       # 키워드 삭제
  python main.py upload filter -q "독서평설" --dry-run            # 미리보기
  python main.py upload search -q "마더텅"                        # 상품 검색
  python main.py upload compare -d 폴더 --a 007-ez --b 007-bm   # 계정 비교
  python main.py upload stats                                    # 통계
  python main.py upload dup -d 폴더                               # 중복 체크
  python main.py upload validate -d 폴더                          # 업로드 검증
  python main.py upload fix-dates -f 파일.xlsx                    # 날짜 형식 수정
  python main.py upload fill -d 폴더                               # 필수 필드 자동 채우기
  python main.py upload auto-fill -d 폴더 --dry-run               # 전체 자동완성 미리보기
  python main.py upload auto-fill -f 파일.xlsx                     # 전체 자동완성 적용
  python main.py upload optimize -f 파일.xlsx --dry-run            # 태그 최적화 미리보기
  python main.py upload optimize -f 파일.xlsx                      # 태그 최적화 적용
  python main.py upload audit -f 파일.xlsx                         # SEO 감사

  # ── API 상품 관리 ──
  python main.py upload register -a 007-ez -f "파일.xlsx" --dry-run  # 일괄등록 미리보기
  python main.py upload register -a 007-ez -f "파일.xlsx" --test 3   # 3개만 테스트
  python main.py upload update-tags -a 007-ez --dry-run              # 태그 업데이트 미리보기
  python main.py upload update-tags -a 007-ez --test 3               # 3개만 테스트
  python main.py upload status -a 007-ez                              # 전체 상태 조회
  python main.py upload status -a 007-ez --id "16044514927"           # 특정 상품 조회
  python main.py upload delete -a 007-ez --id "SPID1,SPID2"          # 상품 삭제
  python main.py upload sync -a 007-ez -d 폴더                       # Excel↔API 동기화
  python main.py upload gap -d 폴더 --from 007-ez --to 002-bm --dry-run  # 갭 미리보기
  python main.py upload gap -d 폴더 --from 007-ez --to 002-bm            # 갭 채우기

  # ── 개별 상품 관리 ──
  python main.py product list -a 007-ez                                   # 상품 목록
  python main.py product list -a 007-ez -s APPROVED -q "마더텅"            # 필터+검색
  python main.py product search -a 007-ez -q "마더텅"                      # 상품명 검색
  python main.py product detail -a 007-ez --id 12345                      # 상세 조회
  python main.py product update-name -a 007-ez --id 12345 --name "새이름" --dry-run  # 상품명 수정
  python main.py product update-tags -a 007-ez --id 12345 -t "태그1/태그2" --dry-run  # 태그 수정
  python main.py product update-price -a 007-ez --id 12345 --price 15000  # 가격 수정
  python main.py product update -a 007-ez --id 12345 -f brand -v "브랜드" --dry-run  # 범용 수정
  python main.py product register -a 007-ez -j product.json --dry-run     # 단일 등록
  python main.py product delete -a 007-ez --id 12345 --dry-run            # 삭제
  python main.py product stop -a 007-ez --id 12345 --dry-run              # 판매 중지
  python main.py product resume -a 007-ez --id 12345                      # 판매 재개
  python main.py product history -a 007-ez --id 12345                     # 변경 이력

  # ── 계정 간 수정 동기화 ──
  python main.py sync map --source-detail src.xlsx --source-price src_p.xlsx --target-detail tgt.xlsx --target-price tgt_p.xlsx
  python main.py sync fix --source-detail src.xlsx --source-price src_p.xlsx --target-detail tgt.xlsx --target-price tgt_p.xlsx -o corrected/
  python main.py sync gap --source 007-ez --target 002-bm --mapping-report corrected/mapping_report.xlsx --dry-run
  python main.py sync images --source 007-ez --target 002-bm --mapping-report corrected/mapping_report.xlsx --dry-run
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="실행할 명령")

    # collect
    p_collect = subparsers.add_parser("collect", help="Playwright 자동 수집")
    p_collect.add_argument("--keyword", "-k", required=True, help="검색 키워드")
    p_collect.add_argument("--pages", "-p", type=int, default=3, help="수집 페이지 수 (기본: 3)")
    p_collect.add_argument("--enrich", action="store_true", help="상세페이지 보강 여부")

    # import
    p_import = subparsers.add_parser("import", help="HTML 파일 임포트")
    p_import.add_argument("--file", "-f", required=True, help="HTML 파일 경로")
    p_import.add_argument("--keyword", "-k", default="", help="키워드 (미지정 시 자동 추출)")

    # analyze
    p_analyze = subparsers.add_parser("analyze", help="분석 실행")
    p_analyze.add_argument("--keyword", "-k", required=True, help="키워드")

    # report
    p_report = subparsers.add_parser("report", help="Excel 리포트 생성")
    p_report.add_argument("--keyword", "-k", required=True, help="키워드")

    # strategy
    p_strategy = subparsers.add_parser("strategy", help="전략 제안")
    p_strategy.add_argument("--keyword", "-k", required=True, help="키워드")

    # full
    p_full = subparsers.add_parser("full", help="수집+분석+리포트+전략 일괄 실행")
    p_full.add_argument("--keyword", "-k", required=True, help="키워드")
    p_full.add_argument("--pages", "-p", type=int, default=3, help="수집 페이지 수 (기본: 3)")
    p_full.add_argument("--file", "-f", default="", help="HTML 파일로 임포트 (미지정 시 Playwright 수집)")

    # ── 계정 관리 ──
    p_account = subparsers.add_parser("account", help="셀러 계정 관리")
    account_sub = p_account.add_subparsers(dest="account_action")

    account_sub.add_parser("list", help="계정 목록")

    p_acc_add = account_sub.add_parser("add", help="계정 추가")
    p_acc_add.add_argument("--code", "-c", required=True, help="계정 코드 (예: 007-ez)")
    p_acc_add.add_argument("--name", "-n", default="", help="계정 이름")
    p_acc_add.add_argument("--status", "-s", default="활성", help="상태 (기본: 활성)")
    p_acc_add.add_argument("--vendor-id", "-v", default="", help="벤더 ID")
    p_acc_add.add_argument("--memo", "-m", default="", help="메모")

    p_acc_status = account_sub.add_parser("status", help="계정 상태 변경")
    p_acc_status.add_argument("--code", "-c", required=True, help="계정 코드")
    p_acc_status.add_argument("--new-status", "-s", required=True, help="새 상태 (활성/비활성/복구중/확인필요)")

    # ── 재고 관리 ──
    p_inv = subparsers.add_parser("inv", help="재고 상품 관리")
    inv_sub = p_inv.add_subparsers(dest="inv_action")

    p_inv_import = inv_sub.add_parser("import", help="Wing Excel 임포트")
    p_inv_import.add_argument("--account", "-a", required=True, help="계정 코드")
    p_inv_import.add_argument("--file", "-f", required=True, help="Excel/CSV 파일 경로")

    p_inv_list = inv_sub.add_parser("list", help="재고 목록")
    p_inv_list.add_argument("--account", "-a", required=True, help="계정 코드")
    p_inv_list.add_argument("--status", default=None, help="상태 필터 (판매중/품절/판매중지)")
    p_inv_list.add_argument("--limit", type=int, default=50, help="표시 개수 (기본: 50)")

    p_inv_search = inv_sub.add_parser("search", help="상품 검색 (전체 계정)")
    p_inv_search.add_argument("--query", "-q", required=True, help="검색어")

    # ── 백업 ──
    p_backup = subparsers.add_parser("backup", help="DB 백업/복원")
    p_backup.add_argument("--list", dest="list_backups", action="store_true", help="백업 목록")
    p_backup.add_argument("--restore", default="", help="복원 (latest 또는 파일명)")

    # ── 대시보드 ──
    p_dashboard = subparsers.add_parser("dashboard", help="콘솔 대시보드")
    p_dashboard.add_argument("--account", "-a", default="", help="계정 코드 (미지정 시 전체)")

    # ── 노출 모니터링 ──
    p_exposure = subparsers.add_parser("exposure", help="노출 모니터링")
    exposure_sub = p_exposure.add_subparsers(dest="exposure_action")

    p_exp_check = exposure_sub.add_parser("check", help="단일 키워드 노출 체크")
    p_exp_check.add_argument("--account", "-a", required=True, help="계정 코드")
    p_exp_check.add_argument("--keyword", "-k", required=True, help="검색 키워드")
    p_exp_check.add_argument("--pages", "-p", type=int, default=3, help="검색 페이지 수")

    p_exp_batch = exposure_sub.add_parser("batch", help="상위 N개 상품 배치 체크")
    p_exp_batch.add_argument("--account", "-a", required=True, help="계정 코드")
    p_exp_batch.add_argument("--top", type=int, default=50, help="상위 N개 (기본: 50)")

    p_exp_report = exposure_sub.add_parser("report", help="노출 리포트")
    p_exp_report.add_argument("--account", "-a", required=True, help="계정 코드")

    # ── 역공학 분석 ──
    p_xray = subparsers.add_parser("xray", help="쿠팡 시스템 역공학 분석 (SERP 구조, ID 체계, 순위 심층, 배송 부스트, 가격 알고리즘)")
    p_xray.add_argument("--keyword", "-k", required=True, help="분석할 키워드")
    p_xray.add_argument("--output", "-o", default="", help="결과 JSON 저장 경로 (선택)")

    # ── 광고 추천 ──
    p_ad = subparsers.add_parser("ad", help="광고 추천 리포트 (키워드별 경쟁 분석 + 추천 상품 Excel)")
    p_ad.add_argument("--account", "-a", required=True, help="계정 코드")
    p_ad.add_argument("--source", "-s", choices=["db", "api"], default="db",
                      help="상품 소스 (db=DB재고, api=API 실시간 데이터)")
    p_ad.add_argument("--output", "-o", default="", help="저장 경로 (기본: reports/{계정}_광고추천.xlsx)")

    # ── 카탈로그 매칭 (반자동화) ──
    p_catalog = subparsers.add_parser("catalog", help="카탈로그 매칭 (반자동화)")
    catalog_sub = p_catalog.add_subparsers(dest="catalog_action")

    p_cat_prepare = catalog_sub.add_parser("prepare", help="수동 매칭용 Excel 생성 (웹 검색 없음)")
    p_cat_prepare.add_argument("--account", "-a", required=True, help="계정 코드")

    p_cat_match = catalog_sub.add_parser("match", help="쿠팡 웹에서 후보 검색 (소량 배치)")
    p_cat_match.add_argument("--account", "-a", required=True, help="계정 코드")
    p_cat_match.add_argument("--limit", type=int, default=50, help="검색할 상품 수 (기본: 50, 권장 최대)")

    p_cat_review = catalog_sub.add_parser("review", help="후보 검색 결과 확인")
    p_cat_review.add_argument("--account", "-a", required=True, help="계정 코드")

    p_cat_report = catalog_sub.add_parser("report", help="매칭 체크리스트 Excel 생성")
    p_cat_report.add_argument("--account", "-a", required=True, help="계정 코드")

    # ── 업로드 엑셀 관리 ──
    p_upload = subparsers.add_parser("upload", help="업로드 엑셀 파일 관리 (필터/검색/비교/통계/검증)")
    upload_sub = p_upload.add_subparsers(dest="upload_action")

    p_upl_list = upload_sub.add_parser("list", help="파일 목록")
    p_upl_list.add_argument("--dir", "-d", default="", help="폴더명 (미지정 시 전체)")

    p_upl_filter = upload_sub.add_parser("filter", help="키워드로 상품 삭제")
    p_upl_filter.add_argument("--query", "-q", required=True, help="삭제 키워드 (쉼표 구분)")
    p_upl_filter.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_filter.add_argument("--dry-run", action="store_true", help="삭제하지 않고 미리보기만")

    p_upl_search = upload_sub.add_parser("search", help="상품 검색")
    p_upl_search.add_argument("--query", "-q", required=True, help="검색어")
    p_upl_search.add_argument("--dir", "-d", default="", help="폴더명")

    p_upl_compare = upload_sub.add_parser("compare", help="계정 간 상품 비교")
    p_upl_compare.add_argument("--dir", "-d", required=True, help="폴더명")
    p_upl_compare.add_argument("--a", dest="account_a", required=True, help="계정 A")
    p_upl_compare.add_argument("--b", dest="account_b", required=True, help="계정 B")

    p_upl_stats = upload_sub.add_parser("stats", help="통계")
    p_upl_stats.add_argument("--dir", "-d", default="", help="폴더명")

    p_upl_dup = upload_sub.add_parser("dup", help="중복 체크")
    p_upl_dup.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_dup.add_argument("--file", "-f", default="", help="특정 파일")

    p_upl_validate = upload_sub.add_parser("validate", help="업로드 검증")
    p_upl_validate.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_validate.add_argument("--file", "-f", default="", help="특정 파일")

    p_upl_fixdates = upload_sub.add_parser("fix-dates", help="날짜 형식 자동 수정")
    p_upl_fixdates.add_argument("--file", "-f", required=True, help="파일 경로")

    p_upl_fill = upload_sub.add_parser("fill", help="필수 필드 자동 채우기 (할인율기준가/재고/리드타임)")
    p_upl_fill.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_fill.add_argument("--file", "-f", default="", help="특정 파일")
    p_upl_fill.add_argument("--dry-run", action="store_true", help="수정하지 않고 미리보기만")

    p_upl_optimize = upload_sub.add_parser("optimize", help="검색어 태그 SEO 최적화 (자동 태그 생성/병합)")
    p_upl_optimize.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_optimize.add_argument("--file", "-f", default="", help="특정 파일")
    p_upl_optimize.add_argument("--dry-run", action="store_true", help="수정하지 않고 미리보기만")

    p_upl_audit = upload_sub.add_parser("audit", help="SEO 감사 (상품명 길이/태그 수/단편/누락 체크)")
    p_upl_audit.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_audit.add_argument("--file", "-f", default="", help="특정 파일")

    p_upl_autofill = upload_sub.add_parser("auto-fill", help="모든 필수 필드 자동완성 (fill + 날짜 + 태그 + 상태 + 시작일)")
    p_upl_autofill.add_argument("--dir", "-d", default="", help="폴더명")
    p_upl_autofill.add_argument("--file", "-f", default="", help="특정 파일")
    p_upl_autofill.add_argument("--dry-run", action="store_true", help="수정하지 않고 미리보기만")

    # ── API 명령 ──
    p_upl_register = upload_sub.add_parser("register", help="Excel → API 일괄등록")
    p_upl_register.add_argument("--account", "-a", required=True, help="계정 코드")
    p_upl_register.add_argument("--file", "-f", required=True, help="Excel 파일 경로")
    p_upl_register.add_argument("--dry-run", action="store_true", help="등록하지 않고 미리보기만")
    p_upl_register.add_argument("--test", type=int, default=0, help="N개만 테스트 (0=전체)")

    p_upl_tags = upload_sub.add_parser("update-tags", help="판매중 상품 검색어 태그 일괄 업데이트")
    p_upl_tags.add_argument("--account", "-a", required=True, help="계정 코드")
    p_upl_tags.add_argument("--dry-run", action="store_true", help="변경하지 않고 미리보기만")
    p_upl_tags.add_argument("--test", type=int, default=0, help="N개만 테스트 (0=전체)")

    p_upl_status = upload_sub.add_parser("status", help="API 상품 등록 상태 조회")
    p_upl_status.add_argument("--account", "-a", required=True, help="계정 코드")
    p_upl_status.add_argument("--id", default="", help="셀러상품ID (쉼표 구분, 미지정 시 전체)")

    p_upl_delete = upload_sub.add_parser("delete", help="API 상품 삭제")
    p_upl_delete.add_argument("--account", "-a", required=True, help="계정 코드")
    p_upl_delete.add_argument("--id", required=True, help="셀러상품ID (쉼표 구분)")
    p_upl_delete.add_argument("--dry-run", action="store_true", help="삭제하지 않고 미리보기만")

    p_upl_sync = upload_sub.add_parser("sync", help="Excel ↔ API 상태 동기화 비교")
    p_upl_sync.add_argument("--account", "-a", required=True, help="계정 코드")
    p_upl_sync.add_argument("--dir", "-d", default="", help="폴더명")

    p_upl_gap = upload_sub.add_parser("gap", help="계정 간 갭 상품 API 등록 (A에만 있고 B에 없는 상품)")
    p_upl_gap.add_argument("--dir", "-d", required=True, help="비교 폴더명")
    p_upl_gap.add_argument("--from", dest="account_from", required=True, help="기준 계정 (상품이 있는 쪽)")
    p_upl_gap.add_argument("--to", dest="account_to", required=True, help="대상 계정 (빈 쪽, 등록할 계정)")
    p_upl_gap.add_argument("--dry-run", action="store_true", help="등록하지 않고 미리보기만")
    p_upl_gap.add_argument("--test", type=int, default=0, help="N개만 테스트 (0=전체)")

    # ── 상품명/검색어 최적화 ──
    p_optimize = subparsers.add_parser("optimize", help="상품명/검색어 자동 최적화")
    optimize_sub = p_optimize.add_subparsers(dest="optimize_action")

    p_opt_preview = optimize_sub.add_parser("preview", help="최적화 미리보기")
    p_opt_preview.add_argument("--account", "-a", required=True, help="계정 코드")
    p_opt_preview.add_argument("--count", "-n", type=int, default=10, help="미리보기 개수 (기본: 10)")

    p_opt_run = optimize_sub.add_parser("run", help="전체 최적화 실행")
    p_opt_run.add_argument("--account", "-a", required=True, help="계정 코드")

    p_opt_export = optimize_sub.add_parser("export", help="최적화된 Wing Excel 내보내기")
    p_opt_export.add_argument("--account", "-a", required=True, help="계정 코드")

    # ── 개별 상품 관리 ──
    p_product = subparsers.add_parser("product", help="개별 상품 관리 (조회/수정/삭제/등록)")
    product_sub = p_product.add_subparsers(dest="product_action")

    p_prod_list = product_sub.add_parser("list", help="상품 목록 조회")
    p_prod_list.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_list.add_argument("--status", "-s", default="", help="상태 필터 (APPROVED/DRAFT/PENDING)")
    p_prod_list.add_argument("--search", "-q", default="", help="상품명 검색")
    p_prod_list.add_argument("--limit", type=int, default=50, help="최대 표시 개수 (기본: 50)")

    p_prod_detail = product_sub.add_parser("detail", help="상품 상세 조회")
    p_prod_detail.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_detail.add_argument("--id", required=True, help="셀러상품ID (SPID)")

    p_prod_search = product_sub.add_parser("search", help="상품명 검색")
    p_prod_search.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_search.add_argument("--query", "-q", required=True, help="검색어")
    p_prod_search.add_argument("--limit", type=int, default=50, help="최대 표시 개수 (기본: 50)")

    p_prod_update = product_sub.add_parser("update", help="범용 필드 수정 (전략 자동 선택)")
    p_prod_update.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_update.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_update.add_argument("--field", "-f", required=True, help="수정할 필드명 (salePrice/searchTags/brand 등)")
    p_prod_update.add_argument("--value", "-v", required=True, help="새 값")
    p_prod_update.add_argument("--dry-run", action="store_true", help="실제 변경하지 않고 미리보기")

    p_prod_name = product_sub.add_parser("update-name", help="상품명 수정 (Full Update → 재승인 필요)")
    p_prod_name.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_name.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_name.add_argument("--name", "-n", required=True, help="새 상품명")
    p_prod_name.add_argument("--dry-run", action="store_true", help="실제 변경하지 않고 미리보기")

    p_prod_tags = product_sub.add_parser("update-tags", help="검색어 태그 수정 (Full Update → 재승인 필요)")
    p_prod_tags.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_tags.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_tags.add_argument("--tags", "-t", required=True, help="검색어 (/ 구분)")
    p_prod_tags.add_argument("--merge", action="store_true", help="기존 태그와 병합 (기본: 교체)")
    p_prod_tags.add_argument("--dry-run", action="store_true", help="실제 변경하지 않고 미리보기")

    p_prod_price = product_sub.add_parser("update-price", help="가격 수정 (Partial → 재승인 불필요)")
    p_prod_price.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_price.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_price.add_argument("--price", type=int, default=0, help="판매가")
    p_prod_price.add_argument("--original-price", type=int, default=0, help="정가 (할인율기준가)")
    p_prod_price.add_argument("--dry-run", action="store_true", help="실제 변경하지 않고 미리보기")

    p_prod_register = product_sub.add_parser("register", help="단일 상품 등록")
    p_prod_register.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_register.add_argument("--json", "-j", required=True, help="상품 JSON 파일 경로")
    p_prod_register.add_argument("--dry-run", action="store_true", help="등록하지 않고 미리보기")

    p_prod_delete = product_sub.add_parser("delete", help="상품 삭제")
    p_prod_delete.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_delete.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_delete.add_argument("--dry-run", action="store_true", help="삭제하지 않고 미리보기")

    p_prod_stop = product_sub.add_parser("stop", help="판매 중지")
    p_prod_stop.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_stop.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_stop.add_argument("--dry-run", action="store_true", help="실행하지 않고 미리보기")

    p_prod_resume = product_sub.add_parser("resume", help="판매 재개")
    p_prod_resume.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_resume.add_argument("--id", required=True, help="셀러상품ID (SPID)")
    p_prod_resume.add_argument("--dry-run", action="store_true", help="실행하지 않고 미리보기")

    p_prod_history = product_sub.add_parser("history", help="변경 이력 조회")
    p_prod_history.add_argument("--account", "-a", required=True, help="계정 코드")
    p_prod_history.add_argument("--id", default="", help="셀러상품ID (미지정 시 계정 전체)")

    # ── 계정 간 수정 동기화 ──
    p_sync = subparsers.add_parser("sync", help="계정 간 상품 수정사항 동기화 (007-ez → 타겟)")
    sync_sub = p_sync.add_subparsers(dest="sync_action")

    # sync map
    p_sync_map = sync_sub.add_parser("map", help="매핑 리포트 생성 (먼저 실행)")
    p_sync_map.add_argument("--source-detail", required=True, help="소스(007-ez) detailinfo Excel 경로")
    p_sync_map.add_argument("--source-price", required=True, help="소스(007-ez) price_inventory Excel 경로")
    p_sync_map.add_argument("--target-detail", required=True, help="타겟 detailinfo Excel 경로")
    p_sync_map.add_argument("--target-price", required=True, help="타겟 price_inventory Excel 경로")
    p_sync_map.add_argument("--output", "-o", default="", help="리포트 저장 경로 (기본: reports/mapping_report_*.xlsx)")

    # sync apply
    p_sync_apply = sync_sub.add_parser("apply", help="API로 일괄 적용 (가격PATCH + 이름PUT + 갭POST)")
    p_sync_apply.add_argument("--source", required=True, help="소스 계정 코드 (예: 007-ez)")
    p_sync_apply.add_argument("--target", required=True, help="타겟 계정 코드 (예: 002-bm)")
    p_sync_apply.add_argument("--mapping-report", required=True, help="매핑 리포트 Excel 경로")
    p_sync_apply.add_argument("--dry-run", action="store_true", help="실행하지 않고 미리보기만")
    p_sync_apply.add_argument("--test", type=int, default=0, help="N개만 테스트 (0=전체)")
    p_sync_apply.add_argument("--include-fuzzy", action="store_true", help="퍼지 매칭도 포함")
    p_sync_apply.add_argument("--skip-images", action="store_true", help="이미지 동기화 건너뛰기")

    # sync fix
    p_sync_fix = sync_sub.add_parser("fix", help="수정 Excel 생성 (Phase A: 상품명+가격)")
    p_sync_fix.add_argument("--source-detail", required=True, help="소스 detailinfo Excel 경로")
    p_sync_fix.add_argument("--source-price", required=True, help="소스 price_inventory Excel 경로")
    p_sync_fix.add_argument("--target-detail", required=True, help="타겟 detailinfo Excel 경로")
    p_sync_fix.add_argument("--target-price", required=True, help="타겟 price_inventory Excel 경로")
    p_sync_fix.add_argument("--output", "-o", default="corrected", help="출력 폴더 (기본: corrected/)")
    p_sync_fix.add_argument("--include-fuzzy", action="store_true", help="퍼지 매칭도 수정에 포함 (기본: 제외)")

    # sync gap
    p_sync_gap = sync_sub.add_parser("gap", help="미등록 상품 신규 등록 (Phase B: 갭 채우기)")
    p_sync_gap.add_argument("--source", required=True, help="소스 계정 코드 (예: 007-ez)")
    p_sync_gap.add_argument("--target", required=True, help="타겟 계정 코드 (예: 002-bm)")
    p_sync_gap.add_argument("--mapping-report", required=True, help="매핑 리포트 Excel 경로")
    p_sync_gap.add_argument("--dry-run", action="store_true", help="등록하지 않고 미리보기만")
    p_sync_gap.add_argument("--test", type=int, default=0, help="N개만 테스트 (0=전체)")

    # sync images
    p_sync_images = sync_sub.add_parser("images", help="이미지 동기화 (Phase C: API PUT)")
    p_sync_images.add_argument("--source", required=True, help="소스 계정 코드 (예: 007-ez)")
    p_sync_images.add_argument("--target", required=True, help="타겟 계정 코드 (예: 002-bm)")
    p_sync_images.add_argument("--mapping-report", required=True, help="매핑 리포트 Excel 경로")
    p_sync_images.add_argument("--dry-run", action="store_true", help="실행하지 않고 미리보기만")
    p_sync_images.add_argument("--test", type=int, default=0, help="N개만 테스트 (0=전체)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    config = AnalysisConfig()

    commands = {
        "collect": cmd_collect,
        "import": cmd_import,
        "analyze": cmd_analyze,
        "report": cmd_report,
        "strategy": cmd_strategy,
        "full": cmd_full,
        "account": cmd_account,
        "inv": cmd_inv,
        "backup": cmd_backup,
    }

    # 별도 모듈 명령어
    if args.command == "ad":
        cmd_ad(args, config)
    elif args.command == "xray":
        cmd_xray(args, config)
    elif args.command == "dashboard":
        from dashboard.app import cmd_dashboard
        cmd_dashboard(args, config)
    elif args.command == "exposure":
        from operations.exposure import cmd_exposure
        cmd_exposure(args, config)
    elif args.command == "catalog":
        cmd_catalog(args, config)
    elif args.command == "upload":
        cmd_upload(args, config)
    elif args.command == "optimize":
        from operations.optimizer import cmd_optimize
        cmd_optimize(args, config)
    elif args.command == "product":
        cmd_product(args, config)
    elif args.command == "sync":
        from operations.sync_corrections import cmd_sync
        cmd_sync(args, config)
    elif args.command in commands:
        commands[args.command](args, config)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
