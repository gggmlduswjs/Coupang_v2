"""업로드 CLI 명령: upload (Excel 관리 + API 상품 관리)"""

import os

from operations.upload_excel import (
    find_upload_files, filter_products, update_filename_count,
    search_products, compare_accounts, get_stats,
    find_duplicates, validate_upload, fix_dates,
    fill_required_fields, optimize_tags, audit_seo, auto_fill,
)


def cmd_upload(args, config):
    """업로드 엑셀 파일 관리 + API 상품 관리"""
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
        from operations.product_api import check_status
        from core.constants import STATUS_MAP

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
