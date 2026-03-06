"""관리 CLI 명령: account, inv, backup, xray, ad"""

from core.database import SessionLocal
from core.models import Account, InventoryProduct
from operations.backup import create_backup, list_backups, restore_backup
from operations.inventory import import_wing_excel


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
