"""콘솔 대시보드 -- 셀러 계정/재고/노출 현황"""

from core.config import AnalysisConfig
from core.database import CoupangDB
from operations.backup import list_backups


def cmd_dashboard(args, config: AnalysisConfig):
    """대시보드 명령어 핸들러"""
    if args.account:
        _show_account_detail(args.account, config)
    else:
        _show_overview(config)


def _show_overview(config: AnalysisConfig):
    """전체 대시보드"""
    db = CoupangDB(config)
    try:
        accounts = db.list_accounts()

        SEP = "=" * 75
        print(f"\n{SEP}")
        print("  셀러 상품 관리 대시보드")
        print(f"{SEP}")

        if not accounts:
            print("\n  등록된 계정이 없습니다.")
            print("  python main.py account add -c 007-ez -n '007-EZ' 로 추가하세요.")
            print(f"\n{SEP}")
            return

        # 계정별 요약 테이블
        print(f"\n  {'계정':<12} {'이름':<14} {'상태':<8} {'상품수':>7} {'판매중':>7} {'품절':>6} {'노출률':>7} {'마지막 동기화'}")
        print(f"  {'-' * 72}")

        for acc in accounts:
            total = db.get_inventory_total(acc.id)
            status_counts = db.count_inventory_by_status(acc.id)
            selling = status_counts.get("판매중", 0)
            soldout = status_counts.get("품절", 0)

            # 노출률
            exposure = db.get_exposure_summary(acc.id)
            if exposure["total_checks"] > 0:
                rate = f"{exposure['found'] / exposure['total_checks'] * 100:.0f}%"
            else:
                rate = "-"

            # 마지막 동기화
            last_snap = db.get_last_inventory_snapshot(acc.id)
            last_sync = last_snap["imported_at"][:10] if last_snap else "-"

            print(f"  {acc.account_code:<12} {acc.account_name:<14} {acc.status:<8} "
                  f"{total:>7,} {selling:>7,} {soldout:>6,} {rate:>7} {last_sync}")

        # 백업 현황
        backups = list_backups(config)
        print(f"\n  백업 현황: {len(backups)}개")
        if backups:
            latest = backups[0]
            print(f"  최신 백업: {latest['filename']} ({latest['size_mb']}MB)")

        print(f"\n{SEP}")

    finally:
        db.close()


def _show_account_detail(account_code: str, config: AnalysisConfig):
    """계정 상세 대시보드"""
    db = CoupangDB(config)
    try:
        account = db.get_account_by_code(account_code)
        if not account:
            print(f"\n계정을 찾을 수 없습니다: {account_code}")
            return

        SEP = "=" * 65
        print(f"\n{SEP}")
        print(f"  계정 상세: {account.account_code} ({account.account_name})")
        print(f"{SEP}")

        print(f"\n  상태: {account.status}")
        print(f"  벤더ID: {account.vendor_id or '-'}")
        print(f"  메모: {account.memo or '-'}")
        print(f"  등록일: {account.created_at[:10]}")

        # 상태별 분포
        total = db.get_inventory_total(account.id)
        status_counts = db.count_inventory_by_status(account.id)

        print(f"\n  [상품 현황] 총 {total:,}개")
        if status_counts:
            for s, c in status_counts.items():
                pct = c / total * 100 if total > 0 else 0
                bar = "#" * int(pct / 2) + "." * (50 - int(pct / 2))
                print(f"    {s:<8} {c:>6,} ({pct:>5.1f}%) {bar}")
        else:
            print("    상품 없음. Wing Excel을 임포트하세요.")

        # 임포트 히스토리
        last_snap = db.get_last_inventory_snapshot(account.id)
        if last_snap:
            print(f"\n  [마지막 임포트]")
            print(f"    파일: {last_snap['source_file']}")
            print(f"    일시: {last_snap['imported_at'][:19]}")
            print(f"    결과: 전체 {last_snap['total_products']}, "
                  f"신규 {last_snap['new_products']}, 갱신 {last_snap['updated_products']}")

        # 노출 요약
        exposure = db.get_exposure_summary(account.id)
        if exposure["total_checks"] > 0:
            rate = exposure["found"] / exposure["total_checks"] * 100
            print(f"\n  [노출 현황]")
            print(f"    체크 수: {exposure['total_checks']}")
            print(f"    노출됨: {exposure['found']} ({rate:.1f}%)")
            print(f"    미노출: {exposure['not_found']}")
        else:
            print(f"\n  [노출 현황] 데이터 없음")

        print(f"\n{SEP}")

    finally:
        db.close()
