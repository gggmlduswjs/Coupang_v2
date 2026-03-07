"""쿠팡 검색 알고리즘 분석 도구 - CLI 진입점"""

import argparse
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from core.config import AnalysisConfig


def main():
    parser = argparse.ArgumentParser(
        description="쿠팡 검색 알고리즘 분석 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
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

        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="실행할 명령")

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

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    config = AnalysisConfig()

    # ── 명령 디스패치 ──
    cmd = args.command

    if cmd in ("account", "inv", "backup"):
        from cli.commands.management import cmd_account, cmd_inv, cmd_backup
        {"account": cmd_account, "inv": cmd_inv, "backup": cmd_backup}[cmd](args, config)

    elif cmd == "product":
        from cli.commands.product import cmd_product
        cmd_product(args, config)

    elif cmd == "dashboard":
        from dashboard.app import cmd_dashboard
        cmd_dashboard(args, config)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
