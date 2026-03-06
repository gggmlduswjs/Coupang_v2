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

    # ── 명령 디스패치 ──
    cmd = args.command

    if cmd in ("collect", "import", "analyze", "report", "strategy", "full"):
        from cli.commands.analysis import (
            cmd_collect, cmd_import, cmd_analyze, cmd_report, cmd_strategy, cmd_full,
        )
        {"collect": cmd_collect, "import": cmd_import, "analyze": cmd_analyze,
         "report": cmd_report, "strategy": cmd_strategy, "full": cmd_full}[cmd](args, config)

    elif cmd in ("account", "inv", "backup", "xray", "ad"):
        from cli.commands.management import (
            cmd_account, cmd_inv, cmd_backup, cmd_xray, cmd_ad,
        )
        {"account": cmd_account, "inv": cmd_inv, "backup": cmd_backup,
         "xray": cmd_xray, "ad": cmd_ad}[cmd](args, config)

    elif cmd == "upload":
        from cli.commands.upload import cmd_upload
        cmd_upload(args, config)

    elif cmd == "product":
        from cli.commands.product import cmd_product
        cmd_product(args, config)

    elif cmd == "catalog":
        from cli.commands.catalog import cmd_catalog
        cmd_catalog(args, config)

    elif cmd == "dashboard":
        from dashboard.app import cmd_dashboard
        cmd_dashboard(args, config)

    elif cmd == "exposure":
        from operations.exposure import cmd_exposure
        cmd_exposure(args, config)

    elif cmd == "optimize":
        from operations.optimizer import cmd_optimize
        cmd_optimize(args, config)

    elif cmd == "sync":
        from operations.sync_corrections import cmd_sync
        cmd_sync(args, config)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
