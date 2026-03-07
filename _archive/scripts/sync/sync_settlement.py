"""
정산 내역 동기화 스크립트
========================
WING Settlement History API → settlement_history 테이블

사용법:
    python scripts/sync_settlement.py              # 기본 6개월
    python scripts/sync_settlement.py --months 1   # 최근 1개월
    python scripts/sync_settlement.py --account 007-book  # 특정 계정만
"""
import os
import sys
import json
import argparse
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Callable

from sqlalchemy import text

# 프로젝트 루트
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from core.database import get_engine_for_db

from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.services.sync_base import get_accounts, create_wing_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class SettlementSync:
    """정산 내역 동기화 엔진"""

    CREATE_INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS ix_settle_account_month ON settlement_history(account_id, year_month)",
        "CREATE INDEX IF NOT EXISTS ix_settle_month ON settlement_history(year_month)",
    ]

    UPSERT_SQL = """
        INSERT INTO settlement_history
            (account_id, year_month, settlement_type, settlement_date,
             settlement_status, revenue_date_from, revenue_date_to,
             total_sale, service_fee, settlement_target_amount,
             settlement_amount, last_amount, pending_released_amount,
             seller_discount_coupon, downloadable_coupon,
             seller_service_fee, courantee_fee, deduction_amount,
             debt_of_last_week, final_amount,
             bank_name, bank_account, raw_json)
        VALUES
            (:account_id, :year_month, :settlement_type, :settlement_date,
             :settlement_status, :revenue_date_from, :revenue_date_to,
             :total_sale, :service_fee, :settlement_target_amount,
             :settlement_amount, :last_amount, :pending_released_amount,
             :seller_discount_coupon, :downloadable_coupon,
             :seller_service_fee, :courantee_fee, :deduction_amount,
             :debt_of_last_week, :final_amount,
             :bank_name, :bank_account, :raw_json)
        ON CONFLICT (account_id, year_month, settlement_type, settlement_date) DO UPDATE SET
            settlement_status=EXCLUDED.settlement_status,
            revenue_date_from=EXCLUDED.revenue_date_from, revenue_date_to=EXCLUDED.revenue_date_to,
            total_sale=EXCLUDED.total_sale, service_fee=EXCLUDED.service_fee,
            settlement_target_amount=EXCLUDED.settlement_target_amount,
            settlement_amount=EXCLUDED.settlement_amount, last_amount=EXCLUDED.last_amount,
            pending_released_amount=EXCLUDED.pending_released_amount,
            seller_discount_coupon=EXCLUDED.seller_discount_coupon,
            downloadable_coupon=EXCLUDED.downloadable_coupon,
            seller_service_fee=EXCLUDED.seller_service_fee, courantee_fee=EXCLUDED.courantee_fee,
            deduction_amount=EXCLUDED.deduction_amount, debt_of_last_week=EXCLUDED.debt_of_last_week,
            final_amount=EXCLUDED.final_amount,
            bank_name=EXCLUDED.bank_name, bank_account=EXCLUDED.bank_account,
            raw_json=EXCLUDED.raw_json
    """

    def __init__(self, db_path: str = None):
        self.engine = get_engine_for_db(db_path)
        self._ensure_table()

    def _ensure_table(self):
        """인덱스 확인"""
        with self.engine.connect() as conn:
            for idx_sql in self.CREATE_INDEXES_SQL:
                try:
                    conn.execute(text(idx_sql))
                except Exception:
                    pass
            conn.commit()
        logger.info("settlement_history 테이블 확인 완료")

    def _get_accounts(self, account_name: str = None) -> list:
        """WING API 활성화된 계정 목록 조회"""
        return get_accounts(self.engine, account_name)

    def _create_client(self, account: dict) -> CoupangWingClient:
        """계정 정보로 WING 클라이언트 생성"""
        return create_wing_client(account)

    @staticmethod
    def _generate_month_list(months_back: int) -> List[str]:
        """최근 N개월 YYYY-MM 리스트 생성"""
        today = date.today()
        months = []
        for i in range(months_back):
            # i=0 → 이번달, i=1 → 지난달, ...
            year = today.year
            month = today.month - i
            while month <= 0:
                month += 12
                year -= 1
            months.append(f"{year:04d}-{month:02d}")
        return months

    def _safe_int(self, val) -> int:
        """안전한 정수 변환"""
        if val is None:
            return 0
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return 0

    def sync_account(self, account: dict, month_list: List[str],
                     progress_callback: Callable = None) -> dict:
        """
        계정 1개의 정산 동기화

        Returns:
            {"account": str, "fetched": int, "upserted": int}
        """
        account_id = account["id"]
        account_name = account["account_name"]
        client = self._create_client(account)

        logger.info(f"[{account_name}] 정산 동기화 시작: {len(month_list)}개월")

        total_fetched = 0
        total_upserted = 0

        for mi, ym in enumerate(month_list):
            logger.info(f"  [{account_name}] {ym} 조회 중...")

            try:
                items = client.get_settlement_history(ym)
            except CoupangWingError as e:
                logger.error(f"  [{account_name}] {ym} API 오류: {e}")
                continue

            if not items:
                logger.info(f"  [{account_name}] {ym} 데이터 없음")
                continue

            total_fetched += len(items)

            with self.engine.connect() as conn:
                for item in items:
                    s_type = item.get("settlementType", "")
                    s_date = item.get("settlementDate", "")

                    try:
                        conn.execute(text(self.UPSERT_SQL), {
                            "account_id": account_id,
                            "year_month": ym,
                            "settlement_type": s_type,
                            "settlement_date": s_date,
                            "settlement_status": item.get("status", ""),
                            "revenue_date_from": item.get("revenueDateFrom", ""),
                            "revenue_date_to": item.get("revenueDateTo", ""),
                            "total_sale": self._safe_int(item.get("totalSale")),
                            "service_fee": self._safe_int(item.get("serviceFee")),
                            "settlement_target_amount": self._safe_int(item.get("settlementTargetAmount")),
                            "settlement_amount": self._safe_int(item.get("settlementAmount")),
                            "last_amount": self._safe_int(item.get("lastAmount")),
                            "pending_released_amount": self._safe_int(item.get("pendingReleasedAmount")),
                            "seller_discount_coupon": self._safe_int(item.get("sellerDiscountCoupon")),
                            "downloadable_coupon": self._safe_int(item.get("downloadableCoupon")),
                            "seller_service_fee": self._safe_int(item.get("sellerServiceFee")),
                            "courantee_fee": self._safe_int(item.get("couranteeFee")),
                            "deduction_amount": self._safe_int(item.get("deductionAmount")),
                            "debt_of_last_week": self._safe_int(item.get("debtOfLastWeek")),
                            "final_amount": self._safe_int(item.get("finalAmount")),
                            "bank_name": item.get("bankName", ""),
                            "bank_account": item.get("bankAccount", ""),
                            "raw_json": json.dumps(item, ensure_ascii=False),
                        })
                        total_upserted += 1
                    except Exception as e:
                        logger.debug(f"  INSERT 스킵: {e}")

                conn.commit()

            if progress_callback:
                progress_callback(mi + 1, len(month_list),
                                  f"[{account_name}] {ym} 완료 ({total_fetched}건)")

        result = {
            "account": account_name,
            "fetched": total_fetched,
            "upserted": total_upserted,
        }
        logger.info(f"[{account_name}] 완료: 조회 {total_fetched}건, 저장 {total_upserted}건")
        return result

    def sync_all(self, months: int = 6, account_name: str = None,
                 progress_callback: Callable = None) -> List[dict]:
        """
        전체 계정 정산 동기화

        Args:
            months: 동기화 기간 (개월)
            account_name: 특정 계정만 (None=전체)
            progress_callback: 진행 콜백 (current, total, message)

        Returns:
            계정별 결과 리스트
        """
        accounts = self._get_accounts(account_name)
        if not accounts:
            logger.warning("WING API 활성화된 계정이 없습니다.")
            return []

        month_list = self._generate_month_list(months)
        logger.info(f"정산 동기화: {len(accounts)}개 계정, {month_list[-1]} ~ {month_list[0]}")

        results = []
        for i, account in enumerate(accounts):
            if progress_callback:
                progress_callback(i, len(accounts),
                                  f"{account['account_name']} 동기화 중...")

            result = self.sync_account(account, month_list, progress_callback)
            results.append(result)

        if progress_callback:
            progress_callback(len(accounts), len(accounts), "동기화 완료!")

        total_f = sum(r["fetched"] for r in results)
        total_u = sum(r["upserted"] for r in results)
        logger.info(f"전체 완료: {len(accounts)}개 계정, 조회 {total_f}건, 저장 {total_u}건")

        return results


def main():
    parser = argparse.ArgumentParser(description="정산 내역 동기화")
    parser.add_argument("--months", type=int, default=6, help="동기화 기간 (개월, 기본 6)")
    parser.add_argument("--account", type=str, default=None, help="특정 계정명 (기본: 전체)")
    args = parser.parse_args()

    syncer = SettlementSync()
    results = syncer.sync_all(months=args.months, account_name=args.account)

    # 리포트
    print("\n" + "=" * 60)
    print("정산 동기화 결과")
    print("=" * 60)
    for r in results:
        print(f"  {r['account']:12s} | 조회 {r['fetched']:5d} | 저장 {r['upserted']:5d}")
    print("=" * 60)

    # DB 확인
    eng = get_engine_for_db()
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM settlement_history")).scalar()
        print(f"\nsettlement_history 총 레코드: {cnt:,}건")


if __name__ == "__main__":
    main()
