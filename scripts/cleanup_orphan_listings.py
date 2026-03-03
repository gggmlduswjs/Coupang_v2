"""
orphan listings 정리 스크립트
============================
- vendor_item_id IS NULL인 행 중 orders/returns 참조 없는 건 삭제 (16,093건)
- 참조 있는 건 coupang_status = 'deleted'로 변경 (435건)
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from sqlalchemy import text
from core.database import engine

def main():
    with engine.connect() as conn:
        # 현황 확인
        r = conn.execute(text("""
            SELECT COUNT(*) FROM public.listings WHERE vendor_item_id IS NULL
        """))
        total_null = r.fetchone()[0]
        print(f"정리 전 NULL vendor_item_id: {total_null:,}건")

        # 1. 참조 없는 orphan 삭제 (orders, returns, revenue_history 모두 확인)
        r1 = conn.execute(text("""
            DELETE FROM public.listings
            WHERE vendor_item_id IS NULL
              AND id NOT IN (
                  SELECT listing_id FROM orders WHERE listing_id IS NOT NULL
                  UNION ALL
                  SELECT listing_id FROM return_requests WHERE listing_id IS NOT NULL
                  UNION ALL
                  SELECT listing_id FROM revenue_history WHERE listing_id IS NOT NULL
              )
        """))
        conn.commit()
        print(f"삭제 완료: {r1.rowcount:,}건")

        # 2. 참조 있는 orphan → deleted 상태로
        r2 = conn.execute(text("""
            UPDATE public.listings
            SET coupang_status = 'deleted', updated_at = NOW()
            WHERE vendor_item_id IS NULL
              AND coupang_status != 'deleted'
        """))
        conn.commit()
        print(f"상태 변경(deleted): {r2.rowcount:,}건")

        # 3. 결과 확인
        r3 = conn.execute(text("""
            SELECT a.account_code,
                   COUNT(*) as total,
                   COUNT(CASE WHEN l.coupang_status = 'active' THEN 1 END) as active_cnt,
                   COUNT(CASE WHEN l.vendor_item_id IS NOT NULL THEN 1 END) as has_vid
            FROM public.listings l
            JOIN accounts a ON a.id = l.account_id
            GROUP BY a.account_code ORDER BY a.account_code
        """))
        print("\n=== 정리 후 계정별 현황 ===")
        for row in r3:
            print(f"  {row[0]}: 전체={row[1]:,}, active={row[2]:,}, 유효_vid={row[3]:,}")

if __name__ == "__main__":
    main()
