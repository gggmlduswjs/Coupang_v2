"""delivery_list_logs 테이블에 batch_id 컬럼 + 인덱스 추가.

Usage:
    python scripts/migrate_add_batch_id.py
"""
import sys
sys.path.insert(0, ".")

from sqlalchemy import text
from core.database import engine


def migrate():
    with engine.connect() as conn:
        # 컬럼 존재 여부 확인
        result = conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'delivery_list_logs' AND column_name = 'batch_id'
        """))
        if result.fetchone():
            print("batch_id 컬럼이 이미 존재합니다.")
        else:
            conn.execute(text(
                "ALTER TABLE delivery_list_logs ADD COLUMN batch_id VARCHAR(36)"
            ))
            print("batch_id 컬럼 추가 완료.")

        # 인덱스 존재 여부 확인
        result = conn.execute(text("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'delivery_list_logs' AND indexname = 'ix_dllog_batch'
        """))
        if result.fetchone():
            print("ix_dllog_batch 인덱스가 이미 존재합니다.")
        else:
            conn.execute(text(
                "CREATE INDEX ix_dllog_batch ON delivery_list_logs (batch_id)"
            ))
            print("ix_dllog_batch 인덱스 생성 완료.")

        conn.commit()
    print("마이그레이션 완료.")


if __name__ == "__main__":
    migrate()
