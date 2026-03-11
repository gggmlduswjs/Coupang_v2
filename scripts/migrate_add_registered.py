"""delivery_list_logs 테이블에 registered 컬럼 추가.

기존 DB에 registered 컬럼이 없으면 추가하고 기본값 FALSE로 설정.

Usage:
    python scripts/migrate_add_registered.py
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
            WHERE table_name = 'delivery_list_logs' AND column_name = 'registered'
        """))
        if result.fetchone():
            print("registered 컬럼이 이미 존재합니다.")
        else:
            conn.execute(text(
                "ALTER TABLE delivery_list_logs ADD COLUMN registered BOOLEAN NOT NULL DEFAULT FALSE"
            ))
            print("registered 컬럼 추가 완료.")

        conn.commit()
    print("마이그레이션 완료.")


if __name__ == "__main__":
    migrate()
