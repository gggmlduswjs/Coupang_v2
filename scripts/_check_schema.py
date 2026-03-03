import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv(Path(__file__).parent.parent / '.env')
from sqlalchemy import text
from dashboard.utils import engine
with engine.connect() as conn:
    cols = conn.execute(text("""
        SELECT table_schema, column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'listings'
        ORDER BY table_schema, ordinal_position
    """)).fetchall()
    cur_schema = None
    for schema, col, dtype in cols:
        if schema != cur_schema:
            print(f'\n=== schema: {schema} ===')
            cur_schema = schema
        print(f'  {col}: {dtype}')
