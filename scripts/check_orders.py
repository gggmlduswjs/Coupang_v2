"""주문 상태별 수량 확인"""
import sys
from pathlib import Path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from core.database import get_engine_for_db
from sqlalchemy import text

engine = get_engine_for_db()
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT a.account_name, o.status, COUNT(DISTINCT o.shipment_box_id) as cnt "
        "FROM orders o JOIN accounts a ON o.account_id = a.id "
        "WHERE o.canceled = false "
        "AND o.status IN ('ACCEPT','INSTRUCT','DEPARTURE','DELIVERING') "
        "GROUP BY a.account_name, o.status ORDER BY a.account_name, o.status"
    )).fetchall()
    for r in rows:
        print(r[0], r[1], r[2])
