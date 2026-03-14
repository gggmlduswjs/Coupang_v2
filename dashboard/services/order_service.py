"""주문 비즈니스 로직 — UI 독립 함수들.

orders.py에서 추출된 순수 데이터 처리 함수.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text as sa_text

from dashboard.utils import engine
from dashboard.services.order_helpers import (
    parse_dt,
    extract_price,
    extract_order_items,
    build_upsert_params,
    UPSERT_ORDER_SQL,
)

logger = logging.getLogger(__name__)

_IS_LOCAL = sys.platform == "win32"
_HANJIN_CREDS_PATH = Path(__file__).resolve().parents[2] / "hanjin_creds.json"


def load_hanjin_creds() -> dict:
    if _HANJIN_CREDS_PATH.exists():
        try:
            return json.loads(_HANJIN_CREDS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_hanjin_creds(user_id: str, password: str):
    data = {"user_id": user_id, "password": password}
    _HANJIN_CREDS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_ordersheets_to_db(acct, ordersheets, status):
    """WING API 응답 → orders 테이블 UPSERT (직접 호출, match_listing 생략).

    이미 ThreadPoolExecutor 안에서 호출되므로 별도 daemon thread 불필요.
    """
    if not ordersheets:
        return

    account_id = int(acct["id"])
    try:
        with engine.connect() as conn:
            for os_data in ordersheets:
                for item in extract_order_items(os_data):
                    params = build_upsert_params(account_id, status, os_data, item)
                    if not params:
                        continue
                    try:
                        conn.execute(sa_text(UPSERT_ORDER_SQL), params)
                    except Exception as e:
                        logger.warning(
                            f"주문 UPSERT 실패 (box={params.get('shipment_box_id')}, "
                            f"vid={params.get('vendor_item_id')}): {e}"
                        )
                        # deadlock/트랜잭션 깨짐 → rollback 후 계속
                        try:
                            conn.rollback()
                        except Exception:
                            pass
            conn.commit()
    except Exception as e:
        logger.warning(f"주문 DB 저장 오류: {e}")


def update_orders_status_after_invoice(success_items, account_id: int = None):
    """송장 등록 성공 후 DB 주문 상태를 DEPARTURE로 업데이트 (동기 실행).

    Args:
        success_items: list of dict with keys:
            - shipmentBoxId (int)
            - invoiceNumber (str)
            - deliveryCompanyCode (str, e.g. "HANJIN")
        account_id: 계정 ID (지정 시 해당 계정만 업데이트)
    """
    if not success_items:
        return

    try:
        with engine.connect() as conn:
            for item in success_items:
                box_id = item.get("shipmentBoxId")
                invoice = item.get("invoiceNumber", "")
                company = item.get("deliveryCompanyCode", "HANJIN")
                company_name = {"HANJIN": "한진택배"}.get(company, company)
                if not box_id:
                    continue
                sql = """
                    UPDATE orders
                    SET status = 'DEPARTURE',
                        invoice_number = :invoice,
                        delivery_company_name = :company_name,
                        updated_at = :updated_at
                    WHERE shipment_box_id = :box_id
                      AND status = 'INSTRUCT'
                """
                params = {
                    "box_id": int(box_id),
                    "invoice": str(invoice).strip(),
                    "company_name": company_name,
                    "updated_at": datetime.now().isoformat(),
                }
                if account_id is not None:
                    sql += " AND account_id = :account_id"
                    params["account_id"] = account_id
                conn.execute(sa_text(sql), params)
            conn.commit()
    except Exception as e:
        logger.warning(f"송장 등록 후 DB 상태 업데이트 오류: {e}")
