"""
트랜잭션 관리 모듈
==================
원자적 작업 보장, 배치 처리, 에러 수집

사용법:
    with atomic_operation(engine) as conn:
        conn.execute(text("INSERT ..."), params)

    processor = BatchProcessor(engine)
    results = processor.process_batch(items, process_func)
"""
import logging
from contextlib import contextmanager
from typing import List, Dict, Callable, Any, Optional, TypeVar, Generic

from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

logger = logging.getLogger(__name__)

T = TypeVar('T')


@contextmanager
def atomic_operation(engine: Engine, timeout: int = 30):
    """
    원자적 작업을 보장하는 컨텍스트 매니저

    성공 시 자동 커밋, 실패 시 자동 롤백

    Args:
        engine: SQLAlchemy 엔진
        timeout: 트랜잭션 타임아웃 (초)

    Yields:
        Connection: 데이터베이스 연결

    Raises:
        SQLAlchemyError: 데이터베이스 오류 시 (롤백 후)

    Usage:
        with atomic_operation(engine) as conn:
            conn.execute(text("INSERT ..."), params)
            conn.execute(text("UPDATE ..."), params)
        # 여기서 자동 커밋 또는 롤백
    """
    conn = engine.connect()
    trans = conn.begin()

    try:
        yield conn
        trans.commit()
        logger.debug("트랜잭션 커밋 완료")
    except IntegrityError as e:
        trans.rollback()
        logger.warning(f"무결성 오류로 롤백: {e}")
        raise
    except SQLAlchemyError as e:
        trans.rollback()
        logger.error(f"DB 오류로 롤백: {e}")
        raise
    except Exception as e:
        trans.rollback()
        logger.error(f"예상치 못한 오류로 롤백: {e}")
        raise
    finally:
        conn.close()


class BatchProcessor(Generic[T]):
    """
    배치 처리 + 에러 수집기

    일부 항목 실패해도 나머지는 처리하고 에러는 별도 수집

    Usage:
        processor = BatchProcessor(engine, batch_size=50)

        def process_item(conn, item):
            conn.execute(text("INSERT ..."), {"item": item})
            return {"id": item.id, "status": "ok"}

        results = processor.process_batch(items, process_item)
        # results: {"success": [...], "failed": [...], "errors": [...]}
    """

    def __init__(
        self,
        engine: Engine,
        batch_size: int = 50,
        continue_on_error: bool = True,
    ):
        """
        Args:
            engine: SQLAlchemy 엔진
            batch_size: 중간 커밋 단위
            continue_on_error: 오류 발생해도 계속 진행할지
        """
        self.engine = engine
        self.batch_size = batch_size
        self.continue_on_error = continue_on_error

    def process_batch(
        self,
        items: List[T],
        process_func: Callable[[Connection, T], Any],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """
        항목들을 배치로 처리

        Args:
            items: 처리할 항목 리스트
            process_func: 항목 처리 함수 (conn, item) -> result
            progress_callback: 진행 콜백 (current, total, message)

        Returns:
            {"success": [results], "failed": [items], "errors": [messages],
             "total": int, "success_count": int, "fail_count": int}
        """
        results = {
            "success": [],
            "failed": [],
            "errors": [],
            "total": len(items),
            "success_count": 0,
            "fail_count": 0,
        }

        if not items:
            return results

        with self.engine.connect() as conn:
            for i, item in enumerate(items):
                try:
                    result = process_func(conn, item)
                    results["success"].append(result)
                    results["success_count"] += 1

                except IntegrityError as e:
                    # 중복 등 무결성 오류 - 보통 무시해도 됨
                    results["failed"].append(item)
                    results["errors"].append(f"IntegrityError: {str(e)[:100]}")
                    results["fail_count"] += 1
                    logger.debug(f"무결성 오류 (스킵): {e}")

                    if not self.continue_on_error:
                        break

                except Exception as e:
                    results["failed"].append(item)
                    results["errors"].append(f"{type(e).__name__}: {str(e)[:100]}")
                    results["fail_count"] += 1
                    logger.warning(f"처리 오류: {e}")

                    if not self.continue_on_error:
                        break

                # 배치 단위 중간 커밋
                if (i + 1) % self.batch_size == 0:
                    conn.commit()
                    if progress_callback:
                        progress_callback(
                            i + 1,
                            len(items),
                            f"처리 중: {i+1}/{len(items)} ({results['success_count']} 성공)"
                        )
                    logger.info(
                        f"  배치 커밋: {i+1}/{len(items)} "
                        f"(성공 {results['success_count']}, 실패 {results['fail_count']})"
                    )

            # 최종 커밋
            conn.commit()

        if progress_callback:
            progress_callback(
                len(items),
                len(items),
                f"완료: {results['success_count']} 성공, {results['fail_count']} 실패"
            )

        return results

    def process_single(
        self,
        item: T,
        process_func: Callable[[Connection, T], Any],
    ) -> Dict[str, Any]:
        """
        단일 항목 처리 (원자적)

        Args:
            item: 처리할 항목
            process_func: 항목 처리 함수

        Returns:
            {"success": bool, "result": Any, "error": Optional[str]}
        """
        try:
            with atomic_operation(self.engine) as conn:
                result = process_func(conn, item)
                return {"success": True, "result": result, "error": None}
        except Exception as e:
            return {"success": False, "result": None, "error": str(e)}
