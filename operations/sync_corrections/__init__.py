"""계정 간 상품 수정사항 동기화 패키지.

하위호환: `from operations.sync_corrections import cmd_sync` 유지.
"""

from .commands import cmd_sync

__all__ = ["cmd_sync"]
