"""DB 백업/복원"""

import os
import shutil
import sqlite3
from datetime import datetime

from core.config import AnalysisConfig


def create_backup(config: AnalysisConfig = None) -> str:
    """SQLite backup API로 안전하게 DB 백업. 백업 파일 경로 반환."""
    config = config or AnalysisConfig()
    config.ensure_dirs()

    if not os.path.exists(config.db_path):
        print("  DB 파일이 존재하지 않습니다.")
        return ""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(config.backup_dir, f"coupang_{timestamp}.db")

    # SQLite backup API 사용 (WAL 모드에서도 안전)
    src = sqlite3.connect(config.db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
        print(f"  백업 완료: {backup_path}")
    finally:
        dst.close()
        src.close()

    return backup_path


def list_backups(config: AnalysisConfig = None) -> list[dict]:
    """백업 목록 반환 (최신순)."""
    config = config or AnalysisConfig()
    config.ensure_dirs()

    backups = []
    for f in os.listdir(config.backup_dir):
        if f.startswith("coupang_") and f.endswith(".db"):
            path = os.path.join(config.backup_dir, f)
            size = os.path.getsize(path)
            backups.append({
                "filename": f,
                "path": path,
                "size_mb": round(size / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(os.path.getmtime(path)).isoformat(),
            })

    backups.sort(key=lambda x: x["modified"], reverse=True)
    return backups


def restore_backup(identifier: str, config: AnalysisConfig = None) -> bool:
    """백업 복원. identifier는 'latest' 또는 파일명.
    복원 전 현재 DB를 자동 백업."""
    config = config or AnalysisConfig()
    backups = list_backups(config)

    if not backups:
        print("  사용 가능한 백업이 없습니다.")
        return False

    # 복원할 백업 찾기
    if identifier == "latest":
        backup = backups[0]
    else:
        backup = None
        for b in backups:
            if b["filename"] == identifier or identifier in b["filename"]:
                backup = b
                break
        if not backup:
            print(f"  백업을 찾을 수 없습니다: {identifier}")
            return False

    # 현재 DB 자동 백업
    print("  현재 DB 백업 중...")
    pre_restore = create_backup(config)
    if pre_restore:
        print(f"  복원 전 백업: {pre_restore}")

    # 복원
    print(f"  복원 중: {backup['filename']}")
    shutil.copy2(backup["path"], config.db_path)
    print("  복원 완료!")
    return True
