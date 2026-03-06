"""Obsidian 실시간 개발 로거"""
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import json


class ObsidianLogger:
    """
    개발 과정을 실시간으로 Obsidian에 기록하는 로거

    사용법:
        logger = ObsidianLogger()
        logger.log_feature("마진 계산기", "구현 완료", tags=["feature", "calculator"])
        logger.log_decision("DB 스키마 V2", "이유...", "결정...")
    """

    def __init__(self, vault_path: str = None):
        """
        Args:
            vault_path: Obsidian vault 경로 (기본: .env의 OBSIDIAN_VAULT_PATH 또는 obsidian_vault)
        """
        if vault_path:
            self.vault = Path(vault_path)
        else:
            # .env의 OBSIDIAN_VAULT_PATH 사용 (G:에 직접 저장)
            gdrive = self._load_gdrive_path()
            if gdrive:
                self.vault = gdrive
            else:
                self.vault = Path(__file__).parent.parent / "obsidian_vault" / "10. project" / "Coupong"

        # vault 없으면 기록 스킵 (G: 미연결 시)
        self.dirs = {
            "index": self.vault / "00-Index",
            "daily": self.vault / "01-Daily",
            "features": self.vault / "02-Features",
            "technical": self.vault / "03-Technical",
            "decisions": self.vault / "04-Decisions",
            "tasks": self.vault / "05-Tasks",
            "database": self.vault / "06-Database",
        }

    def _load_gdrive_path(self) -> Path | None:
        """.env에서 OBSIDIAN_VAULT_PATH 로드"""
        env_path = Path(__file__).parent.parent / ".env"
        if not env_path.exists():
            return None
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("OBSIDIAN_VAULT_PATH=") and "=" in line:
                val = line.split("=", 1)[1].strip().strip('"').strip("'")
                if val:
                    return Path(val) / "10. project" / "Coupong"
        return None

    def _ensure_vault(self) -> bool:
        """vault 존재 시 디렉터리 생성. 없으면 False (sync from 미실행)"""
        if not self.vault.exists():
            return False
        for dir_path in self.dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        return True

    def get_daily_note_path(self) -> Path:
        """오늘의 일일 노트 경로"""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.dirs["daily"] / f"{today}.md"

    def log_to_daily(self, content: str, title: str = None):
        """
        일일 노트에 로그 추가

        Args:
            content: 로그 내용
            title: 제목 (없으면 시간)
        """
        if not self._ensure_vault():
            return
        daily_note = self.get_daily_note_path()
        now = datetime.now()

        # 파일이 없으면 헤더 생성
        if not daily_note.exists():
            date_str = now.strftime("%Y년 %m월 %d일")
            header = f"""# {date_str} 개발 로그

## 📊 오늘의 작업

---

"""
            daily_note.write_text(header, encoding='utf-8')

        # 로그 추가
        time_str = now.strftime("%H:%M")
        if title:
            log_entry = f"\n## {time_str} - {title}\n\n{content}\n\n---\n"
        else:
            log_entry = f"\n### {time_str}\n\n{content}\n\n"

        with open(daily_note, 'a', encoding='utf-8') as f:
            f.write(log_entry)

        print(f"[Obsidian] 로그 추가: {daily_note.name}")

    def log_feature(self, feature_name: str, description: str, tags: List[str] = None, status: str = "진행중"):
        """
        기능 노트 생성/업데이트

        Args:
            feature_name: 기능 이름
            description: 설명
            tags: 태그 리스트
            status: 상태 (진행중/완료/대기)
        """
        if not self._ensure_vault():
            return
        # 파일명 생성 (공백 제거)
        filename = feature_name.replace(" ", "-")
        feature_path = self.dirs["features"] / f"{filename}.md"

        # 태그 포맷
        tag_str = " ".join([f"#{tag}" for tag in (tags or ['feature'])])

        content = f"""# {feature_name}

{tag_str}

**상태:** {status}
**작성일:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

## 개요

{description}

## 구현 내역

- 작성 중...

## 관련 파일

- 추가 예정

## 관련 노트

- [[Index]]
- [[Development Timeline]]

---

**마지막 업데이트:** {datetime.now().strftime("%Y-%m-%d %H:%M")}
"""

        feature_path.write_text(content, encoding='utf-8')

        # 일일 노트에도 기록
        self.log_to_daily(
            f"**{feature_name}** 작업\n- 상태: {status}\n- {description}",
            f"Feature: {feature_name}"
        )

        print(f"[Obsidian] 기능 노트 생성: {feature_path.name}")

    def log_decision(self, decision_title: str, context: str, decision: str, alternatives: List[str] = None):
        """
        의사결정 로그

        Args:
            decision_title: 결정 제목
            context: 배경
            decision: 결정 내용
            alternatives: 고려한 대안들
        """
        if not self._ensure_vault():
            return
        filename = decision_title.replace(" ", "-")
        decision_path = self.dirs["decisions"] / f"{filename}.md"

        alternatives_str = ""
        if alternatives:
            alternatives_str = "\n## 고려한 대안\n\n"
            for i, alt in enumerate(alternatives, 1):
                alternatives_str += f"{i}. {alt}\n"

        content = f"""# {decision_title}

#decision

**결정일:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

## 배경

{context}

## 결정 사항

{decision}
{alternatives_str}

## 영향

- 추가 예정

## 관련 노트

- [[Index]]
- [[Development Timeline]]
"""

        decision_path.write_text(content, encoding='utf-8')

        # 일일 노트에도 기록
        self.log_to_daily(
            f"**결정:** {decision_title}\n\n{decision}",
            f"Decision: {decision_title}"
        )

        print(f"[Obsidian] 의사결정 로그: {decision_path.name}")

    def log_technical(self, tech_name: str, content: str, tags: List[str] = None):
        """
        기술 문서 작성

        Args:
            tech_name: 기술 이름
            content: 내용
            tags: 태그
        """
        if not self._ensure_vault():
            return
        filename = tech_name.replace(" ", "-")
        tech_path = self.dirs["technical"] / f"{filename}.md"

        tag_str = " ".join([f"#{tag}" for tag in (tags or ['technical'])])

        doc = f"""# {tech_name}

{tag_str}

**작성일:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

{content}

---

## 관련 노트

- [[Index]]
- [[Tech Stack]]
"""

        tech_path.write_text(doc, encoding='utf-8')

        self.log_to_daily(f"**기술 문서 작성:** {tech_name}", f"Tech: {tech_name}")

        print(f"[Obsidian] 기술 문서: {tech_path.name}")

    def log_bug(self, bug_title: str, description: str, solution: str = None):
        """
        버그 로그

        Args:
            bug_title: 버그 제목
            description: 설명
            solution: 해결 방법
        """
        if not self._ensure_vault():
            return
        solution_str = ""
        if solution:
            solution_str = f"\n## 해결 방법\n\n{solution}\n"

        content = f"""**Bug:** {bug_title}

{description}
{solution_str}
"""
        self.log_to_daily(content, f"🐛 Bug: {bug_title}")

    def create_index(self):
        """메인 인덱스 페이지 생성"""
        index_path = self.dirs["index"] / "Index.md"

        content = f"""# 쿠팡 도서 판매 자동화 시스템

**마지막 업데이트:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

## 🎯 프로젝트 개요

알라딘 API 기반 도서 검색 → 마진 분석 → 묶음 SKU 생성 → 쿠팡 자동 업로드

## 📊 개발 현황

### 완료된 기능 ✅

- [[Database Schema V2]] - 데이터베이스 스키마
- [[연도 추출]] - 도서 제목에서 연도 자동 추출
- [[마진 계산기]] - 출판사별 수익성 자동 판단
- [[묶음 SKU 생성기]] - 저마진 도서 자동 묶음

### 진행 중 🚧

- 추가 예정

### 대기 중 ⏳

- 판매 분석 엔진
- Streamlit 대시보드
- 자동 업로드

## 🔗 주요 링크

- [[Development Timeline]] - 개발 타임라인
- [[Tech Stack]] - 기술 스택
- [[Database Schema V2]] - DB 스키마

## 📝 최근 일일 노트

- [[{datetime.now().strftime("%Y-%m-%d")}]] - 오늘

## 🏷️ 태그

#project #automation #coupang #books
"""

        index_path.write_text(content, encoding='utf-8')
        print(f"[Obsidian] 인덱스 생성: {index_path.name}")

    def create_development_timeline(self):
        """개발 타임라인 생성"""
        timeline_path = self.dirs["index"] / "Development-Timeline.md"

        content = f"""# 개발 타임라인

**마지막 업데이트:** {datetime.now().strftime("%Y-%m-%d %H:%M")}

---

## 2026-02-05

### ✅ 완료
- Database V2 스키마 설계 및 구현
- 24개 출판사 데이터 초기화
- 연도 추출 기능 구현 (87% 성공률)
- 마진 계산기 구현
- 묶음 SKU 생성기 구현
- 스마트 업로드 시스템 통합

### 📊 성과
- 테이블 8개 생성
- 모델 5개 신규/개선
- 분석기 2개 구현
- 통합 워크플로우 완성

---

## 다음 계획

- [ ] 판매 분석 엔진
- [ ] Streamlit 대시보드
- [ ] 주간 리포트 자동 생성
- [ ] Playwright 자동 업로드

---

## 관련 노트

- [[Index]]
- [[Tech Stack]]
"""

        timeline_path.write_text(content, encoding='utf-8')
        print(f"[Obsidian] 타임라인 생성: {timeline_path.name}")


def init_obsidian_vault():
    """Obsidian Vault 초기화"""
    print("\n" + "="*60)
    print("Obsidian Vault 초기화")
    print("="*60)

    logger = ObsidianLogger()

    # 메인 페이지들 생성
    logger.create_index()
    logger.create_development_timeline()

    # 오늘의 첫 로그
    logger.log_to_daily(
        "Obsidian 연동 시작! 🚀\n\n이제부터 모든 개발 과정이 실시간으로 기록됩니다.",
        "시스템 시작"
    )

    # 주요 기능 문서화
    logger.log_feature(
        "Database Schema V2",
        "8개 테이블, 중복 방지 제약조건, 마진/배송 정책 필드",
        tags=["database", "feature"],
        status="완료"
    )

    logger.log_feature(
        "연도 추출",
        "도서 제목에서 연도 자동 추출 (2024, 2025, 24년 등)\n정규식 기반, 87% 성공률",
        tags=["feature", "parser"],
        status="완료"
    )

    logger.log_feature(
        "마진 계산기",
        "출판사별 공급률 기반 수익성 자동 판단\n배송 정책 자동 결정",
        tags=["feature", "calculator"],
        status="완료"
    )

    logger.log_technical(
        "Tech Stack",
        """## Backend
- Python 3.10+
- SQLAlchemy (ORM)
- SQLite (Database)
- Pydantic (Settings)

## API
- 알라딘 Open API

## 분석
- 마진 계산기
- 묶음 SKU 생성기

## 개발 도구
- Obsidian (문서화)
- Git (버전 관리)
""",
        tags=["technical", "stack"]
    )

    print("\n" + "="*60)
    print("Obsidian Vault 초기화 완료!")
    print("="*60)
    print(f"\nVault 위치: {logger.vault}")
    print("\nObsidian에서 이 폴더를 vault로 열어주세요:")
    print(f"  {logger.vault.absolute()}")


if __name__ == "__main__":
    init_obsidian_vault()
