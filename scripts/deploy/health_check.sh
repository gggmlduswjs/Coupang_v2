#!/bin/bash
# ============================================================
# Coupang 헬스 체크 스크립트
# venv 독립 — bash + curl만 사용, Python 불필요
# ============================================================

LOG_FILE="/var/log/coupang/health.log"
ENV_FILE="/home/ubuntu/Coupang_v2/.env"
ERROR_COUNT_FILE="/tmp/coupang_sync_errors"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# 로그 디렉토리 확인
mkdir -p /var/log/coupang

log() {
    echo "${TIMESTAMP} [$1] $2" >> "$LOG_FILE"
}

# Telegram 알림 (BOT_TOKEN + CHAT_ID 설정 시)
send_alert() {
    local MSG="$1"
    if [ -f "$ENV_FILE" ]; then
        BOT_TOKEN=$(grep -E '^TELEGRAM_BOT_TOKEN=' "$ENV_FILE" | cut -d'=' -f2-)
        CHAT_ID=$(grep -E '^TELEGRAM_CHAT_ID=' "$ENV_FILE" | cut -d'=' -f2-)
    fi
    if [ -n "$BOT_TOKEN" ] && [ -n "$CHAT_ID" ]; then
        curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
            -d chat_id="${CHAT_ID}" \
            -d text="[Coupang] ${MSG}" \
            -d parse_mode="HTML" > /dev/null 2>&1
    fi
    log "ALERT" "$MSG"
}

ISSUES=0

# ── 1) 대시보드 상태 확인 + 자동 재시작 ──
if ! systemctl is-active --quiet coupang-dashboard; then
    log "WARN" "대시보드 중지됨 -> 재시작 시도"
    systemctl restart coupang-dashboard
    sleep 3
    if systemctl is-active --quiet coupang-dashboard; then
        send_alert "대시보드 중지 감지 -> 자동 재시작 성공"
    else
        send_alert "대시보드 재시작 실패! 수동 확인 필요"
        ISSUES=$((ISSUES + 1))
    fi
else
    # HTTP 응답 확인
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://localhost:8501 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" = "000" ] || [ "$HTTP_CODE" = "502" ]; then
        log "WARN" "대시보드 응답 없음 (HTTP ${HTTP_CODE}) -> 재시작"
        systemctl restart coupang-dashboard
        send_alert "대시보드 응답 없음 (HTTP ${HTTP_CODE}) -> 재시작 시도"
        ISSUES=$((ISSUES + 1))
    fi
fi

# ── 2) 동기화 오류 감지 (최근 로그에서 에러 카운트) ──
SYNC_SERVICES=("coupang-sync-quick" "coupang-sync-full" "coupang-sync-products" "coupang-sync-returns")

for SVC in "${SYNC_SERVICES[@]}"; do
    # 최근 10분 로그에서 에러/실패 카운트
    ERROR_LINES=$(journalctl -u "$SVC" --since "10 min ago" --no-pager 2>/dev/null | grep -ciE "(error|exception|traceback|failed)" || true)

    if [ "$ERROR_LINES" -gt 0 ]; then
        # 누적 에러 카운트 관리
        COUNT_FILE="${ERROR_COUNT_FILE}_${SVC}"
        PREV_COUNT=0
        if [ -f "$COUNT_FILE" ]; then
            PREV_COUNT=$(cat "$COUNT_FILE" 2>/dev/null || echo 0)
        fi
        NEW_COUNT=$((PREV_COUNT + 1))
        echo "$NEW_COUNT" > "$COUNT_FILE"

        if [ "$NEW_COUNT" -ge 3 ]; then
            send_alert "${SVC} 연속 오류 ${NEW_COUNT}회! 확인 필요"
            ISSUES=$((ISSUES + 1))
        else
            log "WARN" "${SVC} 오류 감지 (${NEW_COUNT}/3)"
        fi
    else
        # 정상이면 카운트 리셋
        COUNT_FILE="${ERROR_COUNT_FILE}_${SVC}"
        if [ -f "$COUNT_FILE" ]; then
            rm -f "$COUNT_FILE"
        fi
    fi
done

# ── 3) 타이머 미작동 감지 ──
TIMERS=("coupang-sync-quick" "coupang-sync-full" "coupang-sync-products" "coupang-sync-returns" "coupang-health")

for TMR in "${TIMERS[@]}"; do
    if ! systemctl is-active --quiet "${TMR}.timer"; then
        log "WARN" "${TMR}.timer 비활성 -> 재활성화"
        systemctl enable --now "${TMR}.timer" 2>/dev/null
        send_alert "${TMR}.timer 비활성 감지 -> 재활성화 시도"
        ISSUES=$((ISSUES + 1))
    fi
done

# ── 4) 디스크 사용량 확인 (90% 초과) ──
DISK_USAGE=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
if [ "$DISK_USAGE" -gt 90 ]; then
    send_alert "디스크 사용량 ${DISK_USAGE}%! 정리 필요"
    ISSUES=$((ISSUES + 1))
    log "WARN" "디스크 사용량 ${DISK_USAGE}%"
fi

# ── 5) 메모리 확인 (swap 사용 500MB 초과) ──
SWAP_USED=$(free -m | awk '/Swap:/ {print $3}')
if [ "$SWAP_USED" -gt 500 ]; then
    log "WARN" "Swap 사용량 ${SWAP_USED}MB (500MB 초과)"
    send_alert "메모리 부족! Swap ${SWAP_USED}MB 사용 중"
    ISSUES=$((ISSUES + 1))
fi

# ── 결과 로깅 ──
if [ "$ISSUES" -eq 0 ]; then
    log "OK" "정상 (disk:${DISK_USAGE}% swap:${SWAP_USED}MB)"
fi

# 로그 파일 크기 관리 (1만 줄 초과 시 최근 5천 줄만 유지)
if [ -f "$LOG_FILE" ]; then
    LINE_COUNT=$(wc -l < "$LOG_FILE")
    if [ "$LINE_COUNT" -gt 10000 ]; then
        tail -5000 "$LOG_FILE" > "${LOG_FILE}.tmp"
        mv "${LOG_FILE}.tmp" "$LOG_FILE"
    fi
fi
