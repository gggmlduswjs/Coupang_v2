#!/bin/bash
# ============================================================
# Vultr systemd 서비스 등록 (6개 유닛)
# 사용법: sudo bash scripts/deploy/vultr_services.sh
# ============================================================

set -e

PROJECT_DIR="/home/ubuntu/Coupang_v2"
VENV_PYTHON="${PROJECT_DIR}/.venv/bin/python"
HEALTH_SCRIPT="${PROJECT_DIR}/scripts/deploy/health_check.sh"

echo "=============================="
echo "  systemd 서비스 등록 시작"
echo "=============================="

# ── 1) Streamlit 대시보드 (always-on) ──
cat > /etc/systemd/system/coupang-dashboard.service << EOF
[Unit]
Description=Coupang Dashboard (Streamlit)
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
Restart=always
RestartSec=5
Environment="PATH=${PROJECT_DIR}/.venv/bin:/usr/bin"

[Install]
WantedBy=multi-user.target
EOF
echo "  [1/6] coupang-dashboard.service"

# ── 2) 주문 빠른 동기화 (1분마다) ──
cat > /etc/systemd/system/coupang-sync-quick.service << EOF
[Unit]
Description=Coupang Order Quick Sync (ACCEPT/INSTRUCT)

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m scripts.sync.sync_orders --days 1 --quick
Environment="PATH=${PROJECT_DIR}/.venv/bin:/usr/bin"
EOF

cat > /etc/systemd/system/coupang-sync-quick.timer << EOF
[Unit]
Description=Coupang Order Quick Sync Timer (1분)

[Timer]
OnCalendar=*:*:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
echo "  [2/6] coupang-sync-quick.timer (1분)"

# ── 3) 주문 전체 동기화 (30분, :00 :30) ──
cat > /etc/systemd/system/coupang-sync-full.service << EOF
[Unit]
Description=Coupang Order Full Sync (7일)

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m scripts.sync.sync_orders --days 7
Environment="PATH=${PROJECT_DIR}/.venv/bin:/usr/bin"
EOF

cat > /etc/systemd/system/coupang-sync-full.timer << EOF
[Unit]
Description=Coupang Order Full Sync Timer (30분)

[Timer]
OnCalendar=*:00,30:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
echo "  [3/6] coupang-sync-full.timer (30분, :00 :30)"

# ── 4) 상품 동기화 (30분, :15 :45 — full sync와 15분 오프셋) ──
cat > /etc/systemd/system/coupang-sync-products.service << EOF
[Unit]
Description=Coupang Product Sync

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m scripts.sync.sync_coupang_products --quick
Environment="PATH=${PROJECT_DIR}/.venv/bin:/usr/bin"
EOF

cat > /etc/systemd/system/coupang-sync-products.timer << EOF
[Unit]
Description=Coupang Product Sync Timer (30분, :15 :45)

[Timer]
OnCalendar=*:15,45:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
echo "  [4/6] coupang-sync-products.timer (30분, :15 :45)"

# ── 5) 반품 동기화 (10분마다) ──
cat > /etc/systemd/system/coupang-sync-returns.service << EOF
[Unit]
Description=Coupang Returns Sync

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m scripts.sync.sync_returns --days 7
Environment="PATH=${PROJECT_DIR}/.venv/bin:/usr/bin"
EOF

cat > /etc/systemd/system/coupang-sync-returns.timer << EOF
[Unit]
Description=Coupang Returns Sync Timer (10분)

[Timer]
OnCalendar=*:0/10:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
echo "  [5/6] coupang-sync-returns.timer (10분)"

# ── 6) 헬스 체크 (5분마다) ──
chmod +x "${HEALTH_SCRIPT}"

cat > /etc/systemd/system/coupang-health.service << EOF
[Unit]
Description=Coupang Health Check

[Service]
Type=oneshot
User=root
WorkingDirectory=${PROJECT_DIR}
ExecStart=/bin/bash ${HEALTH_SCRIPT}
EOF

cat > /etc/systemd/system/coupang-health.timer << EOF
[Unit]
Description=Coupang Health Check Timer (5분)

[Timer]
OnCalendar=*:0/5:00
Persistent=true

[Install]
WantedBy=timers.target
EOF
echo "  [6/6] coupang-health.timer (5분)"

# ── 서비스 활성화 ──
echo ""
echo "서비스 활성화 중..."
systemctl daemon-reload

systemctl enable --now coupang-dashboard
systemctl enable --now coupang-sync-quick.timer
systemctl enable --now coupang-sync-full.timer
systemctl enable --now coupang-sync-products.timer
systemctl enable --now coupang-sync-returns.timer
systemctl enable --now coupang-health.timer

# 공용 IP 가져오기
PUBLIC_IP=$(curl -s ifconfig.me 2>/dev/null || echo "확인불가")

echo ""
echo "=============================="
echo "  서비스 등록 완료!"
echo ""
echo "  대시보드: http://${PUBLIC_IP}:8501"
echo "  서버 IP:  ${PUBLIC_IP}"
echo ""
echo "  관리 명령:"
echo "    systemctl status coupang-dashboard"
echo "    systemctl list-timers --all | grep coupang"
echo "    journalctl -u coupang-sync-quick --since '2 min ago'"
echo "    journalctl -u coupang-sync-full --since '1 hour ago'"
echo "    cat /var/log/coupang/health.log"
echo ""
echo "  코드 업데이트:"
echo "    cd ${PROJECT_DIR} && git pull"
echo "    sudo systemctl restart coupang-dashboard"
echo "=============================="
