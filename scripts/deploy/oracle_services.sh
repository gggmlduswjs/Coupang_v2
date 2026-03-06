#!/bin/bash
# ============================================================
# systemd 서비스 등록 (Streamlit + 주문 동기화)
# 사용법: sudo bash scripts/deploy/oracle_services.sh
# ============================================================

set -e

PROJECT_DIR="/home/ubuntu/Coupang_v2"
VENV_PYTHON="${PROJECT_DIR}/.venv/bin/python"

# ── 1) Streamlit 대시보드 서비스 ──
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

# ── 2) 주문 빠른 동기화 (1분마다: ACCEPT/INSTRUCT) ──
cat > /etc/systemd/system/coupang-sync-quick.service << EOF
[Unit]
Description=Coupang Order Quick Sync

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m scripts.sync.sync_orders --days 1 --quick
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

# ── 3) 주문 전체 동기화 (30분마다) ──
cat > /etc/systemd/system/coupang-sync-full.service << EOF
[Unit]
Description=Coupang Order Full Sync

[Service]
Type=oneshot
User=ubuntu
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_PYTHON} -m scripts.sync.sync_orders --days 7
EOF

cat > /etc/systemd/system/coupang-sync-full.timer << EOF
[Unit]
Description=Coupang Order Full Sync Timer (30분)

[Timer]
OnCalendar=*:00/30:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

# ── 서비스 활성화 ──
systemctl daemon-reload
systemctl enable --now coupang-dashboard
systemctl enable --now coupang-sync-quick.timer
systemctl enable --now coupang-sync-full.timer

echo ""
echo "=============================="
echo "  서비스 등록 완료!"
echo ""
echo "  대시보드: http://$(curl -s ifconfig.me):8501"
echo "  서버 IP:  $(curl -s ifconfig.me)"
echo ""
echo "  이 IP를 쿠팡 셀러센터에 등록하세요!"
echo ""
echo "  상태 확인:"
echo "    systemctl status coupang-dashboard"
echo "    systemctl list-timers --all | grep coupang"
echo "    journalctl -u coupang-sync-quick -f"
echo "=============================="
