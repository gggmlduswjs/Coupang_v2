#!/bin/bash
# ============================================================
# Vultr 서버 초기 세팅 스크립트 (Ubuntu 22.04, $6 서울)
# ============================================================
# 사용법: SSH 접속 후
#   bash vultr_setup.sh
# ============================================================

set -e

echo "=============================="
echo "  Vultr 서버 초기 세팅 시작"
echo "  (Ubuntu 22.04 / 1vCPU / 1GB RAM)"
echo "=============================="

# ── 1) 시스템 업데이트 + 필수 패키지 ──
echo "[1/6] 시스템 업데이트 + 패키지 설치..."
apt update && apt upgrade -y
apt install -y python3.11 python3.11-venv python3.11-dev python3-pip git curl jq

# ── 2) UFW 방화벽 ──
echo "[2/6] UFW 방화벽 설정 (SSH + 8501)..."
ufw allow OpenSSH
ufw allow 8501/tcp
ufw --force enable
echo "  -> UFW 활성화: SSH(22), Streamlit(8501)"

# ── 3) 1GB Swap 생성 (RAM 1GB 안전망) ──
echo "[3/6] 1GB Swap 생성..."
if [ ! -f /swapfile ]; then
    fallocate -l 1G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
    # 메모리 부족 시에만 swap 사용
    sysctl vm.swappiness=10
    echo 'vm.swappiness=10' >> /etc/sysctl.conf
    echo "  -> Swap 1GB 생성 완료"
else
    echo "  -> Swap 이미 존재, 건너뜀"
fi

# ── 4) 프로젝트 클론 + venv + pip ──
echo "[4/6] 프로젝트 클론 + 패키지 설치..."
PROJECT_DIR="/home/ubuntu/Coupang_v2"

# ubuntu 사용자 생성 (Vultr는 root로 시작)
if ! id -u ubuntu &>/dev/null; then
    adduser --disabled-password --gecos "" ubuntu
    usermod -aG sudo ubuntu
    echo "ubuntu ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu
    echo "  -> ubuntu 사용자 생성"
fi

# 프로젝트 클론
if [ ! -d "$PROJECT_DIR" ]; then
    sudo -u ubuntu git clone https://github.com/gggmlduswjs/Coupang_v2.git "$PROJECT_DIR"
else
    echo "  -> 이미 클론됨, git pull..."
    sudo -u ubuntu git -C "$PROJECT_DIR" pull
fi

# venv + pip install
sudo -u ubuntu bash -c "
    cd $PROJECT_DIR
    python3.11 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
"
echo "  -> venv + 패키지 설치 완료"

# ── 5) 대화형 .env 생성 ──
echo "[5/6] .env 설정..."
ENV_FILE="$PROJECT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    echo "  -> .env 이미 존재합니다. 덮어쓸까요? (y/N)"
    read -r OVERWRITE
    if [ "$OVERWRITE" != "y" ] && [ "$OVERWRITE" != "Y" ]; then
        echo "  -> .env 유지, 건너뜀"
        SKIP_ENV=true
    fi
fi

if [ "$SKIP_ENV" != "true" ]; then
    echo ""
    echo "=== DATABASE ==="
    read -rp "DATABASE_URL (PostgreSQL): " DB_URL

    echo ""
    echo "=== 쿠팡 API 키 (5개 계정) ==="
    echo "  각 계정의 vendor_id, access_key, secret_key를 입력하세요."
    echo "  건너뛰려면 Enter를 누르세요."
    echo ""

    ACCOUNTS=("007EZ" "002BM" "007BM" "007BOOK" "BIG6CEO")
    ACCOUNT_LABELS=("007-ez" "002-bm" "007-bm" "007-book" "big6ceo")

    declare -A ENV_VARS
    ENV_VARS[DATABASE_URL]="$DB_URL"

    for i in "${!ACCOUNTS[@]}"; do
        ACC="${ACCOUNTS[$i]}"
        LABEL="${ACCOUNT_LABELS[$i]}"
        echo "--- ${LABEL} ---"
        read -rp "  VENDOR_ID: " VID
        read -rp "  ACCESS_KEY: " AK
        read -rp "  SECRET_KEY: " SK
        ENV_VARS["COUPANG_${ACC}_VENDOR_ID"]="$VID"
        ENV_VARS["COUPANG_${ACC}_ACCESS_KEY"]="$AK"
        ENV_VARS["COUPANG_${ACC}_SECRET_KEY"]="$SK"
        echo ""
    done

    echo "=== Telegram 알림 (선택) ==="
    read -rp "TELEGRAM_BOT_TOKEN (없으면 Enter): " TG_TOKEN
    read -rp "TELEGRAM_CHAT_ID (없으면 Enter): " TG_CHAT

    # .env 파일 작성
    cat > "$ENV_FILE" << ENVEOF
# Database
DATABASE_URL=${ENV_VARS[DATABASE_URL]}

# 쿠팡 API - 007-ez
COUPANG_007EZ_VENDOR_ID=${ENV_VARS[COUPANG_007EZ_VENDOR_ID]}
COUPANG_007EZ_ACCESS_KEY=${ENV_VARS[COUPANG_007EZ_ACCESS_KEY]}
COUPANG_007EZ_SECRET_KEY=${ENV_VARS[COUPANG_007EZ_SECRET_KEY]}

# 쿠팡 API - 002-bm
COUPANG_002BM_VENDOR_ID=${ENV_VARS[COUPANG_002BM_VENDOR_ID]}
COUPANG_002BM_ACCESS_KEY=${ENV_VARS[COUPANG_002BM_ACCESS_KEY]}
COUPANG_002BM_SECRET_KEY=${ENV_VARS[COUPANG_002BM_SECRET_KEY]}

# 쿠팡 API - 007-bm
COUPANG_007BM_VENDOR_ID=${ENV_VARS[COUPANG_007BM_VENDOR_ID]}
COUPANG_007BM_ACCESS_KEY=${ENV_VARS[COUPANG_007BM_ACCESS_KEY]}
COUPANG_007BM_SECRET_KEY=${ENV_VARS[COUPANG_007BM_SECRET_KEY]}

# 쿠팡 API - 007-book
COUPANG_007BOOK_VENDOR_ID=${ENV_VARS[COUPANG_007BOOK_VENDOR_ID]}
COUPANG_007BOOK_ACCESS_KEY=${ENV_VARS[COUPANG_007BOOK_ACCESS_KEY]}
COUPANG_007BOOK_SECRET_KEY=${ENV_VARS[COUPANG_007BOOK_SECRET_KEY]}

# 쿠팡 API - big6ceo
COUPANG_BIG6CEO_VENDOR_ID=${ENV_VARS[COUPANG_BIG6CEO_VENDOR_ID]}
COUPANG_BIG6CEO_ACCESS_KEY=${ENV_VARS[COUPANG_BIG6CEO_ACCESS_KEY]}
COUPANG_BIG6CEO_SECRET_KEY=${ENV_VARS[COUPANG_BIG6CEO_SECRET_KEY]}

# Telegram (선택)
TELEGRAM_BOT_TOKEN=${TG_TOKEN}
TELEGRAM_CHAT_ID=${TG_CHAT}
ENVEOF

    chown ubuntu:ubuntu "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "  -> .env 생성 완료 (chmod 600)"
fi

# ── 6) 로그 디렉토리 ──
echo "[6/6] 로그 디렉토리 생성..."
mkdir -p /var/log/coupang
chown ubuntu:ubuntu /var/log/coupang

echo ""
echo "=============================="
echo "  초기 세팅 완료!"
echo ""
echo "  다음 단계: systemd 서비스 등록"
echo "    sudo bash $PROJECT_DIR/scripts/deploy/vultr_services.sh"
echo "=============================="
