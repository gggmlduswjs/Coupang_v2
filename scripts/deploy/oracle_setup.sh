#!/bin/bash
# ============================================================
# Oracle Cloud VM 초기 세팅 스크립트
# ============================================================
# 사용법: SSH 접속 후
#   curl -sSL https://raw.githubusercontent.com/gggmlduswjs/Coupang_v2/main/scripts/deploy/oracle_setup.sh | bash
#   또는: bash oracle_setup.sh
# ============================================================

set -e

echo "=============================="
echo "  Oracle Cloud VM 세팅 시작"
echo "=============================="

# 1) 시스템 업데이트
sudo apt update && sudo apt upgrade -y

# 2) Python 3.11 + pip
sudo apt install -y python3.11 python3.11-venv python3.11-dev python3-pip git

# 3) 프로젝트 클론
cd /home/ubuntu
if [ ! -d "Coupang_v2" ]; then
    git clone https://github.com/gggmlduswjs/Coupang_v2.git
fi
cd Coupang_v2

# 4) 가상환경 + 패키지 설치
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# 5) .env 파일 생성 (수동으로 값 입력 필요)
if [ ! -f ".env" ]; then
    cat > .env << 'ENVEOF'
DATABASE_URL=postgresql://postgres.glivxzmrgypqhtryoglg:0864gmldus!@aws-1-ap-south-1.pooler.supabase.com:6543/postgres
ENVEOF
    echo ".env 파일 생성됨"
fi

# 6) 방화벽: Streamlit 포트 열기
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 8501 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || true

echo ""
echo "=============================="
echo "  기본 설치 완료!"
echo "  다음 단계: systemd 서비스 등록"
echo "  sudo bash scripts/deploy/oracle_services.sh"
echo "=============================="
