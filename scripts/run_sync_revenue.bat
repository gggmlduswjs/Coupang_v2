@echo off
cd /d "C:\Users\MSI\Desktop\쿠팡비즈니스"
"C:\Users\MSI\AppData\Local\Programs\Python\Python310\python.exe" -c "
import sys, os
sys.path.insert(0, os.getcwd())
from dotenv import load_dotenv
load_dotenv('.env')
from scripts.sync.sync_revenue import RevenueSync
syncer = RevenueSync()
results = syncer.sync_all(months=1)
total_f = sum(r.get('fetched', 0) for r in results)
total_i = sum(r.get('inserted', 0) for r in results)
print(f'매출 동기화: {len(results)}계정, 조회 {total_f}건, 저장 {total_i}건')
" >> "C:\Users\MSI\Desktop\쿠팡비즈니스\data\logs\sync_revenue.log" 2>&1
