@echo off
cd /d "C:\Users\MSI\Desktop\쿠팡비즈니스"
"C:\Users\MSI\AppData\Local\Programs\Python\Python310\python.exe" -c "
import sys, os
sys.path.insert(0, os.getcwd())
from dotenv import load_dotenv
load_dotenv('.env')
from scripts.sync.sync_coupang_products import main
main()
" >> "C:\Users\MSI\Desktop\쿠팡비즈니스\data\logs\sync_products.log" 2>&1
