"""
Test: GET /v1/stock_adjustments - 取得所有庫存調整紀錄
Usage: python3 test_stock_adjustments.py
"""
import hmac
import hashlib
import base64
import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Credentials
USERNAME = os.environ["CYBERBIZ_USERNAME"]
SECRET = os.environ["CYBERBIZ_SECRET"]
BASE_URL = os.environ["CYBERBIZ_BASE_URL"]

# Generate auth headers
x_date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
path = '/v1/stock_adjustments'
sig_str = f'x-date: {x_date}\nGET {path} HTTP/1.1'
signature = base64.b64encode(
    hmac.new(SECRET.encode(), sig_str.encode(), hashlib.sha256).digest()
).decode()

headers = {
    'Accept': 'application/json',
    'X-Date': x_date,
    'Authorization': (
        f'hmac username="{USERNAME}", '
        f'algorithm="hmac-sha256", '
        f'headers="x-date request-line", '
        f'signature="{signature}"'
    )
}

# Call API
print(f"=== GET {path} - 取得所有庫存調整紀錄 ===")
resp = requests.get(f"{BASE_URL}{path}", headers=headers, params={'page': 1, 'per_page': 5}, timeout=30)
print(f"Status: {resp.status_code}")
print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
