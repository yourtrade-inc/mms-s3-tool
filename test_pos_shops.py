"""
Test: GET /v1/pos_shops - 取得所有 POS 商店資訊
      GET /v1/pos_shops/{id}/product_variants - 取得該POS商店的所有商品款式
Usage: python3 test_pos_shops.py
"""
import hmac
import hashlib
import base64
import json
import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# Credentials
USERNAME = os.environ["CYBERBIZ_USERNAME"]
SECRET = os.environ["CYBERBIZ_SECRET"]
BASE_URL = os.environ["CYBERBIZ_BASE_URL"]


def make_headers(method, path):
    x_date = datetime.now(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')
    sig_str = f'x-date: {x_date}\n{method} {path} HTTP/1.1'
    signature = base64.b64encode(
        hmac.new(SECRET.encode(), sig_str.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        'Accept': 'application/json',
        'X-Date': x_date,
        'Authorization': (
            f'hmac username="{USERNAME}", '
            f'algorithm="hmac-sha256", '
            f'headers="x-date request-line", '
            f'signature="{signature}"'
        )
    }


# 1) 取得所有 POS 商店
path = '/v1/pos_shops'
print(f"=== GET {path} ===")
resp = requests.get(f"{BASE_URL}{path}", headers=make_headers('GET', path), params={'page': 1, 'per_page': 5}, timeout=30)
print(f"Status: {resp.status_code}")
data = resp.json()
print(json.dumps(data, indent=2, ensure_ascii=False))

# 2) 取得第一個 POS 商店的所有商品款式
shops = data if isinstance(data, list) else data.get('data') or data.get('pos_shops') or []
if shops:
    shop_id = shops[0].get('id') or shops[0].get('pos_shop_id')
    path2 = f'/v1/pos_shops/{shop_id}/product_variants'
    print(f"\n=== GET {path2} ===")
    resp2 = requests.get(f"{BASE_URL}{path2}", headers=make_headers('GET', path2), params={'page': 1, 'per_page': 50}, timeout=30)
    print(f"Status: {resp2.status_code}")
    print(json.dumps(resp2.json(), indent=2, ensure_ascii=False))
else:
    print("\nNo POS shops found, skipping product_variants call.")
