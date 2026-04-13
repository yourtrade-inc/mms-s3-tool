"""
在庫実績ファイル（POS）- Stock Daily
GET /v1/pos_shops → GET /v1/pos_shops/{id}/product_variants → CSV → S3 upload
Usage: python3 test_stock_daily.py
"""
import hmac
import hashlib
import base64
import csv
import io
import json
import os
import requests
import boto3
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ── Cyberbiz credentials ──
USERNAME = os.environ["CYBERBIZ_USERNAME"]
SECRET = os.environ["CYBERBIZ_SECRET"]
BASE_URL = os.environ["CYBERBIZ_BASE_URL"]

# ── AWS credentials ──
AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_REGION = os.environ["S3_REGION"]


def cyberbiz_headers(method, path):
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


def fetch_pos_shops():
    """Fetch all POS shops."""
    path = '/v1/pos_shops'
    headers = cyberbiz_headers('GET', path)
    resp = requests.get(
        f"{BASE_URL}{path}", headers=headers,
        params={'page': 1, 'per_page': 50}, timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all_product_variants(shop_id):
    """Paginate through all product variants for a POS shop."""
    all_variants = []
    page = 1
    per_page = 50
    while True:
        path = f'/v1/pos_shops/{shop_id}/product_variants'
        headers = cyberbiz_headers('GET', path)
        resp = requests.get(
            f"{BASE_URL}{path}",
            headers=headers,
            params={'page': page, 'per_page': per_page},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data:
            break
        all_variants.extend(data)
        if page % 50 == 0 or len(data) < per_page:
            print(f"    Page {page}: {len(all_variants)} variants so far...")
        if len(data) < per_page:
            break
        page += 1
    return all_variants


def format_datetime(dt_str):
    """'2025-06-23 22:17:15' → '2025/6/23 22:17:15'"""
    if not dt_str:
        return ''
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return f"{dt.year}/{dt.month}/{dt.day} {dt.strftime('%H:%M:%S')}"


def build_csv(variants):
    """Build CSV from product variants as stock snapshot (在庫実績).
    This is a stock record: stock_qty has value, in_qty and out_qty are empty.
    Per spec: when all three quantities are zero, treat as stock record.
    """
    buf = io.StringIO()
    writer = csv.writer(buf)

    # Header (field names per spec)
    writer.writerow([
        'processed_at',
        'sku_cd',
        'stock_cd',
        'rank_cd',
        'good_ng',
        'stock_qty',
        'in_qty',
        'out_qty',
        'expiration_date',
    ])

    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    for v in variants:
        # good_ng: "01"=良品, "02"=不良品
        qc = v.get('qc')
        if qc and str(qc).strip().lower() in ('ng', 'defective', '02', '2'):
            good_ng = '02'
        else:
            good_ng = '01'  # default: 良品

        writer.writerow([
            format_datetime(v.get('updated_at', '')) or now_str,  # processed_at
            v.get('sku', ''),                                      # sku_cd
            '',                                                    # stock_cd (not in API)
            '',                                                    # rank_cd (not in API)
            good_ng,                                               # good_ng: 01=良品/02=不良品
            v.get('inventory_quantity', 0),                         # stock_qty
            '',                                                    # in_qty (N/A for stock snapshot)
            '',                                                    # out_qty (N/A for stock snapshot)
            '',                                                    # expiration_date (not in API)
        ])

    return buf.getvalue()


def build_csv_for_shop(shop_id, variants):
    """Build CSV for a specific POS shop.

    Per confirmed spec: `stock_cd` should be the POS shop id (`pos_shop_id`).
    """
    buf = io.StringIO()
    writer = csv.writer(buf)

    writer.writerow([
        'processed_at',
        'sku_cd',
        'stock_cd',
        'rank_cd',
        'good_ng',
        'stock_qty',
        'in_qty',
        'out_qty',
        'expiration_date',
    ])

    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    for v in variants:
        qc = v.get('qc')
        if qc and str(qc).strip().lower() in ('ng', 'defective', '02', '2'):
            good_ng = '02'
        else:
            good_ng = '01'

        writer.writerow([
            format_datetime(v.get('updated_at', '')) or now_str,
            v.get('sku', ''),
            shop_id,                 # stock_cd = pos_shop_id
            '',
            good_ng,
            v.get('inventory_quantity', 0),
            '',
            '',
            '',
        ])

    return buf.getvalue()


def upload_to_s3(csv_content):
    """Upload CSV to S3 with timestamped key."""
    now = datetime.now()
    ts = now.strftime('%Y%m%d%H%M%S') + f"{now.microsecond // 1000:03d}"
    s3_key = f"stock-files-after/P_stockdaily_{ts}.csv"

    s3 = boto3.client(
        's3',
        region_name=S3_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_content.encode('utf-8-sig'),  # BOM for Excel compatibility
        ContentType='text/csv',
    )
    print(f"  Uploaded to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


# ── Main ──
if __name__ == '__main__':
    print("=== Fetching POS shops ===")
    shops = fetch_pos_shops()
    print(f"Found {len(shops)} shop(s): {[s['name'] for s in shops]}")

    all_csv = []
    for shop in shops:
        shop_id = shop['id']
        print(f"\n=== Fetching product variants for shop {shop_id} ({shop['name']}) ===")
        variants = fetch_all_product_variants(shop_id)
        print(f"  Total variants: {len(variants)}")
        all_csv.append(build_csv_for_shop(shop_id, variants))

    print(f"\n=== Building CSV (在庫実績) ===")
    header, *parts = ''.join(all_csv).splitlines(True)
    csv_content = header + ''.join(parts)
    row_count = csv_content.count('\n') - 1  # minus header
    print(f"CSV rows: {row_count}")

    # Preview first few lines
    lines = csv_content.split('\n')
    for line in lines[:5]:
        print(f"  {line}")
    if len(lines) > 5:
        print(f"  ... ({len(lines) - 5} more lines)")

    print("\n=== Uploading to S3 ===")
    upload_to_s3(csv_content)
    print("\nDone!")
