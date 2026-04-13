"""
GET /v1/stock_receipts - 取得所有進倉單 → CSV → S3 upload
Usage: python3 test_receiving_invoices.py
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


def fetch_all_stock_receipts():
    """Paginate through all stock receipts."""
    all_items = []
    page = 1
    per_page = 50
    while True:
        path = '/v1/stock_receipts'
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
        all_items.extend(data)
        print(f"  Page {page}: fetched {len(data)} receipts")
        if len(data) < per_page:
            break
        page += 1
    return all_items


def format_datetime(dt_str):
    """'2025-06-23 22:17:15' → '2025/6/23 22:17:15'"""
    if not dt_str:
        return ''
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return f"{dt.year}/{dt.month}/{dt.day} {dt.strftime('%H:%M:%S')}"


def build_csv(receipts):
    """Build CSV string from stock receipts. One row per item in each receipt."""
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

    for receipt in receipts:
        # Skip canceled receipts
        if receipt.get('status') == 'canceled':
            continue

        processed_at = format_datetime(receipt.get('created_at', ''))
        for item in receipt.get('items', []):
            # good_ng: "01"=良品, "02"=不良品
            qc = item.get('qc')
            if qc and str(qc).strip().lower() in ('ng', 'defective', '02', '2'):
                good_ng = '02'
            else:
                good_ng = '01'  # default: 良品

            writer.writerow([
                processed_at,                          # processed_at
                item.get('sku', ''),                   # sku_cd
                receipt.get('pos_shop_id'),                           # stock_cd = pos_shop_id (POS商店ID)
                '',                                    # rank_cd (not in API)
                good_ng,                               # good_ng: 01=良品/02=不良品
                '',                                    # stock_qty (not in API)
                item.get('quantity', 0),                # in_qty
                '',                                    # out_qty (N/A for inbound)
                '',                                    # expiration_date (not in API)
            ])

    return buf.getvalue()


def upload_to_s3(csv_content):
    """Upload CSV to S3 with timestamped key."""
    now = datetime.now()
    ts = now.strftime('%Y%m%d%H%M%S') + f"{now.microsecond // 1000:03d}"
    s3_key = f"stock-files-after/P_inboundReceiptDaily_{ts}.csv"

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

    done_key = f"{s3_key}.done"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=done_key,
        Body=b'',
        ContentType='text/plain',
    )
    print(f"  Uploaded done marker to s3://{S3_BUCKET}/{done_key}")
    return s3_key


# ── Main ──
if __name__ == '__main__':
    print("=== Fetching stock receipts (進倉單) ===")
    receipts = fetch_all_stock_receipts()
    print(f"Total receipts: {len(receipts)}")

    print("\n=== Building CSV ===")
    csv_content = build_csv(receipts)
    row_count = csv_content.count('\n') - 1  # minus header
    print(f"CSV rows (non-canceled items): {row_count}")

    # Preview first few lines
    lines = csv_content.split('\n')
    for line in lines[:5]:
        print(f"  {line}")
    if len(lines) > 5:
        print(f"  ... ({len(lines) - 5} more lines)")

    print("\n=== Uploading to S3 ===")
    upload_to_s3(csv_content)
    print("\nDone!")
