# -*- coding: utf-8 -*-
"""DB에 retail_listings 샘플 적재 (로컬 또는 Fly API).

  export LISTINGS_INGEST_KEY=비밀키
  python scripts/import_retail_listings.py

  기본 URL: http://127.0.0.1:8000  → BLUEDOT_API_BASE 환경변수로 변경 가능
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request


def main() -> int:
    key = (os.environ.get("LISTINGS_INGEST_KEY") or "").strip()
    if not key:
        print("LISTINGS_INGEST_KEY 환경변수를 설정하세요 (Fly: fly secrets set LISTINGS_INGEST_KEY=...)", file=sys.stderr)
        return 1
    base = (os.environ.get("BLUEDOT_API_BASE") or "http://127.0.0.1:8000").rstrip("/")
    path = os.path.join(os.path.dirname(__file__), "retail_listings_sample.json")
    with open(path, encoding="utf-8") as f:
        body = f.read()
    req = urllib.request.Request(
        f"{base}/api/retail-listings/import",
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Listings-Ingest-Key": key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            out = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(e.read().decode(errors="replace"), file=sys.stderr)
        return e.code
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if not out.get("errors") else 2


if __name__ == "__main__":
    raise SystemExit(main())
