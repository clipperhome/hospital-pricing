"""
crawler.py — Downloads hospital price files and tracks changes.
Versioned storage: raw/<hospital_id>/<YYYY-MM-DD>.<ext>
Never overwrites existing files.
"""

import os
import hashlib
import datetime
import requests
import yaml
import json
import sqlite3
import pathlib

RAW_DIR = pathlib.Path(__file__).parent.parent / "raw"
DB_PATH = pathlib.Path(__file__).parent.parent / "db" / "prices.db"
REGISTRY = pathlib.Path(__file__).parent / "hospitals.yaml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (HospitalPriceClarity/1.0; public research tool)"
}


def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS hospitals (
        id TEXT PRIMARY KEY,
        name TEXT,
        system TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        lat REAL,
        lng REAL,
        cms_id TEXT,
        file_url TEXT,
        file_format TEXT,
        last_checked_at TEXT,
        last_changed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS price_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hospital_id TEXT,
        fetched_at TEXT,
        file_hash TEXT,
        raw_path TEXT,
        file_format TEXT,
        row_count INTEGER,
        status TEXT DEFAULT 'pending',
        FOREIGN KEY(hospital_id) REFERENCES hospitals(id)
    );

    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hospital_id TEXT,
        price_file_id INTEGER,
        cpt_code TEXT,
        drg_code TEXT,
        raw_description TEXT,
        plain_name TEXT,
        cash_price REAL,
        min_price REAL,
        max_price REAL,
        payer_name TEXT,
        price_type TEXT,
        effective_date TEXT,
        recorded_at TEXT,
        FOREIGN KEY(hospital_id) REFERENCES hospitals(id),
        FOREIGN KEY(price_file_id) REFERENCES price_files(id)
    );
    """)
    conn.commit()


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def download_hospital(hospital, conn):
    hid = hospital["id"]
    url = hospital.get("file_url", "TBD")
    fmt = hospital.get("file_format", "unknown")
    now = datetime.datetime.utcnow().isoformat()
    today = datetime.date.today().isoformat()

    if url == "TBD" or not url:
        print(f"[SKIP] {hid} — URL not yet discovered")
        return

    # Upsert hospital record
    conn.execute("""
        INSERT OR REPLACE INTO hospitals
        (id, name, system, city, state, country, lat, lng, cms_id, file_url, file_format, last_checked_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        hid, hospital["name"], hospital.get("system"), hospital.get("city"),
        hospital.get("state"), hospital.get("country", "US"),
        hospital.get("lat"), hospital.get("lng"), hospital.get("cms_id"),
        url, fmt, now
    ))
    conn.commit()

    out_dir = RAW_DIR / hid
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = "csv" if fmt == "csv" else "json"
    out_path = out_dir / f"{today}.{ext}"

    print(f"[FETCH] {hid} → {url}")
    try:
        r = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
        print(f"  ✓ Saved to {out_path} ({out_path.stat().st_size / 1024:.1f} KB)")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return

    fhash = file_hash(out_path)

    # Check if we already have this exact file
    existing = conn.execute(
        "SELECT id FROM price_files WHERE hospital_id=? AND file_hash=?",
        (hid, fhash)
    ).fetchone()

    if existing:
        print(f"  → No change detected (same hash), skipping ingestion")
        os.remove(out_path)  # Don't keep duplicate
        return

    # Register new price file version
    cursor = conn.execute("""
        INSERT INTO price_files (hospital_id, fetched_at, file_hash, raw_path, file_format, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
    """, (hid, now, fhash, str(out_path), fmt))
    conn.commit()

    # Update last_changed_at
    conn.execute("UPDATE hospitals SET last_changed_at=? WHERE id=?", (now, hid))
    conn.commit()

    print(f"  → New version registered (price_file id={cursor.lastrowid})")
    return cursor.lastrowid


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    with open(REGISTRY) as f:
        registry = yaml.safe_load(f)

    for hospital in registry["hospitals"]:
        download_hospital(hospital, conn)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
