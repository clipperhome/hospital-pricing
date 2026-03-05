"""
extractor.py — Parallel extraction agent per hospital price file.
Reads pending price_files from DB, spawns one worker per file.
"""

import sqlite3
import json
import csv
import pathlib
import datetime
import concurrent.futures
import sys
import io

DB_PATH = pathlib.Path(__file__).parent.parent / "db" / "prices.db"


def get_pending_files(conn):
    return conn.execute("""
        SELECT pf.id, pf.hospital_id, pf.raw_path, pf.file_format
        FROM price_files pf
        WHERE pf.status = 'pending'
    """).fetchall()


def extract_stanford_json(raw_path):
    """Extract from CMS v2.2 JSON format (Stanford/most modern hospitals)"""
    rows = []
    with open(raw_path) as f:
        data = json.load(f)

    hospital_name = data.get("hospital_name", "")
    charges = data.get("standard_charge_information", [])

    for charge in charges:
        desc = charge.get("description", "")
        codes = charge.get("code_information", [])
        
        # Get primary CPT/HCPCS code
        cpt_code = None
        for c in codes:
            if c.get("type") in ("CPT", "HCPCS"):
                cpt_code = c.get("code")
                break
        if not cpt_code:
            for c in codes:
                cpt_code = c.get("code")
                break

        for sc in charge.get("standard_charges", []):
            gross = sc.get("gross_charge")
            cash = sc.get("discounted_cash")
            min_p = sc.get("minimum")
            max_p = sc.get("maximum")
            setting = sc.get("setting", "")
            billing_class = sc.get("billing_class", "")

            # Base row (no payer)
            if gross or cash:
                rows.append({
                    "cpt_code": cpt_code,
                    "raw_description": desc,
                    "cash_price": cash,
                    "gross_charge": gross,
                    "min_price": min_p,
                    "max_price": max_p,
                    "payer_name": None,
                    "plan_name": None,
                    "price_type": "cash" if cash else "gross",
                    "setting": setting,
                    "billing_class": billing_class,
                })

            # Per-payer rows
            for payer in sc.get("payers_information", []):
                neg = payer.get("standard_charge_dollar")
                if neg:
                    rows.append({
                        "cpt_code": cpt_code,
                        "raw_description": desc,
                        "cash_price": None,
                        "gross_charge": gross,
                        "min_price": min_p,
                        "max_price": max_p,
                        "payer_name": payer.get("payer_name"),
                        "plan_name": payer.get("plan_name"),
                        "price_type": "negotiated",
                        "negotiated_price": neg,
                        "setting": setting,
                        "billing_class": billing_class,
                    })

    return rows


def extract_sutter_csv(raw_path):
    """Extract from CMS v2.2 CSV format (Sutter Health hospitals)"""
    rows = []
    with open(raw_path, newline="", encoding="utf-8-sig") as f:
        # Skip header metadata rows (first 3 rows are hospital info)
        lines = f.readlines()
    
    # Find the actual header row (contains 'description')
    header_idx = None
    for i, line in enumerate(lines):
        if "description" in line.lower() and "code|1" in line.lower():
            header_idx = i
            break
    
    if header_idx is None:
        print(f"  ! Could not find header row in {raw_path}")
        return rows

    content = "".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(content))

    for row in reader:
        desc = row.get("description", "").strip()
        if not desc:
            continue

        # Get primary code
        cpt_code = (row.get("code|1") or "").strip() or None
        code_type = (row.get("code|1|type") or "").strip()

        gross = _to_float(row.get("standard_charge|gross"))
        cash = _to_float(row.get("standard_charge|discounted_cash"))
        min_p = _to_float(row.get("standard_charge|min"))
        max_p = _to_float(row.get("standard_charge|max"))
        neg = _to_float(row.get("standard_charge|negotiated_dollar"))
        payer = (row.get("payer_name") or "").strip() or None
        plan = (row.get("plan_name") or "").strip() or None
        setting = (row.get("setting") or "").strip()
        billing_class = (row.get("billing_class") or "").strip()

        price_type = "negotiated" if payer else ("cash" if cash else "gross")

        rows.append({
            "cpt_code": cpt_code,
            "raw_description": desc,
            "cash_price": cash,
            "gross_charge": gross,
            "min_price": min_p,
            "max_price": max_p,
            "payer_name": payer,
            "plan_name": plan,
            "price_type": price_type,
            "negotiated_price": neg,
            "setting": setting,
            "billing_class": billing_class,
        })

    return rows


def _to_float(val):
    if val is None:
        return None
    try:
        v = str(val).strip().replace(",", "").replace("$", "")
        return float(v) if v else None
    except ValueError:
        return None


def process_file(file_rec):
    file_id, hospital_id, raw_path, file_format = file_rec
    now = datetime.datetime.utcnow().isoformat()
    
    print(f"[START] {hospital_id} (file_id={file_id}, format={file_format})")

    try:
        if file_format == "json":
            rows = extract_stanford_json(raw_path)
        elif file_format == "csv":
            rows = extract_sutter_csv(raw_path)
        else:
            print(f"  ! Unknown format: {file_format}")
            return file_id, hospital_id, 0, "failed"

        print(f"  → Extracted {len(rows):,} rows from {hospital_id}")
        return file_id, hospital_id, rows, "done"

    except Exception as e:
        print(f"  ✗ Error processing {hospital_id}: {e}")
        import traceback; traceback.print_exc()
        return file_id, hospital_id, 0, "failed"


def insert_rows(conn, file_id, hospital_id, rows):
    now = datetime.datetime.utcnow().isoformat()
    batch = []
    for r in rows:
        price = r.get("negotiated_price") or r.get("cash_price") or r.get("gross_charge")
        batch.append((
            hospital_id,
            file_id,
            r.get("cpt_code"),
            None,  # drg_code
            r.get("raw_description"),
            None,  # plain_name (LLM enrichment later)
            r.get("cash_price"),
            r.get("min_price"),
            r.get("max_price"),
            r.get("payer_name"),
            r.get("plan_name"),
            r.get("price_type"),
            r.get("gross_charge"),
            r.get("setting"),
            r.get("billing_class"),
            now,
        ))
    
    conn.executemany("""
        INSERT INTO prices (
            hospital_id, price_file_id, cpt_code, drg_code,
            raw_description, plain_name,
            cash_price, min_price, max_price,
            payer_name, plan_name, price_type, gross_charge,
            setting, billing_class, recorded_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, batch)
    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)

    # Ensure prices table has all needed columns
    conn.executescript("""
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
            plan_name TEXT,
            price_type TEXT,
            gross_charge REAL,
            setting TEXT,
            billing_class TEXT,
            recorded_at TEXT
        );
    """)
    conn.commit()

    pending = get_pending_files(conn)
    if not pending:
        print("No pending files to process.")
        return

    print(f"Processing {len(pending)} file(s) in parallel...\n")

    # Parallel extraction (CPU-bound reading, so use threads for I/O)
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pending)) as executor:
        futures = {executor.submit(process_file, f): f for f in pending}
        results = []
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    # Insert results sequentially (SQLite doesn't like concurrent writes)
    print("\nInserting into DB...")
    for file_id, hospital_id, rows, status in results:
        if status == "done" and isinstance(rows, list):
            insert_rows(conn, file_id, hospital_id, rows)
            conn.execute("UPDATE price_files SET status=?, row_count=? WHERE id=?",
                        ("done", len(rows), file_id))
            conn.commit()
            print(f"  ✓ {hospital_id}: {len(rows):,} rows inserted")
        else:
            conn.execute("UPDATE price_files SET status='failed' WHERE id=?", (file_id,))
            conn.commit()
            print(f"  ✗ {hospital_id}: failed")

    # Summary
    total = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    print(f"\nTotal price rows in DB: {total:,}")
    conn.close()


if __name__ == "__main__":
    main()
