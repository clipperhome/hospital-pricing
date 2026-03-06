"""
search_agent.py — Plain language hospital price search.
Uses gpt-5.3-chat-latest to interpret queries, then queries SQLite.
"""

import sqlite3
import json
import pathlib
import os
from openai import OpenAI

DB_PATH = pathlib.Path(__file__).parent.parent / "db" / "prices.db"
client = OpenAI(
    api_key=os.environ.get("MOONSHOT_API_KEY"),
    base_url="https://api.moonshot.ai/v1"
)
MODEL = "kimi-k2.5"


SYSTEM_PROMPT = """You are a medical procedure search assistant for a hospital price comparison tool.

Your job is to convert a user's plain language query into a structured search request.

Given a user query, return a JSON object with:
{
  "cpt_codes": ["27447", "27446"],     // likely CPT codes, empty if unknown
  "keywords": ["knee replacement", "arthroplasty"],  // keywords to search in procedure descriptions
  "plain_name": "Total Knee Replacement",  // clean human-readable name for this procedure
  "explanation": "brief explanation of what this procedure is"
}

Rules:
- Include common CPT code variants (e.g. for knee replacement: 27447, 27446, 27440)
- Keywords should match what hospital billing descriptions might say (often abbreviated/medical)
- Be broad enough to catch variations but specific enough to avoid unrelated procedures
- Return ONLY valid JSON, no other text
"""


def interpret_query(user_query: str) -> dict:
    """Use LLM to convert plain English to search parameters."""
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_query}
        ],
        temperature=1,
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)


def search_prices(params: dict, limit_per_hospital: int = 5) -> list:
    """Query SQLite for matching procedures across all hospitals."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Build WHERE clause
    conditions = []
    args = []

    if params.get("cpt_codes"):
        placeholders = ",".join("?" * len(params["cpt_codes"]))
        conditions.append(f"p.cpt_code IN ({placeholders})")
        args.extend(params["cpt_codes"])

    if params.get("keywords"):
        kw_conditions = []
        for kw in params["keywords"]:
            kw_conditions.append("LOWER(p.raw_description) LIKE ?")
            args.append(f"%{kw.lower()}%")
        conditions.append(f"({' OR '.join(kw_conditions)})")

    if not conditions:
        return []

    where = " OR ".join(conditions)

    query = f"""
        SELECT
            p.hospital_id,
            h.name as hospital_name,
            h.city,
            h.state,
            p.cpt_code,
            p.raw_description,
            p.price_type,
            p.cash_price,
            p.gross_charge,
            p.min_price,
            p.max_price,
            p.payer_name,
            p.plan_name,
            p.setting,
            p.billing_class,
            pf.fetched_at
        FROM prices p
        JOIN hospitals h ON p.hospital_id = h.id
        JOIN price_files pf ON p.price_file_id = pf.id
        WHERE ({where})
        ORDER BY p.hospital_id, p.price_type, p.cash_price NULLS LAST
    """

    rows = conn.execute(query, args).fetchall()
    conn.close()
    return [dict(r) for r in rows]


PAYER_GROUPS = {
    "aetna": "Aetna",
    "blue shield": "Blue Shield",
    "blue cross": "Anthem Blue Cross",
    "anthem": "Anthem Blue Cross",
    "cigna": "Cigna",
    "united": "United Healthcare",
    "uhc": "United Healthcare",
    "healthnet": "HealthNet",
    "medicare": "Medicare",
    "medicaid": "Medicaid",
    "medi-cal": "Medi-Cal",
    "multiplan": "MultiPlan",
    "kaiser": "Kaiser",
}

def normalize_payer(payer_name: str) -> str:
    """Normalize payer name to a common group label."""
    if not payer_name:
        return None
    lower = payer_name.lower()
    for key, label in PAYER_GROUPS.items():
        if key in lower:
            return label
    return payer_name.split("[")[0].strip().title()


def summarize_by_hospital(rows: list, plain_name: str) -> list:
    """Group results by hospital, collect cash + per-payer rates."""
    by_hospital = {}

    for r in rows:
        hid = r["hospital_id"]
        if hid not in by_hospital:
            by_hospital[hid] = {
                "hospital_name": r["hospital_name"],
                "city": r["city"],
                "state": r["state"],
                "cash_price": None,
                "gross_charge": None,
                "min_price": None,
                "max_price": None,
                "payer_rates": {},   # payer_label -> best price
                "procedures": set(),
                "last_updated": r["fetched_at"][:10] if r["fetched_at"] else None,
            }

        h = by_hospital[hid]
        h["procedures"].add(r["raw_description"])

        if r["price_type"] == "cash" and r["cash_price"] and r["cash_price"] > 10:
            if h["cash_price"] is None or r["cash_price"] < h["cash_price"]:
                h["cash_price"] = r["cash_price"]

        if r["gross_charge"] and r["gross_charge"] > 10:
            if h["gross_charge"] is None or r["gross_charge"] < h["gross_charge"]:
                h["gross_charge"] = r["gross_charge"]

        if r["min_price"] and (h["min_price"] is None or r["min_price"] < h["min_price"]):
            h["min_price"] = r["min_price"]
        if r["max_price"] and (h["max_price"] is None or r["max_price"] > h["max_price"]):
            h["max_price"] = r["max_price"]

        # Collect negotiated payer rates
        if r["price_type"] == "negotiated" and r.get("payer_name"):
            label = normalize_payer(r["payer_name"])
            if label:
                # Pick the price field that has a value
                price = None
                for col in ("cash_price", "min_price", "gross_charge"):
                    v = r.get(col)
                    if v and v > 10:
                        price = v
                        break
                if price:
                    if label not in h["payer_rates"] or price < h["payer_rates"][label]:
                        h["payer_rates"][label] = price

    # Convert sets to lists
    for h in by_hospital.values():
        h["procedures"] = list(h["procedures"])[:3]

    # Sort by cash price then gross charge
    sorted_hospitals = sorted(
        by_hospital.values(),
        key=lambda x: x["cash_price"] or x["gross_charge"] or float("inf")
    )

    return sorted_hospitals


def format_result(hospitals: list, plain_name: str, explanation: str) -> str:
    """Format results showing cash + key insurance rates per hospital."""
    lines = [f"## {plain_name}"]
    lines.append(f"_{explanation}_\n")

    if not hospitals:
        lines.append("No pricing data found for this procedure.")
        return "\n".join(lines)

    # Collect all payer labels seen across all hospitals
    all_payers = []
    priority_payers = ["Aetna", "Blue Shield", "Anthem Blue Cross", "Cigna",
                       "United Healthcare", "HealthNet", "Medicare", "Medi-Cal"]
    seen = set()
    for h in hospitals:
        for p in priority_payers:
            if p in h["payer_rates"] and p not in seen:
                all_payers.append(p)
                seen.add(p)
    # Add remaining payers not in priority list
    for h in hospitals:
        for p in h["payer_rates"]:
            if p not in seen:
                all_payers.append(p)
                seen.add(p)
    all_payers = all_payers[:6]  # cap at 6 payers for readability

    # Header
    col_width = 12
    header = f"{'Hospital':<34} {'Cash':>10} {'Gross':>10}"
    for p in all_payers:
        header += f"  {p[:10]:>10}"
    lines.append(header)
    lines.append("-" * len(header))

    for h in hospitals:
        cash = f"${h['cash_price']:,.0f}" if h["cash_price"] else "—"
        gross = f"${h['gross_charge']:,.0f}" if h["gross_charge"] else "—"
        name = h["hospital_name"][:32]
        row = f"{name:<34} {cash:>10} {gross:>10}"
        for p in all_payers:
            price = h["payer_rates"].get(p)
            cell = f"${price:,.0f}" if price else "—"
            row += f"  {cell:>10}"
        lines.append(row)

    lines.append(f"\n_Data fetched from hospital-published CMS price transparency files._")
    lines.append(f"_Prices shown are professional/facility fees and may not reflect total cost._")

    return "\n".join(lines)


def search(query: str, verbose: bool = False) -> str:
    """Main search entry point."""
    print(f"\nSearching for: '{query}'")
    print("Interpreting query with LLM...")

    params = interpret_query(query)
    print(f"  CPT codes: {params.get('cpt_codes')}")
    print(f"  Keywords: {params.get('keywords')}")
    print(f"  Procedure: {params.get('plain_name')}\n")

    rows = search_prices(params)
    print(f"Found {len(rows):,} matching rows across hospitals")

    hospitals = summarize_by_hospital(rows, params.get("plain_name", query))
    result = format_result(hospitals, params.get("plain_name", query), params.get("explanation", ""))

    if verbose and rows:
        result += "\n\n### Sample raw rows\n"
        for r in rows[:3]:
            result += f"  {r['hospital_name']} | {r['raw_description']} | {r['price_type']} | cash=${r['cash_price']} gross=${r['gross_charge']}\n"

    return result


if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "knee replacement"
    print(search(query, verbose=True))
