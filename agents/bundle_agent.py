"""
bundle_agent.py — Care bundle estimator.
Given a plain-language visit description, builds a typical care bundle,
looks up each item in the DB per hospital, and returns an itemized total.
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

BUNDLE_SYSTEM_PROMPT = """You are a medical billing expert who knows exactly what line items appear on a hospital bill for any given patient visit.

Given a patient visit description, return a JSON object with:
{
  "visit_type": "outpatient_er" | "inpatient" | "outpatient_surgery" | "outpatient_imaging" | "outpatient_lab",
  "plain_name": "Short human-readable name for this visit",
  "scenario": "Brief description of severity/scenario assumed",
  "items": [
    {
      "name": "Human-readable item name",
      "cpt_code": "12345",
      "keywords": ["keyword1", "keyword2"],
      "category": "facility_fee" | "physician" | "lab" | "imaging" | "medication" | "supply" | "anesthesia",
      "required": true,
      "note": "optional note about when this applies"
    }
  ],
  "drg_code": "391",      // if inpatient — DRG covers the whole stay, no individual items needed
  "drg_name": "...",      // human readable DRG description
  "disclaimer": "What this estimate does and doesn't include"
}

Rules:
- For outpatient ER visits: include facility fee (ER Level 1-5, pick most appropriate), physician E&M code, and typical labs/meds/supplies for this condition
- For inpatient: use the DRG code — it covers everything, no individual items
- For outpatient surgery: include main procedure CPT, anesthesia CPT, facility fee
- Be realistic — include what would actually appear on a typical bill for this condition
- ER Level mapping: Level 1 (99281/minor) Level 2 (99282) Level 3 (99283/moderate) Level 4 (99284/severe) Level 5 (99285/critical)
- For ER facility fees, also include the facility-level codes: 5021=Level1, 5022=Level2, 5023=Level3, 5024=Level4, 5025=Level5
- Return ONLY valid JSON
"""

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
}

def normalize_payer(payer_name: str) -> str:
    if not payer_name:
        return None
    lower = payer_name.lower()
    for key, label in PAYER_GROUPS.items():
        if key in lower:
            return label
    return None  # skip unknown payers in bundle view


def build_bundle(visit_description: str) -> dict:
    """Use LLM to build a care bundle for the given visit."""
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": BUNDLE_SYSTEM_PROMPT},
            {"role": "user", "content": visit_description}
        ],
        temperature=1,
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)


def lookup_item(conn, item: dict, hospital_ids: list) -> dict:
    """Look up a single bundle item across all hospitals."""
    results = {}  # hospital_id -> {cash, payer_rates}

    conditions = []
    args = []

    if item.get("cpt_code"):
        conditions.append("p.cpt_code = ?")
        args.append(item["cpt_code"])

    if item.get("keywords"):
        kw_conds = []
        for kw in item["keywords"][:3]:
            kw_conds.append("LOWER(p.raw_description) LIKE ?")
            args.append(f"%{kw.lower()}%")
        conditions.append(f"({' OR '.join(kw_conds)})")

    if not conditions:
        return results

    where = " OR ".join(conditions)

    rows = conn.execute(f"""
        SELECT p.hospital_id, h.name, p.price_type, p.cash_price,
               p.gross_charge, p.min_price, p.payer_name, p.billing_class
        FROM prices p
        JOIN hospitals h ON p.hospital_id = h.id
        WHERE ({where})
        AND p.gross_charge > 5
        ORDER BY p.hospital_id, p.price_type
    """, args).fetchall()

    for r in rows:
        hid = r[0]
        if hid not in results:
            results[hid] = {
                "hospital_name": r[1],
                "cash": None,
                "gross": None,
                "payers": {}
            }
        h = results[hid]
        price_type = r[2]
        cash_price = r[3]
        gross = r[4]
        min_p = r[5]
        payer = r[6]
        billing_class = r[7]

        if price_type == "cash" and cash_price and cash_price > 5:
            if h["cash"] is None or cash_price < h["cash"]:
                h["cash"] = cash_price

        if gross and gross > 5:
            if h["gross"] is None or gross < h["gross"]:
                h["gross"] = gross

        if price_type == "negotiated" and payer:
            label = normalize_payer(payer)
            price = cash_price or min_p or gross
            if label and price and price > 5:
                if label not in h["payers"] or price < h["payers"][label]:
                    h["payers"][label] = price

    return results


def lookup_drg(conn, drg_code: str) -> dict:
    """Look up a DRG bundled inpatient charge across hospitals."""
    results = {}
    rows = conn.execute("""
        SELECT p.hospital_id, h.name, p.price_type, p.cash_price,
               p.gross_charge, p.min_price, p.payer_name, p.raw_description
        FROM prices p
        JOIN hospitals h ON p.hospital_id = h.id
        WHERE p.cpt_code = ?
        ORDER BY p.hospital_id
    """, (drg_code,)).fetchall()

    for r in rows:
        hid = r[0]
        if hid not in results:
            results[hid] = {
                "hospital_name": r[1],
                "cash": None,
                "gross": None,
                "payers": {},
                "description": r[7]
            }
        h = results[hid]
        price_type = r[2]
        cash_price = r[3]
        gross = r[4]
        min_p = r[5]
        payer = r[6]

        if price_type == "cash" and cash_price and cash_price > 5:
            if h["cash"] is None or cash_price < h["cash"]:
                h["cash"] = cash_price
        if gross and gross > 5:
            if h["gross"] is None or gross < h["gross"]:
                h["gross"] = gross
        if price_type == "negotiated" and payer:
            label = normalize_payer(payer)
            price = cash_price or min_p or gross
            if label and price and price > 5:
                if label not in h["payers"] or price < h["payers"][label]:
                    h["payers"][label] = price

    return results


def aggregate_totals(item_results: list) -> dict:
    """Sum all item prices per hospital per payer."""
    totals = {}  # hospital_id -> {hospital_name, cash_total, payer_totals}

    for item_name, hospital_prices in item_results:
        for hid, prices in hospital_prices.items():
            if hid not in totals:
                totals[hid] = {
                    "hospital_name": prices["hospital_name"],
                    "cash_total": 0,
                    "gross_total": 0,
                    "payer_totals": {},
                    "items_found": 0,
                    "items_missing": [],
                }
            t = totals[hid]

            # Use best available price for cash estimate
            best_cash = prices.get("cash") or prices.get("gross")
            if best_cash:
                t["cash_total"] += best_cash
                t["items_found"] += 1
            else:
                t["items_missing"].append(item_name)

            for payer, price in prices.get("payers", {}).items():
                if payer not in t["payer_totals"]:
                    t["payer_totals"][payer] = 0
                t["payer_totals"][payer] += price

    return totals


def format_bundle_result(bundle: dict, item_results: list, totals: dict) -> str:
    """Format the full bundle estimate for display."""
    lines = []
    lines.append(f"## {bundle['plain_name']}")
    lines.append(f"_{bundle.get('scenario', '')}_\n")

    # Collect payers seen
    all_payers = []
    priority = ["Aetna", "Blue Shield", "Anthem Blue Cross", "Cigna",
                "United Healthcare", "HealthNet", "Medicare", "Medi-Cal"]
    seen = set()
    for _, hprices in totals.items():
        for p in priority:
            if p in hprices["payer_totals"] and p not in seen:
                all_payers.append(p)
                seen.add(p)
    all_payers = all_payers[:5]

    # Summary table
    lines.append("### Estimated Total Cost")
    header = f"{'Hospital':<35} {'Cash Est.':>12}"
    for p in all_payers:
        header += f"  {p[:12]:>12}"
    lines.append(header)
    lines.append("-" * len(header))

    sorted_hospitals = sorted(totals.items(),
        key=lambda x: x[1]["cash_total"] or float("inf"))

    for hid, t in sorted_hospitals:
        cash = f"${t['cash_total']:,.0f}" if t["cash_total"] else "—"
        row = f"{t['hospital_name'][:33]:<35} {cash:>12}"
        for p in all_payers:
            pt = t["payer_totals"].get(p)
            cell = f"${pt:,.0f}" if pt else "—"
            row += f"  {cell:>12}"
        lines.append(row)

    # Itemized breakdown
    lines.append("\n### Itemized Breakdown")
    for item_name, hospital_prices in item_results:
        lines.append(f"\n**{item_name}**")
        row = f"  {'Hospital':<35} {'Cash':>10} {'Gross':>10}"
        for p in all_payers:
            row += f"  {p[:10]:>10}"
        lines.append(row)
        lines.append("  " + "-" * (len(row) - 2))
        for hid, prices in sorted(hospital_prices.items()):
            cash = f"${prices['cash']:,.0f}" if prices.get("cash") else "—"
            gross = f"${prices['gross']:,.0f}" if prices.get("gross") else "—"
            row = f"  {prices['hospital_name'][:33]:<35} {cash:>10} {gross:>10}"
            for p in all_payers:
                pt = prices.get("payers", {}).get(p)
                cell = f"${pt:,.0f}" if pt else "—"
                row += f"  {cell:>10}"
            lines.append(row)

    lines.append(f"\n_{bundle.get('disclaimer', 'Estimates based on hospital-published CMS price transparency data. Actual bills vary based on specific services rendered.')}_")
    return "\n".join(lines)


def estimate(visit_description: str) -> str:
    """Main entry point — estimate cost for a visit description."""
    print(f"\nEstimating cost for: '{visit_description}'")
    print("Building care bundle with LLM...")

    bundle = build_bundle(visit_description)
    visit_type = bundle.get("visit_type", "outpatient_er")

    print(f"  Visit type: {visit_type}")
    print(f"  Scenario: {bundle.get('scenario')}")

    conn = sqlite3.connect(DB_PATH)

    # Inpatient: just DRG lookup
    if visit_type == "inpatient" and bundle.get("drg_code"):
        drg = bundle["drg_code"]
        print(f"  Inpatient DRG: {drg} — {bundle.get('drg_name')}")
        hospital_prices = lookup_drg(conn, drg)
        conn.close()

        lines = [f"## {bundle['plain_name']} (Inpatient Admission)"]
        lines.append(f"_{bundle.get('scenario')}_")
        lines.append(f"\nDRG {drg}: {bundle.get('drg_name', '')}\n")
        if not hospital_prices:
            lines.append("No DRG pricing data found for this condition in the database.")
        else:
            all_payers = []
            priority = ["Aetna", "Blue Shield", "Cigna", "United Healthcare", "Medicare"]
            seen = set()
            for hdata in hospital_prices.values():
                for p in priority:
                    if p in hdata.get("payers", {}) and p not in seen:
                        all_payers.append(p)
                        seen.add(p)
            all_payers = all_payers[:5]

            header = f"{'Hospital':<35} {'Cash':>12}"
            for p in all_payers:
                header += f"  {p[:12]:>12}"
            lines.append(header)
            lines.append("-" * len(header))
            for hid, hdata in sorted(hospital_prices.items()):
                cash = f"${hdata['cash']:,.0f}" if hdata.get("cash") else "—"
                row = f"{hdata['hospital_name'][:33]:<35} {cash:>12}"
                for p in all_payers:
                    pt = hdata.get("payers", {}).get(p)
                    cell = f"${pt:,.0f}" if pt else "—"
                    row += f"  {cell:>12}"
                lines.append(row)
        lines.append(f"\n_{bundle.get('disclaimer', '')}_")
        return "\n".join(lines)

    # Outpatient: item-by-item lookup
    items = bundle.get("items", [])
    print(f"  Bundle items: {len(items)}")
    for item in items:
        print(f"    - {item['name']} (CPT {item.get('cpt_code')})")

    item_results = []
    for item in items:
        print(f"  Looking up: {item['name']}...")
        prices = lookup_item(conn, item, [])
        item_results.append((item["name"], prices))

    conn.close()

    totals = aggregate_totals(item_results)
    return format_bundle_result(bundle, item_results, totals)


if __name__ == "__main__":
    import sys
    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "ER visit for food poisoning"
    print(estimate(query))
