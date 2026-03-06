# Hospital Price Clarity — Design Decisions

## Care Bundle Agent (Key Design — 2026-03-05)

### Problem
Hospital bills have 5-20 line items per visit. Showing a single CPT code price is misleading and useless. A real ER visit for food poisoning includes:
- ER facility fee (Level 1-5 depending on severity)
- ED physician fee
- Blood/metabolic lab panel
- Stool culture (if taken)
- IV fluids (if administered)
- Antiemetic medication

### Solution: LLM Care Bundle Estimator
When a user describes a visit purpose (not just a procedure), the system:

1. **Classifies** the scenario: outpatient ER vs inpatient admission
2. **Builds a bundle** using LLM clinical knowledge — lists all typical line items for that visit
3. **Maps** each item to CPT/DRG codes
4. **Looks up** each item price per hospital from DB
5. **Sums** all items → total estimated cost per hospital
6. **Shows** itemized breakdown + total, per payer (cash, Aetna, Blue Shield, Medicare, etc.)

### Key Data Assets
- **ER Level codes** (5021-5025): outpatient ER facility fee by severity
- **DRG codes**: bundled inpatient charges (already cover the full stay)
- **CPT codes**: individual procedure/lab/med components for outpatient visits

### Example Bundles

**ER — Food Poisoning (mild, outpatient)**
| Item | CPT | Typical Price |
|---|---|---|
| ER facility fee (Level 3) | 5023 | $800–$2,500 |
| ED physician evaluation | 99283 | $200–$500 |
| Basic metabolic panel | 80048 | $50–$200 |
| IV fluid administration | 96360 | $100–$400 |
| Antiemetic (ondansetron) | J2405 | $20–$100 |
| **Total estimate** | | **$1,170–$3,700** |

**ER — Food Poisoning (severe, inpatient)**
| Item | DRG | Typical Price |
|---|---|---|
| Full inpatient stay (gastroenteritis) | 391/392 | $8,000–$25,000 |

**Knee Replacement (outpatient surgical)**
| Item | CPT | Typical Price |
|---|---|---|
| Total knee arthroplasty | 27447 | $15,000–$40,000 |
| Anesthesia | 01402 | $1,500–$4,000 |
| Implant device | supply | $5,000–$15,000 |
| **Total estimate** | | **$21,500–$59,000** |

### Display Format
```
## ER Visit — Food Poisoning (Outpatient, Moderate)

Estimated total for a typical Level 3 ER visit including evaluation,
labs, IV fluids, and medication.

Hospital               Cash Est.    Aetna Est.  Blue Shield   Medicare
----------------------------------------------------------------------
Stanford Hospital       $1,840        $1,210       $1,580        $890
CPMC Van Ness           $2,100        $1,450         —           $920
Alta Bates Summit         —           $1,680         —             —

[ See itemized breakdown ↓ ]

Item                    CPT      Stanford    CPMC Van Ness
----------------------------------------------------------
ER Facility (Level 3)   5023      $1,200         $1,400
ED Physician            99283       $380           $420
Basic Metabolic Panel   80048        $80            $90
IV Fluid Admin          96360       $180           $190
Antiemetic (Zofran)     J2405        —              —
──────────────────────────────────────────────────────
Total                            $1,840         $2,100
```

---

## What We Learned From Building the Bundle Agent (2026-03-05)

### Data Quality Issues — Chargemaster Files Are Not Clean

**1. Duplicate rows per CPT**
Stanford lists CPT `99283` 5+ times (same code, different settings: "both/inpatient/outpatient" and per payer plan). Summing without careful deduplication inflates prices wildly.

**2. Professional + Facility fees per CPT**
Same CPT code (e.g. `99283`) appears twice per hospital:
- `billing_class = professional` → physician fee (~$153 cash at Stanford)
- `billing_class = facility` → hospital/building fee (~$1,995 cash at Stanford)
Both must be summed for a real estimate. Originally we were only getting the professional fee, making Stanford look artificially cheap ($193 total vs realistic ~$4,800+).

**3. Hospital-specific code quirks**
Stanford does not use CPT `5023` for ER facility fees — they file it under `99283 billing_class=facility`. Code must not assume standard CPT mappings hold across hospitals.

**4. Keyword search is dangerous**
Searching "emergency" matched surgical screws (C1713 "SCREW NEURO EMERGENCY") as ER fees. Rule: use exact CPT match when available; keywords are last resort only.

**5. Sutter hospitals have no fixed negotiated rates**
CPMC, Alta Bates, Mills-Peninsula use algorithm-based pricing. Their MRFs do not contain fixed dollar amounts for payer-negotiated rates → "—" in all payer columns. This is by design, not a data gap.

**6. Chargemaster prices ≠ what people actually pay**
The MRF "cash price" is the self-pay discounted rate. The "gross charge" is the inflated list price. Neither matches what insured patients pay — for that you need actual claims data.

### Fundamental Methodology Problem

Summing individual CPT line items to get a visit total is **unreliable** because:
- Line items don't add linearly (hospitals bundle services internally)
- We don't know which items a specific patient actually received
- Prices vary by day, physician, payer contract vintage

### What the Data IS Good For

- **Relative comparison**: Hospital A is 2x more expensive than Hospital B for a given procedure
- **Single-procedure lookup**: CPT 27447 (knee replacement) at Stanford vs CPMC
- **Cash price baseline**: Useful for uninsured/self-pay patients negotiating
- **Payer transparency**: What different insurers pay for the same code (where available)

### Recommended Direction

Rather than computing a precise total (which the data can't reliably support), show:
- **Price ranges** with clear "typical visit" context sourced from DESIGN.md benchmarks
- **Per-item comparisons** — let users see individual line items across hospitals
- **Heavy caveats**: "This is an estimate based on published chargemaster rates. Your actual bill will vary."
- Long-term: supplement with CMS claims data (actual paid amounts) for more accurate totals

---

## Architecture Docs
Full architecture: `workspace/projects/hospital-pricing/ARCHITECTURE.md`
