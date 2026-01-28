#!/usr/bin/env python3

# YNAB → Monarch migration script
# - Uses JSON mapping (emojis preserved)
# - Produces consolidated + per-account transactions and balances
# - Adds Split Group ID column for splits
# - Handles transfers, starting balances, reconciliations, splits, and multiple tags
# - Ensures balances have only one row per date/account (latest balance)

import csv
import json
import argparse
import hashlib
from datetime import datetime
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, Tuple, List, Optional


# ------------------------------
# Helpers
# ------------------------------

def parse_money(s: Optional[str]) -> float:
    """
    Convert a YNAB money string into a float.

    Handles:
    - Empty or None values
    - Dollar signs and commas
    - Parentheses for negative values: (123.45)
    - Unexpected formatting by stripping non-numeric characters

    Returns:
        Signed float value
    """
    if not s:
        return 0.0
    s = s.strip()
    if not s:
        return 0.0
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    s = s.replace("$", "").replace(",", "").strip()
    if not s:
        return 0.0
    try:
        amt = float(s)
    except ValueError:
        amt = float("".join(ch for ch in s if ch.isdigit() or ch in ".-") or 0)
    return -amt if neg else amt


def txn_type(amount: float) -> str:
    """
    Determine transaction direction for Monarch.

    Monarch expects:
    - "credit" for positive amounts
    - "debit" for negative amounts
    """
    return "credit" if amount >= 0 else "debit"


def sha8(s: str) -> str:
    """
    Generate a short, stable hash used for Split Group IDs.
    """
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def normalize_date(date_str: str) -> str:
    """
    Normalize various YNAB date formats into ISO YYYY-MM-DD.

    Accepted formats:
    - MM/DD/YYYY
    - YYYY-MM-DD
    - MM/DD/YY

    If parsing fails, returns the original string unchanged.
    """
    date_str = date_str.strip()
    if not date_str:
        return ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


# ------------------------------
# Mapping
# ------------------------------

class CategoryMapper:
    """
    Applies YNAB → Monarch category mappings.

    Supports:
    - Exact group + category matches
    - Category-only fallbacks
    - Memo-based conditional rules
    - Multiple tags
    """
    def __init__(self, mapping_records: List[dict]):
        self.map_full = {}
        self.map_cat_only = {}
        self.map_memo = defaultdict(list)

        for rec in mapping_records:
            sg = rec.get("source_group", "").strip()
            sc = rec.get("source_category", "").strip()
            mg = rec.get("monarch_group", "Other").strip()
            mc = rec.get("monarch_category", "Uncategorized").strip()
            tags = [t.strip() for t in rec.get("tags", []) if t.strip()]
            memo = rec.get("memo", "").strip()

            if memo:
                self.map_memo[(sg, sc)].append((memo.lower(), mg, mc, tags))
            else:
                self.map_full[(sg, sc)] = (mg, mc, tags)
                if sc not in self.map_cat_only:
                    self.map_cat_only[sc] = (mg, mc, tags)


    def map(self, sg: str, sc: str, memo: str) -> Tuple[str, str, List[str], bool]:
        """
        Resolve a YNAB category into a Monarch category.

        Resolution order:
        1. Memo-based match
        2. Exact group+category
        3. Category-only fallback
        4. Default: Other / Uncategorized

        Returns:
            monarch_group,
            monarch_category,
            tags,
            mapped_successfully (bool)
        """
        sg, sc, memo = sg.strip(), sc.strip(), memo.strip()

        # Memo rules take highest priority
        for memo_rule, mg, mc, tags in self.map_memo.get((sg, sc), []):
            if memo_rule in memo.lower():
                return mg, mc, tags, True

        # Exact match
        if (sg, sc) in self.map_full:
            mg, mc, tags = self.map_full[(sg, sc)]
            return mg, mc, tags, True

        # Category-only fallback
        if sc in self.map_cat_only:
            mg, mc, tags = self.map_cat_only[sc]
            return mg, mc, tags, True

        # Default fallback
        return "Other", "Uncategorized", [], False


# ------------------------------
# Split detection
# ------------------------------

def build_split_ids(rows: List[dict]) -> Tuple[Dict[Tuple[str, str, str, float, int], str], Counter]:
    """
    Detect split transactions.

    YNAB exports split transactions as multiple rows with the same:
    - Account
    - Date
    - Payee

    This function:
    - Groups transactions by (account, date, payee)
    - If multiple rows exist, assigns a shared Split Group ID
    """
    per = defaultdict(list)
    counts = Counter()

    for r in rows:
        acct = (r.get("Account") or "").strip()
        date = normalize_date(r.get("Date") or "")
        payee = (r.get("Payee") or "").strip()
        amt = parse_money(r.get("Inflow")) - parse_money(r.get("Outflow"))
        per[(acct, date, payee)].append(amt)
        counts[(acct, date, payee)] += 1

    ids = {}
    for (acct, date, payee), vals in per.items():
        if len(vals) > 1:
            total = round(sum(vals), 2)
            base = f"{date}|{acct}|{payee}|{total:.2f}|{len(vals)}"
            ids[(acct, date, payee, total, len(vals))] = f"SPLIT-{date.replace('-','')}-{sha8(base)}"

    return ids, counts


# ------------------------------
# Balances
# ------------------------------

def derive_balances(register_rows: List[dict]) -> Dict[str, List[dict]]:
    """
    Derive running balances per account from transaction history.

    Rules:
    - Balances are calculated chronologically
    - If multiple transactions occur on the same date,
      only the LAST balance of that day is kept

    Returns both global and per-account balances.
    """
    by_acct = defaultdict(list)

    for r in register_rows:
        acct = (r.get("Account") or "").strip()
        date = normalize_date(r.get("Date") or "")
        amt = parse_money(r.get("Inflow")) - parse_money(r.get("Outflow"))
        by_acct[acct].append((date, amt))

    results_global = []
    results_per_acct = defaultdict(list)

    for acct, items in by_acct.items():
        if not items:
            continue

        items.sort(key=lambda x: x[0])
        balance = 0.0
        last_per_day = {}

        for d, amt in items:
            balance += amt
            last_per_day[d] = f"{balance:.2f}"

        for d, bal in sorted(last_per_day.items()):
            results_global.append({"Date": d, "Account": acct, "Balance": bal})
            results_per_acct[acct].append({"Date": d, "Balance": bal})

    return {"global": results_global, "per_account": results_per_acct}


# ------------------------------
# Conversion
# ------------------------------

TX_FIELDS = [
    "Date","Description","Original Description","Amount","Transaction Type",
    "Category","Account Name","Labels","Notes","Split Group ID"
]

PER_ACCOUNT_TX_FIELDS = [
    "Date","Merchant","Category","Account","Original Statement","Notes","Amount","Tags"
]

PER_ACCOUNT_BAL_FIELDS = ["Date", "Balance"]


def convert_register(ynab_register: Path,
                     category_mapping: Path,
                     out_dir: Path,
                     dry_run: bool) -> None:
    """
    Convert a YNAB CSV register to Monarch CSVs.
    
    If dry_run=True, no files are written; prints counts instead.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    mappings = json.load(open(category_mapping, "r", encoding="utf-8"))
    mapper = CategoryMapper(mappings)

    with open(ynab_register, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        reg_rows = list(r)

    split_ids, counts = build_split_ids(reg_rows)

    tx_out, unmapped = [], set()
    per_account_data = defaultdict(list)

    for row in reg_rows:
        acct  = (row.get("Account") or "").strip()
        date  = normalize_date(row.get("Date") or "")
        payee = (row.get("Payee") or "").strip()
        memo  = (row.get("Memo") or "").strip()
        sg = (row.get("Category Group") or "").strip()
        sc = (row.get("Category") or "").strip()

        # Special cases override category mapping
        if payee.startswith("Transfer :"):
            category_group, category, tags, mapped = "Transfers", "Transfer", [], True
            payee = payee.replace("Transfer :", "Transfer:").strip()
        elif "Starting Balance" in payee or "Reconciliation Balance Adjustment" in payee:
            category_group, category, tags, mapped = "Transfers", "Balance Adjustments", [], True
        else:
            category_group, category, tags, mapped = mapper.map(sg, sc, memo)
            if not mapped:
                unmapped.add((sg, sc))

        outflow = parse_money(row.get("Outflow"))
        inflow  = parse_money(row.get("Inflow"))
        signed  = inflow - outflow
        direction = txn_type(signed)

        count = counts[(acct, date, payee)]
        sgid = split_ids.get((acct, date, payee, round(signed, 2), count), "")

        tx_out.append({
            "Date": date,
            "Description": payee,
            "Original Description": payee,
            "Amount": f"{signed:.2f}",
            "Transaction Type": direction,
            "Category": category,
            "Account Name": acct,
            "Labels": ",".join(tag.strip() for tag in tags),
            "Notes": memo,
            "Split Group ID": sgid
        })

        per_account_data[acct].append({
            "Date": date,
            "Merchant": payee,
            "Category": category,
            "Account": acct,
            "Original Statement": payee,
            "Notes": memo,
            "Amount": f"{signed:.2f}",
            "Tags": ",".join(tag.strip() for tag in tags),
        })

    # Exit if dry_run is set (no output files)
    if dry_run:
        if unmapped:
            with open(out_dir / "unmapped_categories.csv", "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                w.writerow(["Category Group", "Category"])
                for sg, sc in sorted(unmapped):
                    w.writerow([sg, sc])

        split_group_count = len({r["Split Group ID"] for r in tx_out if r["Split Group ID"]})

        print("🧪 DRY RUN — no CSVs written")
        print(f"Transactions processed: {len(tx_out)}")
        print(f"Split groups detected: {split_group_count}")
        print(f"Unmapped categories: {len(unmapped)}")

        return
    
    with open(out_dir / "transactions_mapped.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TX_FIELDS)
        w.writeheader()
        w.writerows(tx_out)

    balances = derive_balances(reg_rows)

    with open(out_dir / "balances_mapped.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Date","Account","Balance"])
        w.writeheader()
        w.writerows(balances["global"])

    for acct, rows in per_account_data.items():
        acct_dir = out_dir / acct.replace(":", "_").replace(" ", "_")
        acct_dir.mkdir(parents=True, exist_ok=True)

        with open(acct_dir / "transactions.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=PER_ACCOUNT_TX_FIELDS)
            w.writeheader()
            w.writerows(rows)

        with open(acct_dir / "balances.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=PER_ACCOUNT_BAL_FIELDS)
            w.writeheader()
            w.writerows(balances["per_account"][acct])

    if unmapped:
        with open(out_dir / "unmapped_categories.csv", "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Category Group","Category"])
            for sg, sc in sorted(unmapped):
                w.writerow([sg, sc])
        print(f"⚠️ {len(unmapped)} unmapped categories written to unmapped_categories.csv")

    print(f"✅ {len(tx_out)} transactions → transactions_mapped.csv")
    print(f"✅ {len(balances['global'])} balances → balances_mapped.csv")
    print(f"✅ Per-account CSVs in {out_dir}/<account_name>/")


# ------------------------------
# CLI
# ------------------------------

def main():
    """
    Command-line interface entry point.
    """
    ap = argparse.ArgumentParser(
        description="YNAB Register → Monarch CSVs (transactions + balances)"
    )
    ap.add_argument(
        "--ynab-register",
        required=True,
        help="Path to the YNAB register CSV export (all accounts to migrate)"
    )
    ap.add_argument(
        "--category-mapping",
        required=True,
        help="Path to the JSON file defining YNAB → Monarch category mappings"
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        help="Directory where output CSVs will be written"
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate mappings and splits without writing CSV outputs"
    )
    args = ap.parse_args()

    convert_register(
        Path(args.ynab_register),
        Path(args.category_mapping),
        Path(args.out_dir),
        args.dry_run
    )



if __name__ == "__main__":
    main()
