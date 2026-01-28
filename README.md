# YNAB → Monarch Money Migration

Migrate transaction history from **YNAB** to **Monarch Money** using CSV exports and a JSON mapping file.

## Features

* Map YNAB categories to Monarch categories with **multiple tags**
* Detect and assign **Split Group IDs** for splits
* Handle transfers, starting balances, and reconciliation adjustments
* Normalize dates and amounts
* Generate:

  * Consolidated transactions CSV
  * Consolidated balances CSV
  * Per-account transactions and balances CSVs
* Report unmapped categories
* **Dry-run** mode to preview mappings without writing files

## Requirements

* Python 3.9+
* Standard library only

## Inputs

### Register CSV

Columns: `Date`, `Account`, `Payee`, `Memo`, `Category Group`, `Category`, `Outflow`, `Inflow`

* Supports typical formatting (`$`, `,`, parentheses)
* Transfers appear as `Transfer : Account Name`

### Category Mapping JSON

Array of mappings with optional multiple tags:

```json
[
  {
    "source_group": "Inflow",
    "source_category": "Ready to Assign",
    "monarch_group": "Income",
    "monarch_category": "Paycheck",
    "tags": [],
    "memo": ""
  },
  {
    "source_group": "🛒 VARIABLE",
    "source_category": "🍎 Groceries",
    "monarch_group": "Food & Dining",
    "monarch_category": "Groceries",
    "tags": [],
    "memo": ""
  },
  {
    "source_group": "🎁 GIFTS",
    "source_category": "🎁 Gifts (Joshua)",
    "monarch_group": "Savings Categories",
    "monarch_category": "Gifts",
    "tags": ["Joshua"]
  },
]
```

* Resolution order: Memo-based → Exact → Category-only → Default (Other/Uncategorized)

## Usage

```bash
python3 migrate_ynab_to_monarch.py \
  --ynab-register ynab_register.csv \
  --category-mapping category_mapping.json \
  --out-dir output/ \
  [--dry-run]
```

* `--dry-run`: run without writing any files; prints summary counts

## Outputs

* `transactions_mapped.csv`: Consolidated transactions
* `balances_mapped.csv`: Latest balances per account per day
* `unmapped_categories.csv`: Categories with no mapping
* Per-account directories: `transactions.csv` and `balances.csv`
* Tags exported as comma-separated strings for Monarch import

## Optional Helper: CSV Splitter

For very large CSV files, the `/helpers/split_csv.py` utility can split files into smaller chunks for Monarch import.

Example usage:

```bash
python helpers/split_csv.py --input transactions_mapped.csv --max-rows 3000
```

* Creates a subdirectory `transactions_mapped_split/`
* Generates files named `transactions_mapped_part1.csv`, `transactions_mapped_part2.csv`, etc.
* Preserves the header row in every split file
* Useful for avoiding upload limits in Monarch or other tools

## Notes

* Safe, offline, and deterministic
* Useful for verifying category mappings and transaction conversions
* Account names sanitized (spaces/colons → `_`)
