#!/usr/bin/env python3
"""
CSV Splitter Utility
--------------------

Splits a large CSV file into smaller CSV files with a maximum number of rows,
preserving the header row in each output file.

Each output file will be written to a subdirectory named <input_stem>_split
(e.g., if the input file is transactions.csv, output files go into transactions_split/).

Example:
    python split_csv.py --input transactions.csv --max-rows 3000

Arguments:
    --input     Path to the input CSV file
    --max-rows  Maximum rows (excluding header) per split file [default: 3000]

Output:
    - Creates one or more CSV files, each with the same header as the input.
    - Files are named <input_stem>_partN.csv inside the split directory.

Notes:
    - The header row is preserved in every output file.
    - Row count is strictly enforced; no logic to keep split groups together.
"""

import csv
import argparse
from pathlib import Path

def split_csv(input_file: Path, max_rows: int = 3000) -> None:
    input_file = Path(input_file)
    out_dir = input_file.parent / f"{input_file.stem}_split"
    out_dir.mkdir(exist_ok=True)

    with open(input_file, "r", encoding="utf-8-sig", newline="") as f:
        reader = list(csv.reader(f))

    if not reader:
        print("⚠️ Empty CSV file")
        return

    header, rows = reader[0], reader[1:]
    total_rows = len(rows)
    file_count = (total_rows // max_rows) + (1 if total_rows % max_rows else 0)

    for i in range(file_count):
        chunk = rows[i * max_rows : (i + 1) * max_rows]
        out_path = out_dir / f"{input_file.stem}_part{i+1}.csv"
        with open(out_path, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(chunk)
        print(f"✅ Wrote {len(chunk)} rows to {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Split large CSVs into chunks with header preserved.")
    ap.add_argument("--input", required=True, help="Path to the CSV file")
    ap.add_argument("--max-rows", type=int, default=3000, help="Maximum rows per split (excluding header)")
    args = ap.parse_args()

    split_csv(Path(args.input), args.max_rows)

if __name__ == "__main__":
    main()
