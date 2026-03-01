from pathlib import Path

output_dir = Path("output")
csv_files = sorted(output_dir.glob("*.csv"))

print("Dataset Record Counts")
print("=" * 50)
print()

total_records = 0
for csv_file in csv_files:
    try:
        # Count lines instead of loading entire file for efficiency
        with open(csv_file, "r", encoding="utf-8") as f:
            count = sum(1 for _ in f) - 1  # Subtract header row
        total_records += count
        print(f"{csv_file.name:35s} {count:>12,} records")
    except Exception as e:
        print(f"{csv_file.name:35s} ERROR: {e}")

print()
print("=" * 50)
print(f"{'Total Records':35s} {total_records:>12,}")
