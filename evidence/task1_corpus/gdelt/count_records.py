import csv

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

folders = [
    BASE_DIR / "gdelt_organised"
]

file_suffix = ".export.csv"
delimiter = "\t"
has_header = False
output_path = BASE_DIR / "record_counts.csv"

for folder in folders:
    if not folder.is_dir():
        raise FileNotFoundError(f"Folder not found: {folder}")

grand_total = 0

with output_path.open("w", encoding="utf-8", newline="") as output:
    writer = csv.writer(output)
    writer.writerow(["folder", "relative_file_path", "record_count"])

    for folder in folders:
        folder_total = 0
        file_count = 0

        for file_path in sorted(folder.rglob("*")):
            if not file_path.is_file() or not file_path.name.lower().endswith(file_suffix):
                continue

            with file_path.open("r", encoding="utf-8-sig", newline="") as source:
                reader = csv.reader(source, delimiter=delimiter)
                if has_header:
                    next(reader, None)

                # Skip blank rows.
                record_count = sum(1 for row in reader if row)

            writer.writerow([
                folder.name,  # folder name only, so no machine path is written
                file_path.relative_to(folder).as_posix(),
                record_count,
            ])
            folder_total += record_count
            file_count += 1

        grand_total += folder_total
        print(f"{folder.name}: {file_count:,} files, {folder_total:,} records")

    writer.writerow(["", "TOTAL", grand_total])

print(f"\nGrand total: {grand_total:,} records")
print(f"Saved to: {output_path.name}")