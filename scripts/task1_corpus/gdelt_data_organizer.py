import os
import re
import shutil

# Folder containing existing extracted GDELT data
SOURCE_DIR = "gdelt_data"

# New folder for the organised data
DEST_DIR = "gdelt_organized"

MONTHS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]

os.makedirs(DEST_DIR, exist_ok=True)

for root, dirs, files in os.walk(SOURCE_DIR):
    # Ignore destination if it is inside the source directory
    dirs[:] = [
        d for d in dirs
        if os.path.abspath(os.path.join(root, d))
        != os.path.abspath(DEST_DIR)
    ]

    for filename in files:
        # Process CSV files only
        if not filename.lower().endswith(".csv"):
            continue

        # Identify daily, monthly or yearly filenames
        match = re.match(r"^(\d{8}|\d{6}|\d{4})", filename)

        if not match:
            continue

        date_part = match.group(1)

        if len(date_part) == 8:
            # Daily file: 20131214.export.CSV
            year = date_part[:4]
            month = int(date_part[4:6])

        elif len(date_part) == 6:
            # Monthly file: 201303.export.CSV
            year = date_part[:4]
            month = int(date_part[4:6])

        else:
            # Yearly file: 2005.export.CSV
            year = date_part
            month = None

        # Create year and month folders
        if month is not None and 1 <= month <= 12:
            target_folder = os.path.join(
                DEST_DIR, year, MONTHS[month - 1]
            )
        elif month is None:
            target_folder = os.path.join(DEST_DIR, year)
        else:
            print("Skipping invalid date:", filename)
            continue

        os.makedirs(target_folder, exist_ok=True)

        source_path = os.path.join(root, filename)
        target_path = os.path.join(target_folder, filename)

        # Avoid overwriting files with the same name
        if os.path.exists(target_path):
            print("Already exists, skipping:", filename)
            continue

        shutil.move(source_path, target_path)
        print("Organised:", year, filename)

print("\nAll files have been organised!")
print("Output folder:", os.path.abspath(DEST_DIR))
