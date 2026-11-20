from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


# 1. CONFIGURATION

DATA_PATH = "/project/raw/gdelt"

# Report will be saved on the EMR primary node.
REPORT_PATH = "/home/hadoop/gdelt_quality_report.txt"


# 2. DEFINE GDELT COLUMN NAMES

columns = [
    "GlobalEventID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",

    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",

    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",

    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",

    "Actor1Geo_Type",
    "Actor1Geo_Fullname",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",

    "Actor2Geo_Type",
    "Actor2Geo_Fullname",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",

    "ActionGeo_Type",
    "ActionGeo_Fullname",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",

    "DATEADDED",
    "SOURCEURL"
]

# All fields are initially read as strings.
# This prevents Spark from incorrectly inferring data types.

schema = StructType([
    StructField(name, StringType(), True)
    for name in columns
])


# 3. START SPARK SESSION

spark = (
    SparkSession.builder
    .appName("GDELT_Data_Quality_Assessment")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# 4. READ ALL GDELT FILES

df = (
    spark.read
    .schema(schema)
    .option("header", "false")
    .option("sep", "\t")
    .option("recursiveFileLookup", "true")
    .option("mode", "PERMISSIVE")
    .csv(DATA_PATH)
)

# Remove leading and trailing spaces from string values.
# Keep empty values for missing-value assessment.

df = df.select([
    F.trim(F.col(c)).alias(c)
    for c in columns
])

total_records = df.count()
total_columns = len(columns)


# 5. MISSING VALUES

missing_expressions = [
    F.sum(
        F.when(
            F.col(c).isNull() | (F.col(c) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in columns
]

missing_result = df.agg(*missing_expressions).first()

missing_counts = missing_result.asDict()

total_missing = sum(missing_counts.values())


# 6. DUPLICATE RECORDS

duplicate_records = (
    df.groupBy(*columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_record_summary = duplicate_records.agg(
    F.count("*").alias("duplicate_groups"),
    F.sum(F.col("count") - 1).alias("duplicate_rows")
).first()

duplicate_groups = duplicate_record_summary["duplicate_groups"] or 0
duplicate_rows = duplicate_record_summary["duplicate_rows"] or 0


# 7. DUPLICATE GLOBAL EVENT ID

duplicate_event_ids = (
    df.filter(
        F.col("GlobalEventID").isNotNull()
        & (F.col("GlobalEventID") != "")
    )
    .groupBy("GlobalEventID")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_id_summary = duplicate_event_ids.agg(
    F.count("*").alias("duplicate_groups"),
    F.sum(F.col("count") - 1).alias("duplicate_rows")
).first()

duplicate_id_groups = duplicate_id_summary["duplicate_groups"] or 0
duplicate_id_rows = duplicate_id_summary["duplicate_rows"] or 0


# 8. INVALID DATES

# SQLDATE format:    YYYYMMDD
# DATEADDED format:  YYYYMMDD (8 digits). This is the 58-column GDELT 1.0
#                    event file; the 14-digit YYYYMMDDHHMMSS stamp belongs to
#                    GDELT 2.0, which has 61 columns.

invalid_sqldate = (
    F.col("SQLDATE").isNotNull()
    & (F.col("SQLDATE") != "")
    & F.expr(
        "try_to_timestamp(SQLDATE, 'yyyyMMdd')"
    ).isNull()
)

invalid_dateadded = (
    F.col("DATEADDED").isNotNull()
    & (F.col("DATEADDED") != "")
    & (
        ~F.col("DATEADDED").rlike(r"^[0-9]{8}$")
        | F.expr(
            "try_to_timestamp(DATEADDED, 'yyyyMMdd')"
        ).isNull()
    )
)

date_result = df.agg(
    F.sum(
        F.when(invalid_sqldate, 1).otherwise(0)
    ).alias("invalid_sqldate"),

    F.sum(
        F.when(invalid_dateadded, 1).otherwise(0)
    ).alias("invalid_dateadded")
).first()

invalid_sqldate_count = date_result["invalid_sqldate"] or 0
invalid_dateadded_count = date_result["invalid_dateadded"] or 0

total_invalid_dates = (
    invalid_sqldate_count + invalid_dateadded_count
)


# 9. INVALID EVENT CODES

# Check whether event codes follow the expected
# numeric CAMEO code formats.
#
# EventCode: 2 to 4 digits
# EventBaseCode: 2 to 3 digits
# EventRootCode: 2 digits

event_code_rules = {
    "EventCode": r"^[0-9]{2,4}$",
    "EventBaseCode": r"^[0-9]{2,3}$",
    "EventRootCode": r"^[0-9]{2}$"
}

event_code_expressions = []

for column, pattern in event_code_rules.items():

    invalid_condition = (
        F.col(column).isNotNull()
        & (F.col(column) != "")
        & (~F.col(column).rlike(pattern))
    )

    event_code_expressions.append(
        F.sum(
            F.when(invalid_condition, 1).otherwise(0)
        ).alias(column)
    )

event_code_result = df.agg(
    *event_code_expressions
).first()

invalid_event_codes = event_code_result.asDict()

total_invalid_event_codes = sum(
    invalid_event_codes.values()
)


# 10. INVALID NUMERIC VALUES

# Columns that should contain integer values.

integer_columns = [
    "GlobalEventID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "IsRootEvent",
    "QuadClass",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "Actor1Geo_Type",
    "Actor2Geo_Type",
    "ActionGeo_Type",
    "DATEADDED"
]

# The three Geo_FeatureID columns are left out on purpose. 
# They are identifiers, not numbers: they hold numeric IDs (some negative, e.g. -2601889, others positive, e.g. 531871) and text codes (e.g. US, NY, UK, CA). 
# Checking them as integers wrongly flags every text code.

# Columns that should contain decimal/numeric values.

decimal_columns = [
    "FractionDate",
    "GoldsteinScale",
    "AvgTone",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "ActionGeo_Lat",
    "ActionGeo_Long"
]

numeric_columns = integer_columns + decimal_columns

# Additional logical range checks.

numeric_ranges = {
    "IsRootEvent": (0, 1),
    "QuadClass": (1, 4),
    "GoldsteinScale": (-10, 10),
    "NumMentions": (0, None),
    "NumSources": (0, None),
    "NumArticles": (0, None),
    # 0 means "no location" in this dataset (16,421,399 Actor1Geo_Type rows, about the same as the missing Actor1Geo_CountryCode count), so it is accepted. 
    # Only values outside 0 to 5 are invalid.
    "Actor1Geo_Type": (0, 5),
    "Actor2Geo_Type": (0, 5),
    "ActionGeo_Type": (0, 5),
    "Actor1Geo_Lat": (-90, 90),
    "Actor2Geo_Lat": (-90, 90),
    "ActionGeo_Lat": (-90, 90),
    "Actor1Geo_Long": (-180, 180),
    "Actor2Geo_Long": (-180, 180),
    "ActionGeo_Long": (-180, 180)
}

numeric_expressions = []

for column in numeric_columns:

    value = F.col(column)
    numeric_value = value.cast("double")

    # Ignore missing values because they are already counted in the missing-value check.

    invalid_condition = (
        value.isNotNull()
        & (value != "")
        & numeric_value.isNull()
    )

    # Integer columns must contain whole numbers.

    if column in integer_columns:

        invalid_condition = (
            value.isNotNull()
            & (value != "")
            & (~value.rlike(r"^-?[0-9]+$"))
        )

    # Check logical numeric ranges where applicable.

    if column in numeric_ranges:

        minimum, maximum = numeric_ranges[column]

        if minimum is not None:
            invalid_condition = (
                invalid_condition
                | (
                    value.isNotNull()
                    & (value != "")
                    & (numeric_value < minimum)
                )
            )

        if maximum is not None:
            invalid_condition = (
                invalid_condition
                | (
                    value.isNotNull()
                    & (value != "")
                    & (numeric_value > maximum)
                )
            )

    numeric_expressions.append(
        F.sum(
            F.when(invalid_condition, 1).otherwise(0)
        ).alias(column)
    )

numeric_result = df.agg(
    *numeric_expressions
).first()

invalid_numeric_counts = numeric_result.asDict()

total_invalid_numeric = sum(
    invalid_numeric_counts.values()
)


# 11. GENERATE TEXT REPORT

report = []

def add_line(text=""):
    report.append(str(text))


add_line("=" * 65)
add_line("             GDELT DATA QUALITY ASSESSMENT")
add_line("=" * 65)

add_line()
add_line("DATASET INFORMATION")
add_line("-" * 65)

add_line(f"Run time (UTC)     : {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}")
add_line(f"Spark application : {spark.sparkContext.applicationId}")
add_line(f"Spark version     : {spark.version}")
add_line(f"Dataset directory : {DATA_PATH}")
add_line(f"Total records     : {total_records:,}")
add_line(f"Total columns     : {total_columns}")
add_line(f"Expected columns  : 58")

add_line()
add_line("=" * 65)
add_line("QUALITY CHECK FINDINGS")
add_line("=" * 65)


# Missing Values

add_line()
add_line("[1] MISSING VALUES")
add_line("-" * 60)

for column in columns:
    count = missing_counts[column]

    if count > 0:
        add_line(
            f"- {count:,} missing values in the {column} column"
        )

if total_missing == 0:
    add_line("No missing values were found.")


# Duplicate Records

add_line()
add_line("[2] DUPLICATE RECORDS")
add_line("-" * 60)

if duplicate_rows == 0:
    add_line("No duplicate records are found.")
else:
    add_line(
        f"- {duplicate_rows:,} additional duplicate records "
        f"were found across {duplicate_groups:,} groups."
    )

# Duplicate Global Event ID

add_line()
add_line("[3] DUPLICATE GLOBAL EVENT ID")
add_line("-" * 60)

if duplicate_id_rows == 0:
    add_line("No duplicate GlobalEventID values were found.")
else:
    add_line(
        f"- {duplicate_id_groups:,} duplicate GlobalEventID "
        f"groups were found."
    )

    add_line(
        f"- {duplicate_id_rows:,} additional records were "
        f"associated with duplicate GlobalEventID values."
    )


# Invalid Dates

add_line()
add_line("[4] INVALID DATES")
add_line("-" * 60)

if invalid_sqldate_count == 0:
    add_line("No invalid SQLDATE values were found.")
else:
    add_line(
        f"- {invalid_sqldate_count:,} invalid SQLDATE values "
        f"were found."
    )

if invalid_dateadded_count == 0:
    add_line("No invalid DATEADDED values were found.")
else:
    add_line(
        f"- {invalid_dateadded_count:,} invalid DATEADDED "
        f"values were found."
    )

# Invalid Event Codes

add_line()
add_line("[5] INVALID EVENT CODES")
add_line("-" * 60)

for column in event_code_rules:

    count = invalid_event_codes[column]

    if count > 0:
        add_line(
            f"- {count:,} invalid event codes "
            f"in the {column} column"
        )

if total_invalid_event_codes == 0:
    add_line("No invalid event codes were found.")

# Invalid Numeric Values

add_line()
add_line("[6] INVALID NUMERIC VALUES")
add_line("-" * 60)

for column in numeric_columns:

    count = invalid_numeric_counts[column]

    if count > 0:
        add_line(
            f"- {count:,} invalid numeric values "
            f"in the {column} column"
        )

if total_invalid_numeric == 0:
    add_line("No invalid numeric values were found.")

add_line()
add_line("Notes:")
add_line("- Geo_Type = 0 marks rows with no location in this dataset, so 0 is accepted. "
         "Only values outside 0 to 5 are counted as invalid.")
add_line("- The Geo_FeatureID columns are identifiers (numeric IDs, some negative, "
         "or text codes such as US and NY), so they are not checked as numbers.")

# 12. SAVE REPORT AS TXT

add_line()
add_line("=" * 65)
add_line("                 END OF REPORT")
add_line("=" * 65)

with open(REPORT_PATH, "w", encoding="utf-8") as file:

    file.write("\n".join(report))

print("\nData quality assessment completed.")
print(f"Report saved successfully at: {REPORT_PATH}")


# 13. STOP SPARK SESSION

spark.stop()