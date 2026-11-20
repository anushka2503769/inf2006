import pandas as pd

df = pd.read_csv("all_stocks_5yr.csv", encoding="utf-8-sig")

# Convert the existing date column in place.
df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

# Keep rows from every year except 2018.
year_to_exclude = 2018
filtered_df = df.loc[df["date"].dt.year != year_to_exclude]

# Save with the original columns and no index column.
filtered_df.to_csv("all_stocks_5yr.csv", date_format="%Y-%m-%d", index=False)