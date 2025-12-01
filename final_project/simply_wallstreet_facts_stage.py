import pandas as pd
import numpy as np

# Load the CSV
df = pd.read_csv("simply_wallstreet_fact.csv")

# Replace empty strings and strings with only whitespace with NaN
df = df.replace(r'^\s*$', np.nan, regex=True)

# Optionally, you can replace Python NaN with SQL-friendly NULL when exporting
# Pandas will automatically write empty fields as blank, which SQL interprets as NULL
df.to_csv("simply_wallstreet_facts_clean.csv", index=False, na_rep="NULL")

print("Cleaning complete. Saved as simply_wallstreet_facts_clean.csv")
