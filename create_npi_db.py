import pandas as pd
import sqlite3

CSV_FILE = "npidata_pfile_20050523-20260412.csv"
DB_FILE = "npi.db"

# Read CSV in chunks because the file is large
chunksize = 100_000

conn = sqlite3.connect(DB_FILE)

for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=chunksize, low_memory=False)):
    chunk.columns = chunk.columns.str.strip()

    chunk.to_sql(
        "npi_providers",
        conn,
        if_exists="append",
        index=False
    )

    print(f"Loaded chunk {i + 1}")

conn.close()

print("Done. Database created:", DB_FILE)

TAXONOMY_CSV = "nucc_taxonomy_250.csv"

taxonomy_df = pd.read_csv(TAXONOMY_CSV, dtype=str)
taxonomy_df = taxonomy_df.fillna("")

taxonomy_df["search_text"] = (
    taxonomy_df["Code"] + " " +
    taxonomy_df["Grouping"] + " " +
    taxonomy_df["Classification"] + " " +
    taxonomy_df["Specialization"] + " " +
    taxonomy_df["Display Name"]
).str.lower()

conn = sqlite3.connect(DB_FILE)

taxonomy_df.to_sql(
    "taxonomy_lookup",
    conn,
    if_exists="replace",
    index=False
)

conn.close()

print("taxonomy_lookup table created:", taxonomy_df.shape)
