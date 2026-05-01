import os
import glob
import pandas as pd
import sqlite3

# -----------------------------
# File settings
# -----------------------------
DB_FILE = "npi.db"

# Folder that contains npi_part_ab, npi_part_ac, etc.
# If files are in the same folder as this script, keep "."
DATA_FOLDER = "."

TAXONOMY_CSV = "nucc_taxonomy_250.csv"

CHUNKSIZE = 100_000


# -----------------------------
# Remove old DB to avoid duplicates
# -----------------------------
if os.path.exists(DB_FILE):
    print(f"Removing old database: {DB_FILE}")
    os.remove(DB_FILE)


# -----------------------------
# Connect to SQLite
# -----------------------------
conn = sqlite3.connect(DB_FILE)


# -----------------------------
# Load split NPI files
# -----------------------------
npi_files = sorted(glob.glob(os.path.join(DATA_FOLDER, "npi_part_*")))

if not npi_files:
    raise FileNotFoundError(
        "No npi_part_* files found. Make sure the split files are in the correct folder."
    )

print(f"Found {len(npi_files)} NPI part files.")

first_chunk = True

for file_path in npi_files:
    print(f"\nLoading file: {file_path}")

    for i, chunk in enumerate(
        pd.read_csv(
            file_path,
            chunksize=CHUNKSIZE,
            dtype=str,
            low_memory=False
        )
    ):
        chunk.columns = chunk.columns.str.strip()
        chunk = chunk.fillna("")

        chunk.to_sql(
            "npi_providers",
            conn,
            if_exists="replace" if first_chunk else "append",
            index=False
        )

        first_chunk = False

        print(f"  Loaded chunk {i + 1}")


print("\nNPI provider table created.")


# -----------------------------
# Load taxonomy lookup
# -----------------------------
if not os.path.exists(TAXONOMY_CSV):
    raise FileNotFoundError(
        f"Taxonomy file not found: {TAXONOMY_CSV}"
    )

print("\nLoading taxonomy lookup...")

taxonomy_df = pd.read_csv(TAXONOMY_CSV, dtype=str).fillna("")
taxonomy_df.columns = taxonomy_df.columns.str.strip()

taxonomy_df["search_text"] = (
    taxonomy_df["Code"] + " " +
    taxonomy_df["Grouping"] + " " +
    taxonomy_df["Classification"] + " " +
    taxonomy_df["Specialization"] + " " +
    taxonomy_df["Display Name"]
).str.lower()

taxonomy_df.to_sql(
    "taxonomy_lookup",
    conn,
    if_exists="replace",
    index=False
)

print("taxonomy_lookup table created:", taxonomy_df.shape)


# -----------------------------
# Create indexes for faster AI agent queries
# -----------------------------
print("\nCreating indexes...")

indexes = [
    'CREATE INDEX IF NOT EXISTS idx_npi ON npi_providers("NPI");',
    'CREATE INDEX IF NOT EXISTS idx_entity_type ON npi_providers("Entity Type Code");',
    'CREATE INDEX IF NOT EXISTS idx_state ON npi_providers("Provider Business Practice Location Address State Name");',
    'CREATE INDEX IF NOT EXISTS idx_city ON npi_providers("Provider Business Practice Location Address City Name");',
    'CREATE INDEX IF NOT EXISTS idx_zip ON npi_providers("Provider Business Practice Location Address Postal Code");',
    'CREATE INDEX IF NOT EXISTS idx_last_name ON npi_providers("Provider Last Name (Legal Name)");',
    'CREATE INDEX IF NOT EXISTS idx_org_name ON npi_providers("Provider Organization Name (Legal Business Name)");',

    'CREATE INDEX IF NOT EXISTS idx_tax1 ON npi_providers("Healthcare Provider Taxonomy Code_1");',
    'CREATE INDEX IF NOT EXISTS idx_tax2 ON npi_providers("Healthcare Provider Taxonomy Code_2");',
    'CREATE INDEX IF NOT EXISTS idx_tax3 ON npi_providers("Healthcare Provider Taxonomy Code_3");',

    'CREATE INDEX IF NOT EXISTS idx_primary1 ON npi_providers("Healthcare Provider Primary Taxonomy Switch_1");',
    'CREATE INDEX IF NOT EXISTS idx_primary2 ON npi_providers("Healthcare Provider Primary Taxonomy Switch_2");',
    'CREATE INDEX IF NOT EXISTS idx_primary3 ON npi_providers("Healthcare Provider Primary Taxonomy Switch_3");',

    'CREATE INDEX IF NOT EXISTS idx_tax_code ON taxonomy_lookup("Code");',
    'CREATE INDEX IF NOT EXISTS idx_tax_grouping ON taxonomy_lookup("Grouping");',
    'CREATE INDEX IF NOT EXISTS idx_tax_classification ON taxonomy_lookup("Classification");'
]

for sql in indexes:
    conn.execute(sql)

conn.commit()
conn.close()

print("\nDone. Database created:", DB_FILE)
