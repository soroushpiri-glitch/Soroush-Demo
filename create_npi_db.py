{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "d9237431-4494-459b-b4e4-696c6e1b1c93",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Loaded chunk 1\n",
      "Done. Database created: npi.db\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import sqlite3\n",
    "\n",
    "CSV_FILE = \"npidata_pfile_20050523-20260412.csv\"\n",
    "DB_FILE = \"npi.db\"\n",
    "\n",
    "# Read CSV in chunks because the file is large\n",
    "chunksize = 100_000\n",
    "\n",
    "conn = sqlite3.connect(DB_FILE)\n",
    "\n",
    "for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=chunksize, low_memory=False)):\n",
    "    chunk.columns = chunk.columns.str.strip()\n",
    "\n",
    "    chunk.to_sql(\n",
    "        \"npi_providers\",\n",
    "        conn,\n",
    "        if_exists=\"append\",\n",
    "        index=False\n",
    "    )\n",
    "\n",
    "    print(f\"Loaded chunk {i + 1}\")\n",
    "\n",
    "conn.close()\n",
    "\n",
    "print(\"Done. Database created:\", DB_FILE)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "d1a38e46-49a3-40a9-b27a-21ad42be2983",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "taxonomy_lookup table created: (879, 9)\n"
     ]
    }
   ],
   "source": [
    "TAXONOMY_CSV = \"nucc_taxonomy_250.csv\"\n",
    "\n",
    "taxonomy_df = pd.read_csv(TAXONOMY_CSV, dtype=str)\n",
    "taxonomy_df = taxonomy_df.fillna(\"\")\n",
    "\n",
    "taxonomy_df[\"search_text\"] = (\n",
    "    taxonomy_df[\"Code\"] + \" \" +\n",
    "    taxonomy_df[\"Grouping\"] + \" \" +\n",
    "    taxonomy_df[\"Classification\"] + \" \" +\n",
    "    taxonomy_df[\"Specialization\"] + \" \" +\n",
    "    taxonomy_df[\"Display Name\"]\n",
    ").str.lower()\n",
    "\n",
    "conn = sqlite3.connect(DB_FILE)\n",
    "\n",
    "taxonomy_df.to_sql(\n",
    "    \"taxonomy_lookup\",\n",
    "    conn,\n",
    "    if_exists=\"replace\",\n",
    "    index=False\n",
    ")\n",
    "\n",
    "conn.close()\n",
    "\n",
    "print(\"taxonomy_lookup table created:\", taxonomy_df.shape)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.14"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
