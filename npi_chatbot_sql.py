import os
import boto3
import sqlite3
import pandas as pd
import streamlit as st

DB_FILE="npi.db"


def setup_database():

    # only build once
    if os.path.exists(DB_FILE):
        return

    s3 = boto3.client(
        "s3",
        region_name=st.secrets["AWS_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
    )

    # download from S3
    s3.download_file(
        st.secrets["S3_BUCKET"],
        st.secrets["S3_NPI_KEY"],
        "npi_data.csv"
    )

    s3.download_file(
        st.secrets["S3_BUCKET"],
        st.secrets["S3_TAXONOMY_KEY"],
        "taxonomy.csv"
    )

    conn = sqlite3.connect(DB_FILE)

    # build provider table
    df = pd.read_csv(
        "npi_data.csv",
        low_memory=False
    )

    df.to_sql(
        "npi_providers",
        conn,
        if_exists="replace",
        index=False
    )

    # build taxonomy lookup
    tax = pd.read_csv(
        "taxonomy.csv",
        dtype=str
    ).fillna("")

    tax["search_text"] = (
        tax["Code"] + " " +
        tax["Grouping"] + " " +
        tax["Classification"] + " " +
        tax["Specialization"] + " " +
        tax["Display Name"]
    ).str.lower()

    tax.to_sql(
        "taxonomy_lookup",
        conn,
        if_exists="replace",
        index=False
    )

    conn.close()


setup_database()

AWS_REGION = st.secrets.get("AWS_REGION", "us-east-2")
BEDROCK_MODEL_ID = st.secrets.get("BEDROCK_MODEL_ID", "us.amazon.nova-lite-v1:0")

bedrock = boto3.client(
    "bedrock-runtime",
    region_name=AWS_REGION,
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
)
# -----------------------------
# SQL helper
# -----------------------------
def run_query(sql, params=None):
    conn = sqlite3.connect(DB_FILE)
    result = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return result


# -----------------------------
# SQL tools
# -----------------------------
def find_provider_by_npi(npi):
    sql = """
    SELECT 
        NPI,
        "Entity Type Code",
        "Provider Organization Name (Legal Business Name)",
        "Provider First Name",
        "Provider Last Name (Legal Name)",
        "Provider Business Practice Location Address City Name",
        "Provider Business Practice Location Address State Name",
        "Healthcare Provider Taxonomy Code_1",
        "Certification Date"
    FROM npi_providers
    WHERE CAST(NPI AS TEXT) = ?
    LIMIT 1
    """
    return run_query(sql, [str(npi)])


def search_taxonomy_codes(keyword, limit=100):
    """
    Search the NUCC taxonomy lookup table by keyword.
    Example: oncology, cardiology, nurse practitioner, clinic, dentist.
    """

    sql = """
    SELECT
        Code,
        Grouping,
        Classification,
        Specialization,
        "Display Name"
    FROM taxonomy_lookup
    WHERE search_text LIKE ?
    LIMIT ?
    """

    return run_query(sql, [f"%{keyword.lower()}%", limit])


def search_providers(
    last_name=None,
    state=None,
    city=None,
    specialty=None,
    limit=20
):
    """
    Search providers using optional filters:
    - last name
    - state
    - city
    - specialty/taxonomy
    """

    sql = """
    SELECT
        NPI,
        "Provider First Name",
        "Provider Last Name (Legal Name)",
        "Provider Organization Name (Legal Business Name)",
        "Provider Business Practice Location Address City Name" AS City,
        "Provider Business Practice Location Address State Name" AS State,
        "Healthcare Provider Taxonomy Code_1" AS Taxonomy_1,
        "Healthcare Provider Taxonomy Code_2" AS Taxonomy_2,
        "Healthcare Provider Taxonomy Code_3" AS Taxonomy_3
    FROM npi_providers
    WHERE 1=1
    """

    params = []

    if last_name:
        sql += ' AND "Provider Last Name (Legal Name)" LIKE ?'
        params.append(f"%{last_name}%")

    if state:
        sql += ' AND "Provider Business Practice Location Address State Name" = ?'
        params.append(state.upper())

    if city:
        sql += ' AND "Provider Business Practice Location Address City Name" LIKE ?'
        params.append(f"%{city}%")

    # Advanced specialty search using taxonomy_lookup
    if specialty:
        taxonomy_matches = search_taxonomy_codes(specialty, limit=200)

        if not taxonomy_matches.empty:
            codes = (
                taxonomy_matches["Code"]
                .dropna()
                .unique()
                .tolist()
            )

            taxonomy_conditions = []

            for i in range(1, 16):
                col = f"Healthcare Provider Taxonomy Code_{i}"
                placeholders = ",".join(["?"] * len(codes))
                taxonomy_conditions.append(f'"{col}" IN ({placeholders})')

            sql += " AND (" + " OR ".join(taxonomy_conditions) + ")"

            for _ in range(15):
                params.extend(codes)

        else:
            sql += ' AND "Healthcare Provider Taxonomy Group_1" LIKE ?'
            params.append(f"%{specialty}%")

    sql += " LIMIT ?"
    params.append(limit)

    return run_query(sql, params)


def count_providers_by_state(limit=20):
    sql = """
    SELECT 
        "Provider Business Practice Location Address State Name" AS State,
        COUNT(*) AS Provider_Count
    FROM npi_providers
    GROUP BY "Provider Business Practice Location Address State Name"
    ORDER BY Provider_Count DESC
    LIMIT ?
    """
    return run_query(sql, [limit])


# -----------------------------
# Convert DataFrame to JSON-safe result
# -----------------------------
def df_to_json_records(result_df, max_rows=20):
    if result_df is None or result_df.empty:
        return {
            "rows": [],
            "message": "No matching records found."
        }

    return {
        "rows": result_df.head(max_rows).to_dict(orient="records"),
        "row_count_returned": min(len(result_df), max_rows)
    }


# -----------------------------
# Bedrock tool definitions
# -----------------------------
tool_config = {
    "tools": [
        {
            "toolSpec": {
                "name": "find_provider_by_npi",
                "description": "Find one healthcare provider using a 10-digit NPI number.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "npi": {
                                "type": "string",
                                "description": "10-digit NPI number"
                            }
                        },
                        "required": ["npi"]
                    }
                }
            }
        },
        {
            "toolSpec": {
                "name": "search_taxonomy_codes",
                "description": "Search the full NUCC healthcare taxonomy table by specialty, provider type, classification, specialization, or display name.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "keyword": {
                                "type": "string",
                                "description": "Specialty keyword such as oncology, cardiology, nephrology, nurse practitioner, clinic, dentist, psychologist, radiology, pediatrics, or emergency medicine"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of taxonomy codes to return"
                            }
                        },
                        "required": ["keyword"]
                    }
                }
            }
        },
        {
            "toolSpec": {
                "name": "search_providers",
                "description": "Search healthcare providers by last name, state, city, specialty, or taxonomy-related keyword.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "last_name": {
                                "type": "string",
                                "description": "Provider last name"
                            },
                            "state": {
                                "type": "string",
                                "description": "Two-letter US state abbreviation, such as MD, NY, CA"
                            },
                            "city": {
                                "type": "string",
                                "description": "Provider city"
                            },
                            "specialty": {
                                "type": "string",
                                "description": "Healthcare specialty or taxonomy keyword such as oncology, cardiology, dermatology, pediatrics, internal medicine, nurse practitioner, clinic, dentist, or psychologist"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of provider records to return"
                            }
                        }
                    }
                }
            }
        },
        {
            "toolSpec": {
                "name": "count_providers_by_state",
                "description": "Count healthcare providers grouped by state.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of states to return"
                            }
                        }
                    }
                }
            }
        }
    ]
}


# -----------------------------
# Execute selected tool
# -----------------------------
def execute_tool(tool_name, tool_input):
    if tool_name == "find_provider_by_npi":
        result = find_provider_by_npi(tool_input["npi"])
        return df_to_json_records(result)

    if tool_name == "search_taxonomy_codes":
        result = search_taxonomy_codes(
            keyword=tool_input.get("keyword"),
            limit=tool_input.get("limit", 100)
        )
        return df_to_json_records(result)

    if tool_name == "search_providers":
        result = search_providers(
            last_name=tool_input.get("last_name"),
            state=tool_input.get("state"),
            city=tool_input.get("city"),
            specialty=tool_input.get("specialty"),
            limit=tool_input.get("limit", 20)
        )
        return df_to_json_records(result)

    if tool_name == "count_providers_by_state":
        result = count_providers_by_state(
            limit=tool_input.get("limit", 20)
        )
        return df_to_json_records(result)

    return {"error": f"Unknown tool: {tool_name}"}


# -----------------------------
# Bedrock Agent
# -----------------------------
def bedrock_agent(question):
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": f"""
You are an NPI healthcare provider data assistant.

Use the available tools to answer questions about the local NPI SQLite database.

Important rules:
- Do not make up provider information.
- If the user asks about provider data, use a tool.
- If the user asks about a specialty, pass the specialty keyword to search_providers.
- search_providers will use the taxonomy_lookup table to find matching NUCC taxonomy codes.
- If the user asks about taxonomy codes directly, use search_taxonomy_codes.
- If no matching records are found, clearly say that.
- Keep the answer concise and explain the result in plain English.
- For state names, convert them to two-letter abbreviations when using tools.

User question:
{question}
"""
                }
            ]
        }
    ]

    response = bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        messages=messages,
        toolConfig=tool_config,
        inferenceConfig={
            "maxTokens": 800,
            "temperature": 0.1
        }
    )

    output_message = response["output"]["message"]
    messages.append(output_message)

    for content_block in output_message["content"]:
        if "toolUse" in content_block:
            tool_use = content_block["toolUse"]

            tool_name = tool_use["name"]
            tool_input = tool_use["input"]
            tool_use_id = tool_use["toolUseId"]

            print(f"\n[Bedrock selected tool: {tool_name}]")
            print(f"[Tool input: {tool_input}]")

            tool_result = execute_tool(tool_name, tool_input)

            tool_result_message = {
                "role": "user",
                "content": [
                    {
                        "toolResult": {
                            "toolUseId": tool_use_id,
                            "content": [
                                {
                                    "json": tool_result
                                }
                            ]
                        }
                    }
                ]
            }

            messages.append(tool_result_message)

            final_response = bedrock.converse(
                modelId=BEDROCK_MODEL_ID,
                messages=messages,
                toolConfig=tool_config,
                inferenceConfig={
                    "maxTokens": 800,
                    "temperature": 0.1
                }
            )

            return final_response["output"]["message"]["content"][0]["text"]

    return output_message["content"][0].get("text", "No response generated.")


# -----------------------------
# Chat loop
# -----------------------------
if __name__ == "__main__":
    while True:
        question=input("\nAsk about NPI data, or type 'quit': ")

        if question.lower()=="quit":
            break

        answer=bedrock_agent(question)
        print(answer)
