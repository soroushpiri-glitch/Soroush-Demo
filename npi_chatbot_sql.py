import os
import re
import json
import boto3
import sqlite3
import pandas as pd
import streamlit as st


DB_FILE = "npi.db"


def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except KeyError:
        if default is not None:
            return default
        st.error(f"Missing Streamlit secret: {key}")
        st.write("Available secrets:", list(st.secrets.keys()))
        st.stop()


AWS_REGION = get_secret("AWS_REGION", "us-east-2")
BEDROCK_MODEL_ID = get_secret("BEDROCK_MODEL_ID", "us.amazon.nova-lite-v1:0")


def db_has_required_tables():
    if not os.path.exists(DB_FILE):
        return False

    try:
        conn = sqlite3.connect(DB_FILE)

        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table';",
            conn
        )["name"].tolist()

        if "npi_providers" not in tables or "taxonomy_lookup" not in tables:
            conn.close()
            return False

        npi_count = pd.read_sql_query(
            "SELECT COUNT(*) AS n FROM npi_providers;",
            conn
        )["n"].iloc[0]

        tax_count = pd.read_sql_query(
            "SELECT COUNT(*) AS n FROM taxonomy_lookup;",
            conn
        )["n"].iloc[0]

        conn.close()
        return npi_count > 0 and tax_count > 0

    except Exception:
        return False


def setup_database():
    if db_has_required_tables():
        return

    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=get_secret("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=get_secret("AWS_SECRET_ACCESS_KEY")
    )

    s3.download_file(
        get_secret("S3_BUCKET"),
        get_secret("S3_NPI_KEY"),
        "npi_data.csv"
    )

    s3.download_file(
        get_secret("S3_BUCKET"),
        get_secret("S3_TAXONOMY_KEY"),
        "taxonomy.csv"
    )

    conn = sqlite3.connect(DB_FILE)

    df = pd.read_csv("npi_data.csv", low_memory=False)
    df.columns = df.columns.str.strip()
    df.to_sql("npi_providers", conn, if_exists="replace", index=False)

    tax = pd.read_csv("taxonomy.csv", dtype=str).fillna("")
    tax.columns = tax.columns.str.strip()

    tax["search_text"] = (
        tax["Code"] + " " +
        tax["Grouping"] + " " +
        tax["Classification"] + " " +
        tax["Specialization"] + " " +
        tax["Display Name"]
    ).str.lower()

    tax.to_sql("taxonomy_lookup", conn, if_exists="replace", index=False)

    conn.close()


setup_database()


bedrock = boto3.client(
    "bedrock-runtime",
    region_name=AWS_REGION,
    aws_access_key_id=get_secret("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=get_secret("AWS_SECRET_ACCESS_KEY")
)


def run_query(sql, params=None):
    conn = sqlite3.connect(DB_FILE)
    result = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return result


def strip_thinking(text):
    if not text:
        return text

    return re.sub(
        r"<thinking>.*?</thinking>",
        "",
        text,
        flags=re.DOTALL | re.IGNORECASE
    ).strip()


def normalize_specialty(s):
    if not s:
        return s

    s = s.lower().strip()

    aliases = {
        "oncology": "oncology",
        "oncologist": "oncology",
        "oncologists": "oncology",
        "medical oncologist": "oncology",
        "medical oncologists": "oncology",
        "cancer doctor": "oncology",
        "cancer doctors": "oncology",
        "cancer specialist": "oncology",
        "cancer specialists": "oncology",
        "cancer physician": "oncology",
        "tumor doctor": "oncology",

        "cardiology": "cardiovascular disease",
        "cardiologist": "cardiovascular disease",
        "cardiologists": "cardiovascular disease",
        "heart doctor": "cardiovascular disease",
        "heart doctors": "cardiovascular disease",
        "heart specialist": "cardiovascular disease",
        "heart failure specialist": "cardiovascular disease",

        "primary care": "internal medicine",
        "primary care doctor": "internal medicine",
        "internist": "internal medicine",
        "internal medicine": "internal medicine",

        "pediatrician": "pediatrics",
        "pediatricians": "pediatrics",
        "children doctor": "pediatrics",
        "kids doctor": "pediatrics",
        "pediatrics": "pediatrics",

        "skin doctor": "dermatology",
        "skin specialist": "dermatology",
        "dermatologist": "dermatology",
        "dermatologists": "dermatology",
        "dermatology": "dermatology",

        "brain doctor": "neurology",
        "neurologist": "neurology",
        "neurologists": "neurology",
        "neurology": "neurology",

        "bone doctor": "orthopaedic surgery",
        "orthopedic": "orthopaedic surgery",
        "orthopedic surgeon": "orthopaedic surgery",
        "orthopedics": "orthopaedic surgery",
        "orthopaedic": "orthopaedic surgery",
        "orthopaedic surgeon": "orthopaedic surgery",

        "kidney doctor": "nephrology",
        "kidney specialist": "nephrology",
        "nephrologist": "nephrology",
        "nephrology": "nephrology",

        "lung doctor": "pulmonary disease",
        "pulmonologist": "pulmonary disease",
        "pulmonary": "pulmonary disease",

        "diabetes doctor": "endocrinology",
        "hormone specialist": "endocrinology",
        "endocrinologist": "endocrinology",
        "endocrinology": "endocrinology",

        "psychiatrist": "psychiatry",
        "mental health doctor": "psychiatry",
        "psych doctor": "psychiatry",

        "psychologist": "psychologist",
        "therapist": "psychologist",

        "obgyn": "obstetrics & gynecology",
        "ob-gyn": "obstetrics & gynecology",
        "gynecologist": "obstetrics & gynecology",
        "women's doctor": "obstetrics & gynecology",

        "dentist": "dentist",
        "teeth doctor": "dentist",

        "nurse practitioner": "nurse practitioner",
        "np": "nurse practitioner",
        "physician assistant": "physician assistant",
        "pa": "physician assistant"
    }

    return aliases.get(s, s)


def normalize_state(state):
    if not state:
        return state

    state = state.strip()

    if len(state) == 2:
        return state.upper()

    state_lower = state.lower()

    state_map = {
        "new jersey": "NJ",
        "new york": "NY",
        "maryland": "MD",
        "california": "CA",
        "texas": "TX",
        "pennsylvania": "PA",
        "virginia": "VA",
        "district of columbia": "DC",
        "washington dc": "DC",
        "washington, dc": "DC",
        "dc": "DC"
    }

    return state_map.get(state_lower, state.upper())


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
    keyword = normalize_specialty(keyword)

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
    taxonomy_code=None,
    entity_type=None,
    limit=20
):
    sql = """
    SELECT
        NPI,
        "Entity Type Code" AS Entity_Type_Code,
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
        params.append(normalize_state(state))

    if city:
        sql += ' AND "Provider Business Practice Location Address City Name" LIKE ?'
        params.append(f"%{city}%")

    if entity_type:
        entity_type = entity_type.lower()

        if entity_type in ["individual", "individuals", "person", "people"]:
            sql += ' AND "Entity Type Code" = ?'
            params.append(1)

        elif entity_type in ["organization", "organizations", "company", "companies"]:
            sql += ' AND "Entity Type Code" = ?'
            params.append(2)

    if taxonomy_code:
        taxonomy_conditions = []

        for i in range(1, 16):
            col = f"Healthcare Provider Taxonomy Code_{i}"
            taxonomy_conditions.append(f'"{col}" = ?')

        sql += " AND (" + " OR ".join(taxonomy_conditions) + ")"

        for _ in range(15):
            params.append(taxonomy_code)

    elif specialty:
        specialty = normalize_specialty(specialty)
        taxonomy_matches = search_taxonomy_codes(specialty, limit=200)

        if taxonomy_matches.empty or "Code" not in taxonomy_matches.columns:
            return pd.DataFrame()

        codes = taxonomy_matches["Code"].dropna().astype(str).unique().tolist()

        if not codes:
            return pd.DataFrame()

        taxonomy_conditions = []

        for i in range(1, 16):
            col = f"Healthcare Provider Taxonomy Code_{i}"
            placeholders = ",".join(["?"] * len(codes))
            taxonomy_conditions.append(f'"{col}" IN ({placeholders})')

        sql += " AND (" + " OR ".join(taxonomy_conditions) + ")"

        for _ in range(15):
            params.extend(codes)

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


def count_providers_by_city(state=None, specialty=None, limit=20):
    sql = """
    SELECT
        "Provider Business Practice Location Address City Name" AS City,
        "Provider Business Practice Location Address State Name" AS State,
        COUNT(*) AS Provider_Count
    FROM npi_providers
    WHERE 1=1
    """

    params = []

    if state:
        sql += ' AND "Provider Business Practice Location Address State Name" = ?'
        params.append(normalize_state(state))

    if specialty:
        taxonomy_matches = search_taxonomy_codes(specialty, limit=200)

        if taxonomy_matches.empty or "Code" not in taxonomy_matches.columns:
            return pd.DataFrame()

        codes = taxonomy_matches["Code"].dropna().astype(str).unique().tolist()

        if not codes:
            return pd.DataFrame()

        taxonomy_conditions = []

        for i in range(1, 16):
            col = f"Healthcare Provider Taxonomy Code_{i}"
            placeholders = ",".join(["?"] * len(codes))
            taxonomy_conditions.append(f'"{col}" IN ({placeholders})')

        sql += " AND (" + " OR ".join(taxonomy_conditions) + ")"

        for _ in range(15):
            params.extend(codes)

    sql += """
    GROUP BY City, State
    ORDER BY Provider_Count DESC
    LIMIT ?
    """

    params.append(limit)
    return run_query(sql, params)


def count_providers_by_taxonomy(state=None, city=None, limit=20):
    sql = """
    SELECT
        p."Healthcare Provider Taxonomy Code_1" AS Taxonomy_Code,
        t."Display Name" AS Taxonomy_Display_Name,
        t.Classification,
        t.Specialization,
        COUNT(*) AS Provider_Count
    FROM npi_providers p
    LEFT JOIN taxonomy_lookup t
        ON p."Healthcare Provider Taxonomy Code_1" = t.Code
    WHERE p."Healthcare Provider Taxonomy Code_1" IS NOT NULL
    """

    params = []

    if state:
        sql += ' AND p."Provider Business Practice Location Address State Name" = ?'
        params.append(normalize_state(state))

    if city:
        sql += ' AND p."Provider Business Practice Location Address City Name" LIKE ?'
        params.append(f"%{city}%")

    sql += """
    GROUP BY Taxonomy_Code, Taxonomy_Display_Name, Classification, Specialization
    ORDER BY Provider_Count DESC
    LIMIT ?
    """

    params.append(limit)
    return run_query(sql, params)


def compare_specialty_between_states(specialty, states, limit=50):
    if not specialty or not states:
        return pd.DataFrame()

    taxonomy_matches = search_taxonomy_codes(specialty, limit=200)

    if taxonomy_matches.empty or "Code" not in taxonomy_matches.columns:
        return pd.DataFrame()

    codes = taxonomy_matches["Code"].dropna().astype(str).unique().tolist()

    if not codes:
        return pd.DataFrame()

    states = [normalize_state(s) for s in states]
    state_placeholders = ",".join(["?"] * len(states))

    taxonomy_conditions = []
    params = states.copy()

    for i in range(1, 16):
        col = f"Healthcare Provider Taxonomy Code_{i}"
        placeholders = ",".join(["?"] * len(codes))
        taxonomy_conditions.append(f'"{col}" IN ({placeholders})')

    for _ in range(15):
        params.extend(codes)

    sql = f"""
    SELECT
        "Provider Business Practice Location Address State Name" AS State,
        COUNT(*) AS Provider_Count
    FROM npi_providers
    WHERE "Provider Business Practice Location Address State Name" IN ({state_placeholders})
    AND ({ " OR ".join(taxonomy_conditions) })
    GROUP BY State
    ORDER BY Provider_Count DESC
    LIMIT ?
    """

    params.append(limit)
    return run_query(sql, params)


def provider_type_breakdown(state=None, city=None):
    sql = """
    SELECT
        "Entity Type Code" AS Entity_Type_Code,
        COUNT(*) AS Provider_Count
    FROM npi_providers
    WHERE 1=1
    """

    params = []

    if state:
        sql += ' AND "Provider Business Practice Location Address State Name" = ?'
        params.append(normalize_state(state))

    if city:
        sql += ' AND "Provider Business Practice Location Address City Name" LIKE ?'
        params.append(f"%{city}%")

    sql += """
    GROUP BY "Entity Type Code"
    ORDER BY Provider_Count DESC
    """

    return run_query(sql, params)


def df_to_json_records(result_df, max_rows=5):
    if result_df is None or result_df.empty:
        return {
            "rows": [],
            "message": "No matching records found."
        }

    clean_df = result_df.head(max_rows).copy()
    clean_df = clean_df.where(pd.notnull(clean_df), None)

    records = clean_df.to_dict(orient="records")
    safe_records = json.loads(json.dumps(records, default=str))

    return {
        "rows": safe_records,
        "row_count_returned": len(safe_records)
    }


def format_tool_result(tool_name, tool_result):
    rows = tool_result.get("rows", [])

    if not rows:
        return tool_result.get("message", "No matching records found.")

    lines = [f"Found {len(rows)} matching record(s):"]

    for row in rows:
        if tool_name == "search_providers":
            name_parts = [
                row.get("Provider First Name"),
                row.get("Provider Last Name (Legal Name)")
            ]

            name = " ".join([x for x in name_parts if x])
            org = row.get("Provider Organization Name (Legal Business Name)")
            city = row.get("City")
            state = row.get("State")
            npi = row.get("NPI")
            tax = row.get("Taxonomy_1")

            display_name = name if name else org if org else "Unknown provider"

            lines.append(
                f"- {display_name} | NPI: {npi} | {city}, {state} | Taxonomy: {tax}"
            )

        elif tool_name == "search_taxonomy_codes":
            lines.append(
                f"- {row.get('Code')} | {row.get('Classification')} | "
                f"{row.get('Specialization')} | {row.get('Display Name')}"
            )

        elif tool_name == "count_providers_by_state":
            lines.append(
                f"- {row.get('State')}: {row.get('Provider_Count')}"
            )

        elif tool_name == "find_provider_by_npi":
            lines.append(str(row))

        elif tool_name == "count_providers_by_city":
            lines.append(
                f"- {row.get('City')}, {row.get('State')}: {row.get('Provider_Count')}"
            )

        elif tool_name == "count_providers_by_taxonomy":
            lines.append(
                f"- {row.get('Taxonomy_Code')} | {row.get('Taxonomy_Display_Name')} | "
                f"{row.get('Classification')} | {row.get('Specialization')} | "
                f"Count: {row.get('Provider_Count')}"
            )

        elif tool_name == "compare_specialty_between_states":
            lines.append(
                f"- {row.get('State')}: {row.get('Provider_Count')}"
            )

        elif tool_name == "provider_type_breakdown":
            entity = row.get("Entity_Type_Code")
            label = (
                "Individual Provider"
                if str(entity) == "1"
                else "Organization Provider"
                if str(entity) == "2"
                else "Unknown"
            )

            lines.append(
                f"- {label} Entity Type {entity}: {row.get('Provider_Count')}"
            )

        else:
            lines.append(str(row))

    return "\n".join(lines)


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
                "description": "Search the NUCC healthcare taxonomy table by specialty, provider type, classification, specialization, or display name.",
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
                                "description": "Healthcare specialty or taxonomy keyword"
                            },
                            "taxonomy_code": {
                                "type": "string",
                                "description": "Exact taxonomy code such as 207RH0003X"
                            },
                            "entity_type": {
                                "type": "string",
                                "description": "Use individual or organization"
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
        },
        {
            "toolSpec": {
                "name": "count_providers_by_city",
                "description": "Count providers grouped by city, optionally filtered by state and specialty.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "state": {
                                "type": "string",
                                "description": "Two-letter state abbreviation such as MD, NY, CA"
                            },
                            "specialty": {
                                "type": "string",
                                "description": "Healthcare specialty such as oncology, cardiology, pediatrics, or dermatology"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of cities to return"
                            }
                        }
                    }
                }
            }
        },
        {
            "toolSpec": {
                "name": "count_providers_by_taxonomy",
                "description": "Count providers grouped by taxonomy code, optionally filtered by state and city.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "state": {
                                "type": "string",
                                "description": "Two-letter state abbreviation"
                            },
                            "city": {
                                "type": "string",
                                "description": "City name"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of taxonomy groups to return"
                            }
                        }
                    }
                }
            }
        },
        {
            "toolSpec": {
                "name": "compare_specialty_between_states",
                "description": "Compare the number of providers for a specialty across multiple states.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "specialty": {
                                "type": "string",
                                "description": "Specialty such as oncology, cardiology, dermatology, or pediatrics"
                            },
                            "states": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of two-letter state abbreviations"
                            },
                            "limit": {
                                "type": "integer",
                                "description": "Maximum number of rows to return"
                            }
                        },
                        "required": ["specialty", "states"]
                    }
                }
            }
        },
        {
            "toolSpec": {
                "name": "provider_type_breakdown",
                "description": "Break providers into individual providers versus organization providers using Entity Type Code.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "state": {
                                "type": "string",
                                "description": "Two-letter state abbreviation"
                            },
                            "city": {
                                "type": "string",
                                "description": "City name"
                            }
                        }
                    }
                }
            }
        }
    ]
}


def execute_tool(tool_name, tool_input):
    if tool_name == "find_provider_by_npi":
        result = find_provider_by_npi(tool_input["npi"])
        return df_to_json_records(result, max_rows=5)

    if tool_name == "search_taxonomy_codes":
        result = search_taxonomy_codes(
            keyword=tool_input.get("keyword"),
            limit=tool_input.get("limit", 100)
        )
        return df_to_json_records(result, max_rows=5)

    if tool_name == "search_providers":
        result = search_providers(
            last_name=tool_input.get("last_name"),
            state=tool_input.get("state"),
            city=tool_input.get("city"),
            specialty=tool_input.get("specialty"),
            taxonomy_code=tool_input.get("taxonomy_code"),
            entity_type=tool_input.get("entity_type"),
            limit=tool_input.get("limit", 20)
        )
        return df_to_json_records(result, max_rows=20)

    if tool_name == "count_providers_by_state":
        result = count_providers_by_state(
            limit=tool_input.get("limit", 20)
        )
        return df_to_json_records(result, max_rows=5)

    if tool_name == "count_providers_by_city":
        result = count_providers_by_city(
            state=tool_input.get("state"),
            specialty=tool_input.get("specialty"),
            limit=tool_input.get("limit", 20)
        )
        return df_to_json_records(result, max_rows=20)

    if tool_name == "count_providers_by_taxonomy":
        result = count_providers_by_taxonomy(
            state=tool_input.get("state"),
            city=tool_input.get("city"),
            limit=tool_input.get("limit", 20)
        )
        return df_to_json_records(result, max_rows=20)

    if tool_name == "compare_specialty_between_states":
        result = compare_specialty_between_states(
            specialty=tool_input.get("specialty"),
            states=tool_input.get("states", []),
            limit=tool_input.get("limit", 50)
        )
        return df_to_json_records(result, max_rows=50)

    if tool_name == "provider_type_breakdown":
        result = provider_type_breakdown(
            state=tool_input.get("state"),
            city=tool_input.get("city")
        )
        return df_to_json_records(result, max_rows=10)

    return {
        "rows": [],
        "message": f"Unknown tool: {tool_name}"
    }
    
def bedrock_agent(question, history=None):
    context_text = ""

    if history:
        recent_history = history[-5:]

        context_parts = []
        for item in recent_history:
            context_parts.append(
                f"Previous question: {item.get('question', '')}\n"
                f"Previous answer: {item.get('answer', '')}"
            )

        context_text = "\n\n".join(context_parts)

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": f"""
You are an NPI healthcare provider data assistant.

You have access to previous conversation history. Use it to understand follow-up questions such as:
- those
- them
- that city
- the previous result
- same specialty
- same state
- compare that

Before using a tool, silently rewrite the user's latest question into a complete standalone question using the previous conversation context.

Example:
Previous question: Find oncologists in Maryland
Previous answer: Found providers in Maryland.
Latest question: Show only those in Baltimore
Standalone meaning: Find oncologists in Baltimore, Maryland.

Example:
Previous question: Find oncologists in Maryland
Previous answer: Found providers with taxonomy 207RH0003X.
Latest question: Which of those have taxonomy 207RH0003X only?
Standalone meaning: Find Maryland oncologists with taxonomy 207RH0003X only.

Important rules:
- Do not make up provider information.
- If the user asks about provider data, always use a tool.
- If the user asks about a specialty, use taxonomy-aware tools.
- If the user asks for comparison, use comparison or count tools.
- If the user asks for top cities, provider density, distribution, or rankings, use count tools.
- If the user asks about individual vs organization providers, use provider_type_breakdown.
- If the user asks about taxonomy categories, use search_taxonomy_codes or count_providers_by_taxonomy.
- Convert state names to two-letter abbreviations when using tools.
- Keep answers concise and plain English.
- Do not output hidden reasoning, chain-of-thought, or <thinking> tags.
- If no matching records are found, clearly say that.

Previous conversation history:
{context_text}

Latest user question:
{question}
"""
                }
            ]
        }
    ]

    try:
        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=messages,
            toolConfig=tool_config,
            inferenceConfig={
                "maxTokens": 800,
                "temperature": 0.1
            }
        )
    except Exception as e:
        return f"Bedrock first response error: {type(e).__name__}: {str(e)}"

    output_message = response["output"]["message"]

    for content_block in output_message.get("content", []):
        if "toolUse" in content_block:
            tool_use = content_block["toolUse"]
            tool_name = tool_use["name"]
            tool_input = tool_use.get("input", {})

            tool_result = execute_tool(tool_name, tool_input)

            return format_tool_result(tool_name, tool_result)

    if output_message.get("content"):
        text = output_message["content"][0].get("text", "No response generated.")
        return strip_thinking(text)

    return "No response generated."


if __name__ == "__main__":
    while True:
        question = input("\nAsk about NPI data, or type 'quit': ")

        if question.lower() == "quit":
            break

        answer = bedrock_agent(question)

        print("\nAnswer:")
        print(answer)
