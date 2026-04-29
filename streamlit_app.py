import streamlit as st
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim, ArcGIS
from geopy.distance import geodesic
from datetime import datetime
from zoneinfo import ZoneInfo
from npi_chatbot_sql import bedrock_agent


st.set_page_config(page_title="NPI Healthcare AI Agent", layout="wide")

st.title("NPI Healthcare Provider AI Agent")


def make_subject(question, max_len=55):
    subject = question.strip()

    prefixes = [
        "find", "show", "what is", "what are", "can you",
        "please", "search for", "give me", "tell me"
    ]

    lower_subject = subject.lower()

    for prefix in prefixes:
        if lower_subject.startswith(prefix):
            subject = subject[len(prefix):].strip()
            break

    if len(subject) > max_len:
        subject = subject[:max_len].strip() + "..."

    return subject if subject else "New chat"


def local_timestamp():
    eastern = ZoneInfo("America/New_York")
    now = datetime.now(eastern)

    return {
        "short": now.strftime("%I:%M %p ET"),
        "full": now.strftime("%Y-%m-%d %I:%M:%S %p ET")
    }


def clean_address_for_geocoding(address):
    if not address:
        return ""

    address = str(address).strip()

    parts = address.split()
    cleaned_parts = []

    for part in parts:
        clean_part = part.strip().replace(",", "")

        if clean_part.isdigit() and len(clean_part) > 5:
            cleaned_parts.append(clean_part[:5])
        else:
            cleaned_parts.append(part)

    cleaned_address = " ".join(cleaned_parts)

    cleaned_address = (
        cleaned_address
        .replace("  ", " ")
        .replace(" ,", ",")
        .strip()
    )

    return cleaned_address


@st.cache_data(show_spinner=False)
def geocode_address(address):
    if not address or not str(address).strip():
        return None, None

    address = str(address).strip()

    fallback_locations = {
        "baltimore": (39.2904, -76.6122),
        "baltimore md": (39.2904, -76.6122),
        "baltimore, md": (39.2904, -76.6122),
        "baltimore maryland": (39.2904, -76.6122),
        "maryland": (39.0458, -76.6413),
        "washington dc": (38.9072, -77.0369),
        "washington, dc": (38.9072, -77.0369),
        "new york": (40.7128, -74.0060),
        "new york ny": (40.7128, -74.0060),
        "new york, ny": (40.7128, -74.0060)
    }

    key = (
        address.lower()
        .replace("usa", "")
        .replace("united states", "")
        .replace(".", "")
        .strip()
    )

    key_no_comma = key.replace(",", " ")
    key_no_comma = " ".join(key_no_comma.split())

    if key in fallback_locations:
        return fallback_locations[key]

    if key_no_comma in fallback_locations:
        return fallback_locations[key_no_comma]

    cleaned_address = clean_address_for_geocoding(address)

    # Try ArcGIS first
    try:
        arcgis = ArcGIS(timeout=10)
        location = arcgis.geocode(cleaned_address)

        if location:
            return location.latitude, location.longitude

    except Exception:
        pass

    # Try Nominatim second
    try:
        nominatim = Nominatim(
            user_agent="npi_healthcare_agent_soroush",
            timeout=15
        )

        location = nominatim.geocode(
            cleaned_address,
            exactly_one=True,
            country_codes="us"
        )

        if location:
            return location.latitude, location.longitude

    except Exception:
        pass

    return None, None


def parse_provider_lines(answer):
    providers = []

    lines = [
        line for line in answer.split("\n")
        if line.strip().startswith("-")
    ]

    for line in lines:
        try:
            clean_line = line.replace("- ", "").strip()
            parts = clean_line.split("|")

            if len(parts) < 5:
                continue

            name = parts[0].strip()
            entity = parts[1].strip()
            npi = parts[2].replace("NPI:", "").strip()
            address = parts[3].strip()
            taxonomy = parts[4].replace("Taxonomy:", "").strip()

            providers.append({
                "name": name,
                "entity": entity,
                "npi": npi,
                "address": address,
                "taxonomy": taxonomy
            })

        except Exception:
            continue

    return providers


def create_provider_map(user_address, providers):
    user_lat, user_lon = geocode_address(user_address)

    if user_lat is None:
        st.error(
            "Could not find your location. Try `Baltimore, MD`, a ZIP code, "
            "or a fuller address such as `2907 Fallstaff Road, Baltimore, MD, USA`."
        )
        return []

    m = folium.Map(location=[user_lat, user_lon], zoom_start=10)

    folium.Marker(
        [user_lat, user_lon],
        popup="Your location",
        tooltip="Your location",
        icon=folium.Icon(color="blue", icon="home")
    ).add_to(m)

    results = []

    for provider in providers[:8]:
        provider_address = provider.get("address")

        if not provider_address:
            continue

        cleaned_provider_address = clean_address_for_geocoding(provider_address)
        lat, lon = geocode_address(cleaned_provider_address)

        if lat is None:
            continue

        distance = geodesic((user_lat, user_lon), (lat, lon)).miles

        result = {
            "Name": provider.get("name"),
            "Entity": provider.get("entity"),
            "NPI": provider.get("npi"),
            "Address": cleaned_provider_address,
            "Taxonomy": provider.get("taxonomy"),
            "Distance (miles)": round(distance, 2)
        }

        results.append(result)

        popup_text = f"""
        <b>{provider.get("name")}</b><br>
        {provider.get("entity")}<br>
        NPI: {provider.get("npi")}<br>
        Address: {cleaned_provider_address}<br>
        Taxonomy: {provider.get("taxonomy")}<br>
        Distance: {round(distance, 2)} miles
        """

        folium.Marker(
            [lat, lon],
            popup=popup_text,
            tooltip=provider.get("name"),
            icon=folium.Icon(color="red", icon="plus-sign")
        ).add_to(m)

    results = sorted(results, key=lambda x: x["Distance (miles)"])

    if results:
        st.markdown("### Closest Providers")
        st.dataframe(results, use_container_width=True)

        st.markdown("### Interactive Map")
        st_folium(m, width=1000, height=550)

    return results


if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


question = st.text_input("Ask a question about NPI provider data:")

col1, col2 = st.columns(2)

with col1:
    ask_button = st.button("Ask")

with col2:
    clear_button = st.button("Clear History")


if clear_button:
    st.session_state.chat_history = []
    st.rerun()


if ask_button and question.strip():
    with st.spinner("Searching NPI data..."):
        answer = bedrock_agent(
            question,
            history=st.session_state.chat_history
        )

    ts = local_timestamp()

    st.session_state.chat_history.append({
        "time": ts["short"],
        "full_time": ts["full"],
        "subject": make_subject(question),
        "question": question,
        "answer": answer
    })


st.subheader("Conversation History")

if not st.session_state.chat_history:
    st.info("No questions asked yet.")
else:
    for i, item in enumerate(reversed(st.session_state.chat_history)):
        title = f"{item['time']} — {item['subject']}"

        with st.expander(title, expanded=(i == 0)):
            st.caption(item["full_time"])

            st.markdown("**Question:**")
            st.write(item["question"])

            st.markdown("**Answer:**")
            st.write(item["answer"])


st.subheader("Provider Map")

user_address = st.text_input(
    "Enter your address, city, or ZIP code:",
    placeholder="Example: Baltimore, MD"
)

map_button = st.button("Show Map for Latest Provider Results")

if map_button:
    if not user_address.strip():
        st.warning("Please enter your address, city, or ZIP code first.")

    elif not st.session_state.chat_history:
        st.warning("Ask a provider search question first.")

    else:
        latest_answer = st.session_state.chat_history[-1]["answer"]
        providers = parse_provider_lines(latest_answer)

        if not providers:
            st.warning("No mappable provider results found in the latest answer.")

        else:
            with st.spinner("Creating provider map..."):
                mapped_results = create_provider_map(user_address, providers)

            if not mapped_results:
                st.warning(
                    "Could not geocode provider addresses. Try asking for fewer providers, "
                    "for example: `Show oncologists in Baltimore only`, then map again."
                )
