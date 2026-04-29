import streamlit as st
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim
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


@st.cache_data(show_spinner=False)
def geocode_address(address):
    geolocator = Nominatim(user_agent="npi_healthcare_agent")
    location = geolocator.geocode(address)

    if location:
        return location.latitude, location.longitude

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
        st.error("Could not find your location. Try using a full address or ZIP code.")
        return []

    m = folium.Map(location=[user_lat, user_lon], zoom_start=9)

    folium.Marker(
        [user_lat, user_lon],
        popup="Your location",
        tooltip="Your location",
        icon=folium.Icon(color="blue", icon="home")
    ).add_to(m)

    results = []

    for provider in providers:
        provider_address = provider.get("address")

        if not provider_address:
            continue

        lat, lon = geocode_address(provider_address)

        if lat is None:
            continue

        distance = geodesic((user_lat, user_lon), (lat, lon)).miles

        result = {
            "Name": provider.get("name"),
            "Entity": provider.get("entity"),
            "NPI": provider.get("npi"),
            "Address": provider_address,
            "Taxonomy": provider.get("taxonomy"),
            "Distance (miles)": round(distance, 2)
        }

        results.append(result)

        popup_text = f"""
        <b>{provider.get("name")}</b><br>
        {provider.get("entity")}<br>
        NPI: {provider.get("npi")}<br>
        Address: {provider_address}<br>
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
    placeholder="Example: Baltimore, MD or 21218"
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
                st.warning("Could not geocode provider addresses.")
