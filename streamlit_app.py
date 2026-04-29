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
        answer = bedrock_agent(     question,     history=st.session_state.chat_history )

    ts = local_timestamp()

    st.session_state.chat_history.append({
        "time": ts["short"],
        "full_time": ts["full"],
        "subject": make_subject(question),
        "question": question,
        "answer": answer
    })

@st.cache_data(show_spinner=False)
def geocode_address(address):
    geolocator = Nominatim(user_agent="npi_healthcare_agent")
    location = geolocator.geocode(address)

    if location:
        return location.latitude, location.longitude

    return None, None


def create_provider_map(user_address, providers):
    user_lat, user_lon = geocode_address(user_address)

    if user_lat is None:
        st.error("Could not find your location. Try using a full address or ZIP code.")
        return

    m = folium.Map(location=[user_lat, user_lon], zoom_start=10)

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

        results.append({
            **provider,
            "distance_miles": round(distance, 2),
            "lat": lat,
            "lon": lon
        })

        popup_text = f"""
        <b>{provider.get('name')}</b><br>
        NPI: {provider.get('npi')}<br>
        Address: {provider_address}<br>
        Distance: {round(distance, 2)} miles
        """

        folium.Marker(
            [lat, lon],
            popup=popup_text,
            tooltip=provider.get("name"),
            icon=folium.Icon(color="red", icon="plus-sign")
        ).add_to(m)

    results = sorted(results, key=lambda x: x["distance_miles"])

    st_folium(m, width=1000, height=550)

    return results

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
