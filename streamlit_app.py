import streamlit as st
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
        answer = bedrock_agent(question)

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
