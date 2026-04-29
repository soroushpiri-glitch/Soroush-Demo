import streamlit as st
from datetime import datetime
from npi_chatbot_sql import bedrock_agent

st.set_page_config(page_title="NPI Healthcare AI Agent", layout="wide")

st.title("NPI Healthcare Provider AI Agent")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

question = st.text_input("Ask a question about NPI provider data:")

col1, col2 = st.columns([1, 1])

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

    st.session_state.chat_history.append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "question": question,
        "answer": answer
    })

st.subheader("Conversation History")

if not st.session_state.chat_history:
    st.info("No questions asked yet.")
else:
    for i, item in enumerate(reversed(st.session_state.chat_history), 1):
        with st.expander(f"Q{i}: {item['question']}", expanded=(i == 1)):
            st.caption(item["time"])
            st.markdown("**Question:**")
            st.write(item["question"])
            st.markdown("**Answer:**")
            st.write(item["answer"])
