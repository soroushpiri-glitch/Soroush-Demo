import streamlit as st

st.title("NPI Bedrock Chatbot")

question = st.text_input(
    "Ask a question about NPI data"
)

if question:
    st.write(
        "Hook Bedrock agent here"
    )
