import streamlit as st
from npi_chatbot_sql import bedrock_agent

st.title("NPI Agent")

question = st.text_input("Ask about providers")

if st.button("Submit"):
    answer = bedrock_agent(question)
    st.write(answer)
