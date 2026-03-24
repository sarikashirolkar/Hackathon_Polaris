"""
Kisan Mitra — Streamlit App
Two tabs: Crop Advisory + Student Scholarship Finder
"""
import os
import streamlit as st
from agent import chat as farmer_chat
from student_agent import chat as student_chat

st.set_page_config(page_title="Kisan Mitra", page_icon="🌾", layout="wide")

st.markdown("""
<div style='text-align:center; padding: 10px 0'>
  <h1>🌾 Kisan Mitra &nbsp;|&nbsp; किसान मित्र</h1>
  <p style='color:#555'>AI advisor for Indian farming families — crop advice & education schemes</p>
</div>
""", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["🌾 Crop Advisory", "🎓 Student Schemes"])

# ── Tab 1: Crop Advisory ──────────────────────────────────────────────────────
with tab1:
    st.markdown("**Tell me about your farm** — I'll ask a few questions and recommend the best crops for your location, soil, and season.")

    if "farmer_history" not in st.session_state:
        st.session_state.farmer_history = []
    if "farmer_messages" not in st.session_state:
        st.session_state.farmer_messages = []

    for msg in st.session_state.farmer_messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    farmer_input = st.chat_input("Describe your farm…", key="farmer_input")
    if farmer_input:
        st.session_state.farmer_messages.append({"role": "user", "content": farmer_input})
        with st.chat_message("user"):
            st.write(farmer_input)

        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                reply, st.session_state.farmer_history = farmer_chat(
                    farmer_input, st.session_state.farmer_history
                )
            st.write(reply)
        st.session_state.farmer_messages.append({"role": "assistant", "content": reply})

    if st.button("Start Over 🔄", key="farmer_reset"):
        st.session_state.farmer_history = []
        st.session_state.farmer_messages = []
        st.rerun()

# ── Tab 2: Student Schemes ────────────────────────────────────────────────────
with tab2:
    st.markdown("**Find scholarships you qualify for** — tell me about yourself and I'll check Central + State databases.")

    if "student_history" not in st.session_state:
        st.session_state.student_history = []
    if "student_messages" not in st.session_state:
        st.session_state.student_messages = []

    for msg in st.session_state.student_messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    student_input = st.chat_input("Tell me about your studies…", key="student_input")
    if student_input:
        st.session_state.student_messages.append({"role": "user", "content": student_input})
        with st.chat_message("user"):
            st.write(student_input)

        with st.chat_message("assistant"):
            with st.spinner("Searching schemes…"):
                reply, st.session_state.student_history = student_chat(
                    student_input, st.session_state.student_history
                )
            st.write(reply)
        st.session_state.student_messages.append({"role": "assistant", "content": reply})

    if st.button("Start Over 🔄", key="student_reset"):
        st.session_state.student_history = []
        st.session_state.student_messages = []
        st.rerun()
