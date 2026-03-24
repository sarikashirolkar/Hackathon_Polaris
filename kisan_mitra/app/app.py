"""
Kisan Mitra — Databricks App (Gradio)
Two tabs:
  1. 🌾 Farmer Advisory  — crop recommendations via FAISS + Llama-3.1
  2. 🎓 Student Schemes  — scholarship eligibility via Spark SQL + Claude
"""
import gradio as gr
from agent import chat as farmer_chat
from student_agent import chat as student_chat

# ── Shared chat handler factory ──────────────────────────────────────────────

def make_respond(chat_fn):
    def respond(user_message: str, gradio_history: list, state: list):
        if not user_message.strip():
            return "", gradio_history, state
        agent_reply, updated_state = chat_fn(user_message, state)
        gradio_history = gradio_history + [[user_message, agent_reply]]
        return "", gradio_history, updated_state
    return respond

def reset():
    return [], []

# ── Theme & CSS ───────────────────────────────────────────────────────────────
theme = gr.themes.Soft(
    primary_hue="green",
    secondary_hue="emerald",
)

# ── App layout ────────────────────────────────────────────────────────────────
with gr.Blocks(theme=theme, title="Kisan Mitra") as demo:

    gr.HTML("""
    <div style="text-align:center; padding: 16px 0 8px;">
      <h1 style="font-size:2rem; margin:0;">🌾 Kisan Mitra &nbsp;|&nbsp; किसान मित्र</h1>
      <p style="color:#555; margin:4px 0 0;">
        AI advisor for <strong>Indian farming families</strong> — crop advice &amp; education schemes in one place
      </p>
    </div>
    """)

    with gr.Tabs():

        # ── Tab 1: Farmer Advisory ──────────────────────────────────────────
        with gr.Tab("🌾 Crop Advisory"):
            gr.Markdown("""
**Tell me about your farm** — I'll ask a few questions and recommend the best crops for your location, soil, and season.
No forms. Just a conversation.
""")
            farmer_state = gr.State([])

            with gr.Row():
                with gr.Column(scale=3):
                    farmer_bot = gr.Chatbot(
                        label="Kisan Mitra",
                        height=460,
                        bubble_full_width=False,
                        avatar_images=(
                            None,
                            "https://em-content.zobj.net/source/apple/391/seedling_1f331.png"
                        ),
                    )
                    with gr.Row():
                        farmer_txt = gr.Textbox(
                            placeholder="Describe your farm…",
                            show_label=False,
                            scale=5,
                            container=False,
                        )
                        farmer_send = gr.Button("Send 🌱", scale=1, variant="primary")
                    farmer_reset = gr.Button("Start Over 🔄", size="sm")

                with gr.Column(scale=1):
                    gr.Markdown("### 💬 Try saying:")
                    for ex in [
                        "I'm in Vidarbha, Maharashtra — 4 acres, black soil",
                        "मैं नासिक में हूं, 3 एकड़, खरीफ की बुआई",
                        "Punjab, 8 acres loamy soil, Rabi season",
                        "2 acres in Coimbatore, red soil, low water",
                        "Rajasthan, sandy soil, 5 acres, drought area",
                    ]:
                        gr.Button(ex, size="sm").click(lambda e=ex: e, outputs=farmer_txt)

                    gr.Markdown("""
### 🏗 How it works
```
You chat
  ↓
Claude agent extracts
your farm profile
  ↓
FAISS search on DBFS
(Delta Lake data)
  ↓
Llama-3.1-70B
synthesises advice
  ↓
Top crops + MSP prices
```
""")

            # Wire farmer tab
            farmer_respond = make_respond(farmer_chat)
            farmer_txt.submit(farmer_respond, [farmer_txt, farmer_bot, farmer_state],
                              [farmer_txt, farmer_bot, farmer_state])
            farmer_send.click(farmer_respond, [farmer_txt, farmer_bot, farmer_state],
                              [farmer_txt, farmer_bot, farmer_state])
            farmer_reset.click(reset, outputs=[farmer_bot, farmer_state])

        # ── Tab 2: Student Schemes ──────────────────────────────────────────
        with gr.Tab("🎓 Student Schemes"):
            gr.Markdown("""
**Find scholarships & government schemes you qualify for** — tell me about yourself and I'll check
Central + State databases for everything you're eligible for.
""")
            student_state = gr.State([])

            with gr.Row():
                with gr.Column(scale=3):
                    student_bot = gr.Chatbot(
                        label="Vidya Mitra",
                        height=460,
                        bubble_full_width=False,
                        avatar_images=(
                            None,
                            "https://em-content.zobj.net/source/apple/391/graduation-cap_1f393.png"
                        ),
                    )
                    with gr.Row():
                        student_txt = gr.Textbox(
                            placeholder="Tell me about your studies…",
                            show_label=False,
                            scale=5,
                            container=False,
                        )
                        student_send = gr.Button("Send 📚", scale=1, variant="primary")
                    student_reset = gr.Button("Start Over 🔄", size="sm")

                with gr.Column(scale=1):
                    gr.Markdown("### 💬 Try saying:")
                    for ex in [
                        "I'm in Class 12, OBC category, Maharashtra",
                        "SC student, doing BSc in Punjab, income below 2 lakh",
                        "मैं ST छात्र हूं, 10वीं में हूं, MP से",
                        "Girl student, UG, EWS, Tamil Nadu, father is farmer",
                        "General category, doing engineering, Karnataka",
                    ]:
                        gr.Button(ex, size="sm").click(lambda e=ex: e, outputs=student_txt)

                    gr.Markdown("""
### 🏗 How it works
```
You chat
  ↓
Claude agent extracts
your eligibility profile
  ↓
Spark SQL queries
Delta Lake scholarship table
  ↓
Top matching schemes
with amounts + links
```
### 📋 Schemes covered
- NSP Pre & Post-Matric
- PM YASASVI
- Central Sector Scholarship
- INSPIRE (Science)
- PM Vidyalaxmi
- State-specific schemes
- Farmer-family schemes
- Minority scholarships
""")

            # Wire student tab
            student_respond = make_respond(student_chat)
            student_txt.submit(student_respond, [student_txt, student_bot, student_state],
                               [student_txt, student_bot, student_state])
            student_send.click(student_respond, [student_txt, student_bot, student_state],
                               [student_txt, student_bot, student_state])
            student_reset.click(reset, outputs=[student_bot, student_state])

    # ── Footer ───────────────────────────────────────────────────────────────
    gr.HTML("""
    <div style="text-align:center; color:#888; font-size:0.85rem; padding:12px 0 4px;">
      Built on Databricks · Delta Lake · Apache Spark · FAISS · MLflow · Claude · Llama-3.1-70B
    </div>
    """)

if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=8080)
