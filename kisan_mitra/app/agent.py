"""
Kisan Mitra — AI Agent (Form-Filler)
Uses OpenAI GPT-4o-mini with tool use to gather farmer profile
and call the RAG crop advisory tool.
"""
import json
import os
from openai import OpenAI
from rag_pipeline import query_crop_advisory

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_crop_advisory",
            "description": (
                "Call this once you know the farmer's state, season, and land size. "
                "It queries the crop database and returns personalised recommendations."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "state": {
                        "type": "string",
                        "description": "Indian state (e.g. Maharashtra, Punjab, Tamil Nadu)"
                    },
                    "district": {
                        "type": "string",
                        "description": "District name, if mentioned"
                    },
                    "season": {
                        "type": "string",
                        "enum": ["Kharif", "Rabi", "Zaid"],
                        "description": "Kharif = Jun-Oct, Rabi = Oct-Feb, Zaid = Feb-Jun"
                    },
                    "acres": {
                        "type": "number",
                        "description": "Farm size in acres"
                    },
                    "soil_type": {
                        "type": "string",
                        "enum": ["black", "loamy", "sandy", "clay", "red", "alluvial"],
                        "description": "Soil type if farmer mentioned it"
                    },
                    "water_availability": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "Irrigation or rainfall availability"
                    },
                },
                "required": ["state", "season", "acres"],
            },
        }
    }
]

SYSTEM_PROMPT = """You are Kisan Mitra (किसान मित्र), a warm and friendly AI advisor for Indian farmers.
Your job: chat naturally with the farmer, gather their details, then call query_crop_advisory to get personalised crop recommendations.

Information to collect (3-5 turns max):
1. Location → state + district (e.g. "Nashik, Maharashtra")
2. Season → Kharif (Jun-Oct), Rabi (Oct-Feb), or Zaid (Feb-Jun) — infer from month if farmer says "June"
3. Land size → convert bighas/guntha to acres if needed (1 bigha ≈ 0.6 acres, 40 guntha = 1 acre)
4. Soil type → black/loamy/sandy/clay/red/alluvial (optional but helpful)
5. Water availability → high/medium/low based on irrigation or rainfall (optional)

Rules:
- Ask 1-2 questions per turn, never overwhelm the farmer.
- Call the tool as soon as you have state, season, and acres — don't wait for optional fields.
- After the tool returns, explain the top 3 crops clearly: what to grow, why it suits them, MSP price, and water needs.
- Be empathetic and simple. You may respond in Hindi if the farmer writes in Hindi.
- Start by greeting and asking the first question."""


def _execute_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "query_crop_advisory":
        result = query_crop_advisory(**tool_input)
        return json.dumps(result, ensure_ascii=False)
    return json.dumps({"error": f"Unknown tool: {tool_name}"})


def chat(user_message: str, history: list[dict]) -> tuple[str, list[dict]]:
    history = history + [{"role": "user", "content": user_message}]

    while True:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1024,
            messages=[{"role": "system", "content": SYSTEM_PROMPT}] + history,
            tools=TOOLS,
            tool_choice="auto",
        )

        msg = response.choices[0].message
        history = history + [msg]

        if response.choices[0].finish_reason == "stop":
            return msg.content or "", history

        if response.choices[0].finish_reason == "tool_calls":
            tool_results = []
            for tc in msg.tool_calls:
                result_str = _execute_tool(tc.function.name, json.loads(tc.function.arguments))
                tool_results.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_str,
                })
            history = history + tool_results
        else:
            return msg.content or "", history
