"""
Kisan Mitra — AI Agent (Form-Filler)
The agent replaces a static form by chatting with the farmer and
extracting their profile, then calling the RAG tool automatically.
"""
import json
import os
import anthropic
from rag_pipeline import query_crop_advisory

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── Tool the agent can call ──────────────────────────────────────────────────
TOOLS = [
    {
        "name": "query_crop_advisory",
        "description": (
            "Call this once you know the farmer's state, season, and land size. "
            "It queries the crop database and returns personalised recommendations."
        ),
        "input_schema": {
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
    """Execute a tool call and return the result as a JSON string."""
    if tool_name == "query_crop_advisory":
        result = query_crop_advisory(**tool_input)
        return json.dumps(result, ensure_ascii=False)
    return json.dumps({"error": f"Unknown tool: {tool_name}"})


def chat(user_message: str, history: list[dict]) -> tuple[str, list[dict]]:
    """
    Send one user message, run the agent (with possible tool calls),
    and return (assistant_text, updated_history).

    history is a list of {"role": ..., "content": ...} dicts matching
    the Anthropic Messages API format.
    """
    # Append the new user turn
    history = history + [{"role": "user", "content": user_message}]

    # Agentic loop — keep going until end_turn
    while True:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=history,
        )

        # Append assistant response to history (preserves tool_use blocks)
        history = history + [{"role": "assistant", "content": response.content}]

        if response.stop_reason == "end_turn":
            # Extract final text
            text = next(
                (b.text for b in response.content if b.type == "text"), ""
            )
            return text, history

        if response.stop_reason == "tool_use":
            # Execute every tool the model requested
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result_str = _execute_tool(block.name, block.input)
                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_str,
                        }
                    )
            # Feed results back
            history = history + [{"role": "user", "content": tool_results}]
            # Loop continues → model will generate a final response
        else:
            # Unexpected stop reason — return whatever text we have
            text = next(
                (b.text for b in response.content if b.type == "text"), ""
            )
            return text, history
