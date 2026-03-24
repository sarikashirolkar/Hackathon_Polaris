"""
Kisan Mitra — Student Scholarship Agent
Uses OpenAI GPT-4o-mini with tool use to gather student profile
and query Delta Lake for eligible schemes.
"""
import json
import os
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "find_scholarships",
            "description": (
                "Query the scholarship database for schemes the student is eligible for. "
                "Call this once you know the student's category, education level, and state."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "state": {
                        "type": "string",
                        "description": "Student's home state (e.g. Maharashtra, Punjab)"
                    },
                    "category": {
                        "type": "string",
                        "enum": ["SC", "ST", "OBC", "EWS", "General", "Minority"],
                        "description": "Caste/community category"
                    },
                    "level": {
                        "type": "string",
                        "enum": ["9th", "10th", "11th", "12th", "UG", "PG", "Diploma", "ITI", "PhD"],
                        "description": "Current class or education level"
                    },
                    "annual_income": {
                        "type": "number",
                        "description": "Annual family income in rupees"
                    },
                    "parent_is_farmer": {
                        "type": "boolean",
                        "description": "Whether the student's parent is a registered farmer"
                    },
                    "gender": {
                        "type": "string",
                        "enum": ["Male", "Female", "Other"],
                        "description": "Student's gender"
                    },
                },
                "required": ["state", "category", "level"],
            },
        }
    }
]

SYSTEM_PROMPT = """You are Vidya Mitra (विद्या मित्र), a friendly scholarship advisor for Indian students.
Your job: chat with the student, gather their details, then call find_scholarships to show them
what government schemes and scholarships they qualify for.

Collect in 3-4 turns:
1. State and current class/level (9th/10th/11th/12th/UG/PG/Diploma/PhD)
2. Category — SC / ST / OBC / EWS / General / Minority
3. Annual family income (approximate is fine)
4. Is your parent a farmer? (yes = extra schemes available)
5. Gender (some schemes are for girls only)

Rules:
- Call the tool as soon as you have state, category, and level.
- After results come back, explain the TOP 3 most valuable schemes:
  * What is it and who gives it
  * How much money
  * How to apply (website)
  * Deadline
- Be warm, encouraging, and simple. You can respond in Hindi too.
- Start by greeting and asking the first question."""


def _find_scholarships_via_spark(
    state, category, level, annual_income=None, parent_is_farmer=False, gender=None
):
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        results = spark.sql(f"""
            SELECT * FROM kisan_mitra.scholarships
            WHERE (
                array_contains(from_json(category, 'ARRAY<STRING>'), '{category}')
                OR array_contains(from_json(category, 'ARRAY<STRING>'), 'General')
            )
            AND array_contains(from_json(level, 'ARRAY<STRING>'), '{level}')
            AND (max_income IS NULL OR {annual_income or 999999} <= max_income)
            AND (states = 'all' OR states LIKE '%{state}%')
            ORDER BY farmer_family_bonus DESC, amount_per_year DESC
            LIMIT 6
        """).collect()

        schemes = []
        for row in results:
            d = row.asDict()
            d["level"]    = json.loads(d["level"])
            d["category"] = json.loads(d["category"])
            d["states"]   = json.loads(d["states"]) if d["states"] != "all" else "all"
            schemes.append(d)
        return schemes
    except Exception:
        return _local_fallback(state, category, level, annual_income, parent_is_farmer)


def _local_fallback(state, category, level, annual_income, parent_is_farmer):
    schemes = [
        {
            "name": f"NSP Post-Matric Scholarship ({category} Students)",
            "provider": "Ministry of Social Justice & Empowerment",
            "amount_per_year": 12000,
            "how_to_apply": "scholarships.gov.in",
            "deadline": "October–November",
            "description": "Covers tuition and maintenance for post-matric education.",
        },
        {
            "name": "Central Sector Scholarship",
            "provider": "Ministry of Education",
            "amount_per_year": 20000,
            "how_to_apply": "scholarships.gov.in",
            "deadline": "October–November",
            "description": "For top 20th percentile Class 12 students pursuing UG/PG.",
        },
    ]
    if parent_is_farmer:
        schemes.append({
            "name": "Kisan Putra Scholarship",
            "provider": f"{state} State Agriculture Department",
            "amount_per_year": 15000,
            "how_to_apply": "State agriculture department portal",
            "deadline": "August–October",
            "description": "Specifically for children of registered farmers.",
        })
    return schemes


def _execute_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "find_scholarships":
        schemes = _find_scholarships_via_spark(
            state=tool_input["state"],
            category=tool_input["category"],
            level=tool_input["level"],
            annual_income=tool_input.get("annual_income"),
            parent_is_farmer=tool_input.get("parent_is_farmer", False),
            gender=tool_input.get("gender"),
        )
        return json.dumps({
            "eligible_schemes": schemes,
            "total_found": len(schemes),
        }, ensure_ascii=False, default=str)
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
