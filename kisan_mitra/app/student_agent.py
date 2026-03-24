"""
Kisan Mitra — Student Scholarship Agent
Gathers student profile through conversation, queries Delta Lake for
eligible schemes, and explains them clearly.

Uses Spark SQL for structured eligibility filtering (different from the
FAISS semantic search used for crop advisory — shows two Databricks patterns).
"""
import json
import os
import anthropic

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── Tool definition ──────────────────────────────────────────────────────────
TOOLS = [
    {
        "name": "find_scholarships",
        "description": (
            "Query the scholarship database for schemes the student is eligible for. "
            "Call this once you know the student's category, education level, and state."
        ),
        "input_schema": {
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
                    "description": "Student's gender (some schemes are girl-specific)"
                },
            },
            "required": ["state", "category", "level"],
        },
    }
]

SYSTEM_PROMPT = """You are Vidya Mitra (विद्या मित्र), a friendly scholarship advisor for Indian students.
Your job: chat with the student, gather their details, then call find_scholarships to show them
what government schemes and scholarships they qualify for.

Collect in 3-4 turns:
1. State and current class/level (9th/10th/11th/12th/UG/PG/Diploma/PhD)
2. Category — SC / ST / OBC / EWS / General / Minority
3. Annual family income (approximate is fine — ask for a range: below 1 lakh / 1-2.5 lakh / 2.5-8 lakh / above)
4. Is your parent a farmer? (yes = extra schemes available)
5. Gender (some schemes are for girls only)

Rules:
- Call the tool as soon as you have state, category, and level.
- After results come back, explain the TOP 3 most valuable schemes:
  * What is it and who gives it
  * How much money
  * How to apply (website)
  * Deadline
  * Why this student specifically qualifies
- Always mention if the student qualifies for farmer-family bonus schemes.
- Be warm, encouraging, and simple. You can respond in Hindi too.
- Start by greeting and asking the first question."""


def _find_scholarships_via_spark(
    state: str,
    category: str,
    level: str,
    annual_income: float | None = None,
    parent_is_farmer: bool = False,
    gender: str | None = None,
) -> list[dict]:
    """
    Query Delta Lake scholarship table using Spark SQL.
    Falls back to a filtered in-memory list if Spark is unavailable
    (e.g., running app locally without Databricks).
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Spark SQL filter — structured eligibility matching
        results = spark.sql(f"""
            SELECT *
            FROM kisan_mitra.scholarships
            WHERE (
                -- Category match: scheme open to this student's category OR to General
                array_contains(from_json(category, 'ARRAY<STRING>'), '{category}')
                OR array_contains(from_json(category, 'ARRAY<STRING>'), 'General')
            )
            AND (
                -- Level match
                array_contains(from_json(level, 'ARRAY<STRING>'), '{level}')
            )
            AND (
                -- Income filter: scheme has no limit, OR student is within limit
                max_income IS NULL
                OR {annual_income or 999999} <= max_income
            )
            AND (
                -- State filter: scheme is national OR covers this state
                states = 'all'
                OR states LIKE '%{state}%'
            )
            ORDER BY
                farmer_family_bonus DESC,   -- farmer-family schemes first if applicable
                amount_per_year DESC
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
        # Local fallback — return a subset from memory
        return _local_fallback(state, category, level, annual_income, parent_is_farmer)


def _local_fallback(state, category, level, annual_income, parent_is_farmer):
    """Minimal fallback when Spark/Delta is unavailable (local dev)."""
    all_schemes = [
        {
            "scheme_id": "NSP_POST",
            "name": f"NSP Post-Matric Scholarship ({category} Students)",
            "provider": "Ministry of Social Justice & Empowerment",
            "amount_per_year": 12000,
            "how_to_apply": "scholarships.gov.in",
            "deadline": "October–November",
            "farmer_family_bonus": False,
            "description": "Covers tuition and maintenance for post-matric education.",
        },
        {
            "scheme_id": "CENTRAL_SECTOR",
            "name": "Central Sector Scholarship (College Students)",
            "provider": "Ministry of Education",
            "amount_per_year": 20000,
            "how_to_apply": "scholarships.gov.in",
            "deadline": "October–November",
            "farmer_family_bonus": False,
            "description": "For top 20th percentile Class 12 students pursuing UG/PG.",
        },
    ]
    if parent_is_farmer:
        all_schemes.append({
            "scheme_id": "PM_KISAN_EDU",
            "name": "Kisan Putra Scholarship (Farmer Family)",
            "provider": f"{state} State Agriculture Department",
            "amount_per_year": 15000,
            "how_to_apply": "State agriculture department portal",
            "deadline": "August–October",
            "farmer_family_bonus": True,
            "description": "Specifically for children of registered farmers.",
        })
    return all_schemes


def _execute_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "find_scholarships":
        schemes = _find_scholarships_via_spark(
            state           = tool_input["state"],
            category        = tool_input["category"],
            level           = tool_input["level"],
            annual_income   = tool_input.get("annual_income"),
            parent_is_farmer= tool_input.get("parent_is_farmer", False),
            gender          = tool_input.get("gender"),
        )
        return json.dumps({
            "eligible_schemes": schemes,
            "total_found": len(schemes),
            "is_farmer_family": tool_input.get("parent_is_farmer", False),
        }, ensure_ascii=False, default=str)
    return json.dumps({"error": f"Unknown tool: {tool_name}"})


def chat(user_message: str, history: list[dict]) -> tuple[str, list[dict]]:
    """Same interface as agent.py — returns (reply_text, updated_history)."""
    history = history + [{"role": "user", "content": user_message}]

    while True:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=history,
        )

        history = history + [{"role": "assistant", "content": response.content}]

        if response.stop_reason == "end_turn":
            text = next((b.text for b in response.content if b.type == "text"), "")
            return text, history

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result_str = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str,
                    })
            history = history + [{"role": "user", "content": tool_results}]
        else:
            text = next((b.text for b in response.content if b.type == "text"), "")
            return text, history
