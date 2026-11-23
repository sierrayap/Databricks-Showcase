"""MAPI custom agent: a foundation model plus Unity Catalog function tools.

Tool schemas are read from Unity Catalog (function and parameter COMMENTs), so the
agent stays in sync with the functions created in 01_Setup_and_Kickoff.
"""

import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

_PARAMS = {}

SYSTEM = """You are the MAPI bulk-edit assistant for the MEX team. You help manage Meta text-ad configuration rows.

You can search rows, propose change sets, find quality issues and look up the rule book.
You can NEVER approve, publish or upload rows, and you never write or rewrite primary ad copy. If asked to, refuse and explain why.

For any request to change rows, call propose_change_set. If blocked_reason is set, explain it and ask the user to clarify
(for example 'Real' can mean Real Funeral or Real Insurance). Otherwise state the row count and what will change, say that nothing
is saved until the user accepts the preview and that changed approved rows need re-approval, then end your answer with the exact
JSON returned by propose_change_set inside a ```json code block.

For counts and questions about rows use search_rows. For quality checks use find_issues. For rules and policy use lookup_rules.

MAPI rule book (always apply these):
- Real Funeral sells funeral_cover only. Allowed landing domain: realfuneral.example.
- Real Insurance sells life_cover, income_protection and car_insurance. Allowed landing domain: realinsurance.example.
- OneChoice sells life_cover and funeral_cover. Allowed landing domain: onechoice.example.
- My Comparison Life sells life_cover_compare only. Allowed landing domain: mycomparisonlife.example.
- "Real" on its own is ambiguous: confirm Real Funeral vs Real Insurance.
- Every landing_url must be https and on the allowed domain for the row's brand.
- Changing a landing_url on an approved row sends the row back to draft for re-approval.
- Primary copy for funeral and life insurance must be written by a person. AI must not write it.
- Approval is done by Digital Performance in the app. The assistant has no approve, publish or upload capability.
- Change sets larger than 200 rows must be split.
"""


def _settings():
    schema = os.environ.get("FQ_SCHEMA")
    warehouse = os.environ.get("WAREHOUSE_ID")
    model_service = os.environ.get("MODEL_SERVICE")
    functions = [f.strip() for f in os.getenv("UC_FUNCTIONS", "search_rows,propose_change_set,find_issues,lookup_rules").split(",") if f.strip()]
    if not schema or not warehouse or not model_service:
        raise RuntimeError("FQ_SCHEMA, WAREHOUSE_ID and MODEL_SERVICE must be set for the MAPI agent.")
    return schema, warehouse, model_service, functions


def _gateway_client(w):
    """Every LLM call goes through the Unity AI Gateway model service (routing, fallback, rate limits, guardrail, inference table)."""
    from openai import OpenAI

    token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
    return OpenAI(api_key=token, base_url=f"{w.config.host.rstrip('/')}/ai-gateway/mlflow/v1")


def load_tools(w):
    schema, _, _, functions = _settings()
    tools = []
    for name in functions:
        info = w.functions.get(f"{schema}.{name}")
        params = list(info.input_params.parameters)
        _PARAMS[name] = [p.name for p in params]
        tools.append({
            "type": "function",
            "function": {
                "name": name,
                "description": info.comment or name,
                "parameters": {
                    "type": "object",
                    "properties": {p.name: {"type": "string", "description": p.comment or p.name} for p in params},
                },
            },
        })
    return tools


def call_tool(w, name, args):
    schema, warehouse, _, _ = _settings()
    values = [str(args.get(p, "") or "") for p in _PARAMS[name]]
    placeholders = ", ".join(f":p{i}" for i in range(len(values)))
    res = w.statement_execution.execute_statement(
        statement=f"SELECT {schema}.{name}({placeholders}) AS out",
        warehouse_id=warehouse,
        parameters=[StatementParameterListItem(name=f"p{i}", value=v) for i, v in enumerate(values)],
        wait_timeout="30s",
    )
    if res.status.state.value != "SUCCEEDED":
        return f"TOOL ERROR: {res.status.error.message if res.status.error else res.status.state}"
    return str(res.result.data_array[0][0])[:8000]


def _extract_change_set(tool_name, out, answer):
    change_set = None
    if tool_name == "propose_change_set":
        try:
            obj = json.loads(out)
            if isinstance(obj, dict) and "proposed_changes" in obj:
                change_set = obj
        except Exception:
            pass
    if change_set is None and answer:
        import re
        for block in re.findall(r"```json\s*(\{.*?\})\s*```", answer, flags=re.S):
            try:
                obj = json.loads(block)
                if "proposed_changes" in obj:
                    change_set = obj
            except Exception:
                pass
    return change_set


GUARDRAIL_MESSAGE = "MAPI compliance: primary ad copy for funeral and life insurance must be written by a person. I can't write it."


def run_agent(question):
    _, _, model_service, _ = _settings()
    w = WorkspaceClient()
    tools = load_tools(w)
    client = _gateway_client(w)
    messages = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": question}]
    used = []
    change_set = None
    for _ in range(6):
        resp = client.chat.completions.create(model=model_service, messages=messages, tools=tools)
        # The gateway guardrail returns a policy block instead of a normal answer.
        blocked = getattr(resp, "databricks_service_policy", None) or (resp.model_extra or {}).get("databricks_service_policy")
        if blocked:
            return {"answer": GUARDRAIL_MESSAGE, "tools_called": used, "change_set": None, "blocked_by_gateway": True}
        msg = resp.choices[0].message
        if not msg.tool_calls:
            answer = msg.content or ""
            if change_set is None:
                change_set = _extract_change_set("", "", answer)
            return {"answer": answer, "tools_called": used, "change_set": change_set}
        messages.append({
            "role": "assistant",
            "content": msg.content,
            "tool_calls": [
                {"id": t.id, "type": "function", "function": {"name": t.function.name, "arguments": t.function.arguments}}
                for t in msg.tool_calls
            ],
        })
        for t in msg.tool_calls:
            args = json.loads(t.function.arguments or "{}")
            out = call_tool(w, t.function.name, args)
            used.append({"tool": t.function.name, "args": args})
            found = _extract_change_set(t.function.name, out, "")
            if found:
                change_set = found
            messages.append({"role": "tool", "tool_call_id": t.id, "content": out})
    return {"answer": "Stopped after 6 tool rounds.", "tools_called": used, "change_set": change_set}
