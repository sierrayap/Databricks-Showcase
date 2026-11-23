"""MAPI custom agent served by the Agent Bricks runtime (DurableAgentServer).

The MAPI app calls POST /api/invocations with its service principal. Every LLM call the
agent makes goes through the Unity AI Gateway model service (see agent_core).
"""

import asyncio
import os

import uvicorn
from databricks_agentkit import DurableAgentServer, InvocationContext

from agent_core import run_agent

app = DurableAgentServer()


@app.invoke
async def invoke(input, context: InvocationContext) -> dict:
    messages = input.get("messages") if isinstance(input, dict) else input
    question = messages[-1]["content"] if isinstance(messages, list) and messages else str(input)
    return await asyncio.to_thread(run_agent, question)


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT") or os.getenv("PORT") or "8000")
    uvicorn.run(app, host="0.0.0.0", port=port)
