"""Minimal authenticated reverse relay between n8n and one Windows UFO agent."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field


API_KEY = os.environ.get("RELAY_API_KEY", "")
AGENT_TOKEN = os.environ.get("AGENT_TOKEN", "")
TASK_TIMEOUT = min(max(int(os.environ.get("TASK_TIMEOUT_SECONDS", "300")), 30), 900)
TASK_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

if len(API_KEY) < 32 or len(AGENT_TOKEN) < 32:
    raise RuntimeError("RELAY_API_KEY and AGENT_TOKEN must each contain at least 32 characters")


class RunRequest(BaseModel):
    task: str = Field(min_length=1, max_length=4000)
    task_id: str = Field(min_length=1, max_length=128)
    source: str | None = Field(default=None, max_length=64)
    session_id: str | None = Field(default=None, max_length=128)
    mode: str | None = Field(default=None, max_length=32)


@dataclass
class RelayState:
    websocket: WebSocket | None = None
    agent_id: str | None = None
    pending: dict[str, asyncio.Future] = field(default_factory=dict)
    task_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    connection_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


state = RelayState()
app = FastAPI(title="UFO Hub Relay", docs_url=None, redoc_url=None)


def require_api_key(value: str | None) -> None:
    if not value or not hmac.compare_digest(value, API_KEY):
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "agent_online": state.websocket is not None}


@app.post("/api/run")
async def run_task(payload: RunRequest, x_api_key: str | None = Header(default=None)) -> dict[str, Any]:
    require_api_key(x_api_key)
    if not TASK_ID_RE.fullmatch(payload.task_id):
        raise HTTPException(status_code=400, detail="invalid_task_id")

    async with state.task_lock:
        websocket = state.websocket
        if websocket is None:
            raise HTTPException(status_code=503, detail="agent_offline")

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        state.pending[payload.task_id] = future
        try:
            await websocket.send_json({
                "type": "task",
                "task_id": payload.task_id,
                "task": payload.task,
                "source": payload.source,
                "session_id": payload.session_id,
                "mode": payload.mode,
            })
            result = await asyncio.wait_for(future, timeout=TASK_TIMEOUT)
            output = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
            return {"status": "completed", "task_id": payload.task_id, "result": result, "output": output}
        except asyncio.TimeoutError as exc:
            raise HTTPException(status_code=504, detail="task_timeout") from exc
        except (RuntimeError, WebSocketDisconnect) as exc:
            raise HTTPException(status_code=503, detail="agent_disconnected") from exc
        finally:
            state.pending.pop(payload.task_id, None)


@app.websocket("/agent")
async def agent_socket(websocket: WebSocket) -> None:
    supplied_token = websocket.headers.get("x-agent-token", "")
    agent_id = websocket.headers.get("x-agent-id", "")
    if not hmac.compare_digest(supplied_token, AGENT_TOKEN):
        await websocket.close(code=1008, reason="unauthorized")
        return
    if not TASK_ID_RE.fullmatch(agent_id):
        await websocket.close(code=1008, reason="invalid agent id")
        return

    async with state.connection_lock:
        if state.websocket is not None:
            await websocket.close(code=1008, reason="agent already connected")
            return
        await websocket.accept()
        state.websocket = websocket
        state.agent_id = agent_id

    try:
        while True:
            message = await websocket.receive_json()
            if not isinstance(message, dict):
                continue
            message_type = message.get("type")
            if message_type == "heartbeat":
                await websocket.send_json({"type": "heartbeat_ack"})
                continue
            if message_type != "result":
                continue

            task_id = message.get("task_id")
            if not isinstance(task_id, str):
                continue
            future = state.pending.get(task_id)
            if future is not None and not future.done():
                future.set_result(message.get("result"))
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        async with state.connection_lock:
            if state.websocket is websocket:
                state.websocket = None
                state.agent_id = None
        for future in list(state.pending.values()):
            if not future.done():
                future.set_exception(RuntimeError("agent disconnected"))
