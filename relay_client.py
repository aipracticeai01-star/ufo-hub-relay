"""Windows-side outbound client for UFO Hub Relay."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import urllib.error
import urllib.parse
import urllib.request

import websockets


SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_-]+")


def http_json(url: str, api_key: str, method: str = "GET", body: dict | None = None, timeout: int = 20):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


async def execute_ufo_task(args, task_id: str, task: str):
    clients_data = await asyncio.to_thread(
        http_json, f"{args.ufo_url}/api/clients", args.ufo_api_key
    )
    clients = clients_data.get("online_clients") or []
    client_id = args.ufo_client_id or (clients[0] if clients else None)
    if not client_id:
        raise RuntimeError("UFO device client is not online")
    if client_id not in clients:
        raise RuntimeError(f"Configured UFO client is not online: {client_id}")

    safe_task_id = SAFE_ID_RE.sub("_", task_id).strip("_")[:100] or "hub_task"
    await asyncio.to_thread(
        http_json,
        f"{args.ufo_url}/api/dispatch",
        args.ufo_api_key,
        "POST",
        {"client_id": client_id, "request": task, "task_name": safe_task_id},
        20,
    )

    deadline = asyncio.get_running_loop().time() + args.task_timeout
    result_url = f"{args.ufo_url}/api/task_result/{urllib.parse.quote(safe_task_id)}"
    while asyncio.get_running_loop().time() < deadline:
        result = await asyncio.to_thread(http_json, result_url, args.ufo_api_key)
        if result.get("status") == "done":
            return result.get("result")
        await asyncio.sleep(2)
    raise TimeoutError("UFO task timed out")


async def connected_loop(args):
    headers = {"X-Agent-Token": args.agent_token, "X-Agent-ID": args.agent_id}
    async with websockets.connect(
        args.relay_url,
        extra_headers=headers,
        ping_interval=20,
        ping_timeout=20,
        max_size=2 * 1024 * 1024,
    ) as websocket:
        print("Связь с облачным мостом установлена")
        async for raw in websocket:
            message = json.loads(raw)
            if message.get("type") != "task":
                continue
            task_id = message.get("task_id", "")
            task = message.get("task", "")
            try:
                result = await execute_ufo_task(args, task_id, task)
            except Exception as exc:
                result = {"ok": False, "error": type(exc).__name__, "message": str(exc)[:500]}
            await websocket.send(json.dumps({"type": "result", "task_id": task_id, "result": result}, ensure_ascii=False))


async def run(args):
    delay = 2
    while True:
        try:
            await connected_loop(args)
            delay = 2
        except (OSError, urllib.error.URLError, websockets.WebSocketException) as exc:
            print(f"Нет связи с мостом ({type(exc).__name__}), повтор через {delay} сек.")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30)


def parse_args():
    parser = argparse.ArgumentParser(description="UFO Hub Relay client")
    parser.add_argument("--relay-url", required=True, help="wss://.../agent")
    parser.add_argument("--agent-token", required=True)
    parser.add_argument("--agent-id", default="ruslan-pc")
    parser.add_argument("--ufo-url", default="http://127.0.0.1:5001")
    parser.add_argument("--ufo-api-key", required=True)
    parser.add_argument("--ufo-client-id", default=None)
    parser.add_argument("--task-timeout", type=int, default=300)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))

