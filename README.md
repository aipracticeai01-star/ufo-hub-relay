# UFO Hub Relay

Small authenticated reverse relay between n8n and one Windows UFO device.

- The Windows client opens an outbound WebSocket connection.
- n8n sends tasks to `POST /api/run` with `X-API-Key`.
- No inbound port or tunnel is opened on the Windows computer.
- Secrets are supplied only as Render environment variables or local CLI arguments.

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
uvicorn relay_server:app --host 0.0.0.0 --port $PORT
```

Required environment variables:

- `RELAY_API_KEY` — at least 32 characters.
- `AGENT_TOKEN` — at least 32 characters.
- `TASK_TIMEOUT_SECONDS` — optional, default 300.

