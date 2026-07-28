"""
proxy.py — Debug/filter proxy for AgentRouter -> Kilo Code (Anthropic Messages API + Open Ai compatible)

venv: python3 -m venv .venv
Install:  pip install fastapi uvicorn httpx
Run:      uvicorn proxy:app --host 127.0.0.1 --port 8787
Then set Kilo Code base URL to:  http://127.0.0.1:8787/v1
"""

import json
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, Response


UPSTREAM = "https://agentrouter.org"

# Official Anthropic stream event types. Anything else gets logged (and dropped
# if STRICT_FILTER is True).
ANTHROPIC_EVENTS = {
    "message_start", "content_block_start", "content_block_delta",
    "content_block_stop", "message_delta", "message_stop", "ping", "error",
    "chat.completion.chunk", "chat.completion"
}

# True  -> drop ALL non-standard events (recommended for AgentRouter)
# False -> drop only 'billing_summary'
STRICT_FILTER = True

model = 1  
# For anthropic  ("https://agentrouter.org/v1/messages") 
# For openai     ("https://agentrouter.org/v1/chat/completions")

app = FastAPI()

HOP_BY_HOP = {
    "host", "content-length", "connection", "keep-alive", "transfer-encoding",
    "te", "trailer", "upgrade", "proxy-authenticate", "proxy-authorization",
    "accept-encoding",
}


def _event_type(block: str) -> str:
    """Extract event type from an SSE event block."""
    global model

    name = ""
    for line in block.splitlines():
        if line.startswith("event:"):
            name = line[len("event:"):].strip()

        elif line.startswith("data:"):
            try:
                payload = json.loads(line[len("data:"):].strip())

                if model:
                    name = payload.get("type", name)
                else:
                    name = payload.get("object", name)

            except (json.JSONDecodeError, AttributeError):
                pass

    return name

def _should_drop(event_type: str) -> bool:
    if not event_type:
        print(str(type(event_type)))
        return False  # comments / keep-alives — pass through
    if event_type == "billing_summary" or event_type == "billing.summary":
        print(str(type(event_type)))
        return True
    if STRICT_FILTER and event_type not in ANTHROPIC_EVENTS:
        return True
    return False


# --- Dedicated /v1/models endpoint (Kilo Code calls this on setup) -----------
@app.get("/v1/models")
async def list_models(request: Request):
    print("\n=== MODELS REQUEST -> /v1/models ===")

    # Extract the API key from either auth style:
    #   - OpenAI style:    Authorization: Bearer sk-xxx
    #   - Anthropic style: x-api-key: sk-xxx
    api_key = ""
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        api_key = auth[len("bearer "):].strip()
    if not api_key:
        api_key = request.headers.get("x-api-key", "").strip()

    if not api_key:
        print(">>> No API key found in Authorization or x-api-key header")

    # Build clean upstream headers (match the known-working curl request)
    upstream_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "opencode/1.0.0",
        "X-Client": "opencode",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{UPSTREAM}/v1/models", headers=upstream_headers)

    try:
        payload = r.json()
        ids = [m.get("id") for m in payload.get("data", [])]
        print(f"=== MODELS ({len(ids)}): {ids}")
    except Exception:
        print(f"=== MODELS raw ({r.status_code}): {r.text[:500]}")

    resp_headers = {k: v for k, v in r.headers.items() if k.lower() not in HOP_BY_HOP}
    return Response(content=r.content, status_code=r.status_code,
                    headers=resp_headers,
                    media_type=r.headers.get("content-type", "application/json"))

# --- Dedicated endpoint to filter out non-standard events (Kilo Code calls this on setup) -----------
@app.api_route("/{path:path}", methods=["GET", "POST"])
async def proxy(request: Request, path: str):
    global model
    body = await request.body()
    headers = {
        k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP
    }
    url = f"{UPSTREAM}/{path}"
    if request.url.query:
        url += f"?{request.url.query}"

    print(f"\n=== REQUEST -> {url} ===")
    if(path == "v1/messages"):
        model = 1
    else:
        model = 0

    client = httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=15.0))
    req = client.build_request(request.method, url, headers=headers, content=body)
    upstream = await client.send(req, stream=True)

    content_type = upstream.headers.get("content-type", "")

    # Non-streaming responses: pass through untouched, but log them.
    if "text/event-stream" not in content_type:
        raw = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        print(f"=== NON-STREAM RESPONSE ({upstream.status_code}) ===")
        print(raw.decode("utf-8", errors="replace")[:5000])
        resp_headers = {
            k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP
        }
        return Response(content=raw, status_code=upstream.status_code,
                        headers=resp_headers, media_type=content_type)

    # Streaming: parse SSE event blocks, log everything, filter bad events.
    async def stream_filtered():
        buffer = ""
        try:
            async for chunk in upstream.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    block, buffer = buffer.split("\n\n", 1)
                    etype = _event_type(block)
                    print(f"--- SSE EVENT [{etype or 'unknown'}] ---")
                    print(block)
                    if _should_drop(etype):
                        print(f">>> DROPPED non-standard event: {etype}")
                        continue
                    yield block + "\n\n"
            if buffer.strip():
                # Flush any trailing partial block
                etype = _event_type(buffer)
                print(f"--- SSE TRAILING [{etype or 'unknown'}] ---")
                print(buffer)
                if not _should_drop(etype):
                    yield buffer
        finally:
            await upstream.aclose()
            await client.aclose()

    resp_headers = {
        k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP
    }
    return StreamingResponse(stream_filtered(), status_code=upstream.status_code,
                             headers=resp_headers, media_type="text/event-stream")
