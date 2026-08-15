/**
 * worker.js — Debug/filter proxy for AgentRouter -> Kilo Code
 * (Anthropic Messages API + OpenAI-compatible), ported to a Cloudflare Worker.
 *
 * Deploy:
 *   npm install -g wrangler        # if you don't have it
 *   wrangler deploy
 *
 * Then set Kilo Code base URL to your worker URL, e.g.:
 *   https://<your-worker>.<your-subdomain>.workers.dev/v1
 */

const UPSTREAM = "https://agentrouter.org";

// Official Anthropic stream event types (+ OpenAI chunk/completion types).
// Anything else gets logged (and dropped if STRICT_FILTER is true).
const ANTHROPIC_EVENTS = new Set([
  "message_start",
  "content_block_start",
  "content_block_delta",
  "content_block_stop",
  "message_delta",
  "message_stop",
  "ping",
  "error",
  "chat.completion.chunk",
  "chat.completion",
]);

// true  -> drop ALL non-standard events (recommended for AgentRouter)
// false -> drop only 'billing_summary'
const STRICT_FILTER = true;

// Headers we never forward verbatim in either direction.
const HOP_BY_HOP = new Set([
  "host",
  "content-length",
  "connection",
  "keep-alive",
  "transfer-encoding",
  "te",
  "trailer",
  "upgrade",
  "proxy-authenticate",
  "proxy-authorization",
  "accept-encoding",
  "cf-connecting-ip",
  "cf-ray",
  "cf-visitor",
  "x-forwarded-for",
  "x-forwarded-proto",
]);

function filterHeaders(headers) {
  const out = new Headers();
  for (const [k, v] of headers.entries()) {
    if (!HOP_BY_HOP.has(k.toLowerCase())) out.set(k, v);
  }
  return out;
}

/**
 * Extract the event "type" from a raw SSE block.
 * isAnthropicMode mirrors the original `model` flag: true for /v1/messages
 * (Anthropic-style payload.type), false for chat/completions (payload.object).
 */
function extractEventType(block, isAnthropicMode) {
  let name = "";
  for (const line of block.split("\n")) {
    if (line.startsWith("event:")) {
      name = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      try {
        const payload = JSON.parse(line.slice("data:".length).trim());
        name = (isAnthropicMode ? payload.type : payload.object) ?? name;
      } catch {
        // ignore malformed/partial JSON (e.g. keep-alive comments)
      }
    }
  }
  return name;
}

function shouldDrop(eventType) {
  if (!eventType) return false; // comments / keep-alives — pass through
  if (eventType === "billing_summary" || eventType === "billing.summary") {
    return true;
  }
  if (STRICT_FILTER && !ANTHROPIC_EVENTS.has(eventType)) return true;
  return false;
}

/**
 * Turn an upstream SSE ReadableStream into a filtered one, dropping
 * non-standard event blocks. Loops internally on `pull` so it never
 * returns without either enqueueing data or closing (avoids stalls).
 */
function filterSSEStream(upstreamBody, isAnthropicMode) {
  const reader = upstreamBody.getReader();
  const decoder = new TextDecoder();
  const encoder = new TextEncoder();
  let buffer = "";

  return new ReadableStream({
    async pull(controller) {
      while (true) {
        const idx = buffer.indexOf("\n\n");
        if (idx !== -1) {
          const block = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);
          const etype = extractEventType(block, isAnthropicMode);
          console.log(`--- SSE EVENT [${etype || "unknown"}] ---\n${block}`);
          if (shouldDrop(etype)) {
            console.log(`>>> DROPPED non-standard event: ${etype}`);
            continue; // keep looping without returning control
          }
          controller.enqueue(encoder.encode(block + "\n\n"));
          return;
        }

        const { done, value } = await reader.read();
        if (done) {
          if (buffer.trim()) {
            const etype = extractEventType(buffer, isAnthropicMode);
            console.log(
              `--- SSE TRAILING [${etype || "unknown"}] ---\n${buffer}`,
            );
            if (!shouldDrop(etype)) {
              controller.enqueue(encoder.encode(buffer));
            }
          }
          controller.close();
          return;
        }
        buffer += decoder.decode(value, { stream: true });
      }
    },
    cancel() {
      reader.cancel();
    },
  });
}

// --- Dedicated /v1/models handler (Kilo Code calls this on setup) ---------
async function handleModels(request) {
  console.log("\n=== MODELS REQUEST -> /v1/models ===");

  // Extract the API key from either auth style:
  //   - OpenAI style:    Authorization: Bearer sk-xxx
  //   - Anthropic style: x-api-key: sk-xxx
  let apiKey = "";
  const auth = request.headers.get("authorization") || "";
  if (auth.toLowerCase().startsWith("bearer ")) {
    apiKey = auth.slice("bearer ".length).trim();
  }
  if (!apiKey) {
    apiKey = (request.headers.get("x-api-key") || "").trim();
  }
  if (!apiKey) {
    console.log(">>> No API key found in Authorization or x-api-key header");
  }

  const upstreamHeaders = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiKey}`,
    "User-Agent": "opencode/1.0.0",
    "X-Client": "opencode",
  };

  const r = await fetch(`${UPSTREAM}/v1/models`, {
    method: "GET",
    headers: upstreamHeaders,
  });

  const raw = await r.arrayBuffer();
  try {
    const payload = JSON.parse(new TextDecoder().decode(raw));
    const ids = (payload.data || []).map((m) => m.id);
    console.log(`=== MODELS (${ids.length}): ${JSON.stringify(ids)}`);
  } catch {
    console.log(
      `=== MODELS raw (${r.status}): ${new TextDecoder().decode(raw).slice(0, 500)}`,
    );
  }

  const respHeaders = filterHeaders(r.headers);
  return new Response(raw, {
    status: r.status,
    headers: respHeaders,
  });
}

// --- Catch-all proxy (mirrors the FastAPI @app.api_route("/{path:path}")) -
async function handleProxy(request, path, url) {
  const isAnthropicMode = path === "v1/messages";

  // Extract API key the same way handleModels does, so AgentRouter sees a
  // valid credential regardless of which header style the client uses.
  let apiKey = "";
  const auth = request.headers.get("authorization") || "";
  if (auth.toLowerCase().startsWith("bearer ")) {
    apiKey = auth.slice("bearer ".length).trim();
  }
  if (!apiKey) {
    apiKey = (request.headers.get("x-api-key") || "").trim();
  }

  // Build upstream headers: start with filtered client headers, then
  // override / supplement with what AgentRouter expects.
  const headers = filterHeaders(request.headers);
  if (apiKey) {
    headers.set("Authorization", `Bearer ${apiKey}`);
  }
  headers.set("User-Agent", "opencode/1.0.0");
  headers.set("X-Client", "opencode");

  const upstreamUrl = `${UPSTREAM}/${path}${url.search}`;

  console.log(`\n=== REQUEST -> ${upstreamUrl} ===`);

  const body = ["GET", "HEAD"].includes(request.method)
    ? undefined
    : await request.arrayBuffer();

  const upstream = await fetch(upstreamUrl, {
    method: request.method,
    headers,
    body,
  });

  const contentType = upstream.headers.get("content-type") || "";
  const respHeaders = filterHeaders(upstream.headers);

  // Non-streaming responses: pass through untouched, but log them.
  if (!contentType.includes("text/event-stream")) {
    const raw = await upstream.arrayBuffer();
    console.log(`=== NON-STREAM RESPONSE (${upstream.status}) ===`);
    console.log(
      new TextDecoder("utf-8", { fatal: false }).decode(raw).slice(0, 5000),
    );
    return new Response(raw, { status: upstream.status, headers: respHeaders });
  }

  // Streaming: parse SSE event blocks, log everything, filter bad events.
  const filtered = filterSSEStream(upstream.body, isAnthropicMode);
  return new Response(filtered, {
    status: upstream.status,
    headers: respHeaders,
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\//, "");

    try {
      if (path === "v1/models" && request.method === "GET") {
        return await handleModels(request);
      }
      return await handleProxy(request, path, url);
    } catch (err) {
      console.log(`!!! PROXY ERROR: ${err.stack || err}`);
      return new Response(JSON.stringify({ error: String(err) }), {
        status: 502,
        headers: { "content-type": "application/json" },
      });
    }
  },
};
