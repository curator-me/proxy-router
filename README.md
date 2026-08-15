# Agent Router Proxy

A Cloudflare Worker that proxies and filters SSE (Server-Sent Events) streams between [Kilo Code](https://kilocode.ai) / OpenCode and the [AgentRouter](https://agentrouter.org) API.

## What it does

- **SSE event filtering**: Drops non-standard events (e.g. `billing_summary`) from the upstream stream so Kilo Code only sees valid Anthropic/OpenAI events.
- **Header sanitization**: Strips hop-by-hop headers before forwarding responses.
- **Dual auth support**: Accepts API keys via either `Authorization: Bearer` (OpenAI style) or `x-api-key` (Anthropic style).
- **Mirror of the original FastAPI proxy**: Behaves identically to the Python reference implementation.

## Local development (with Wrangler)

```bash
# Install wrangler if you don't have it
npm install -g wrangler

# Login to Cloudflare
wrangler login

# Preview locally
wrangler dev

# Deploy
wrangler deploy
```

After deploying, point Kilo Code's base URL at your worker:

```
https://<your-worker>.<your-subdomain>.workers.dev/v1
```

## Configuration

Edit `wrangler.toml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `UPSTREAM` | `https://agentrouter.org` | The upstream AgentRouter URL |
| `STRICT_FILTER` | `true` | Drop all non-standard SSE events. Set to `false` to only drop `billing_summary` |

### Secrets

Set a shared secret that clients must send via `X-Proxy-Secret` header:

```bash
wrangler secret put PROXY_SECRET
```

### CPU limits (paid plans only)

If you hit CPU limits on chatty streams, increase the budget in `wrangler.toml`:

```toml
[limits]
cpu_ms = 50
```

## Project structure

```
worker.js          # Cloudflare Worker entry point
wrangler.toml      # Worker configuration
```
