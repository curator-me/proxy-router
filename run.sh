#!/usr/bin/env bash
cd /home/desktop-potato/.local/custom/agent-router-proxy || exit 1
source .venv/bin/activate
echo "=== AgentRouter Proxy — http://127.0.0.1:8787/v1 ==="
uvicorn proxy:app --host 127.0.0.1 --port 8787
# Keep terminal open if uvicorn exits/crashes so you can read the error
echo ""
echo "Proxy stopped. Press Enter to close..."
read
