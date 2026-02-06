# teletwat

FastAPI app for bridging TVHeadend ↔ NDI ↔ AES67 (GStreamer-based).

## Quick start

1. Create a virtual environment and install Python deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> Note: GStreamer / PyGObject require system packages (varies by OS).  
> On Debian/Ubuntu you typically need: `python3-gi`, `gir1.2-gstreamer-1.0`, and the relevant GStreamer plugins.

2. Run the API (example with uvicorn):

```bash
pip install uvicorn
uvicorn app:app --host 0.0.0.0 --port 8000
```

3. Open: `http://localhost:8000`

## Configuration

Configuration is stored in `config.json` (committed intentionally).

## Repo

Created from an uploaded project zip and prepared for GitHub.
