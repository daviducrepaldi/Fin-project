#!/bin/bash
# Launch the app with the Tiingo API key loaded from .env
# Export only the specific key — avoids leaking every variable in .env to subprocesses.
ENV_FILE="$(dirname "$0")/.env"
if [ -f "$ENV_FILE" ]; then
    TIINGO_API_KEY=$(grep -m1 '^TIINGO_API_KEY=' "$ENV_FILE" | cut -d= -f2-)
    export TIINGO_API_KEY
fi
exec ~/Library/Python/3.9/bin/streamlit run "$(dirname "$0")/app.py" "$@"
