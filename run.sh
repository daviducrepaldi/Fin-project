#!/bin/bash
# Launch the app with API keys loaded from .env
set -a
source "$(dirname "$0")/.env"
set +a
exec ~/Library/Python/3.9/bin/streamlit run "$(dirname "$0")/app.py" "$@"
