#!/usr/bin/env python3
"""
postToolUse hook: run py_compile on any .py file immediately after Edit or Write.
Receives the Claude Code tool payload as JSON on stdin.
"""
import json
import subprocess
import sys

data = json.load(sys.stdin)
file_path = data.get("tool_input", {}).get("file_path", "")

if file_path.endswith(".py"):
    result = subprocess.run(
        ["python3", "-m", "py_compile", file_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"SYNTAX ERROR in {file_path}:\n{result.stderr.strip()}", flush=True)
