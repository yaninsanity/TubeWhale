#!/usr/bin/env bash
set -euo pipefail

# Wrapper to run whitelisted CLI commands inside the container image.
# Usage: ./run_cli.sh analyze_videos --keyword test

CMD="$1"; shift || true
PYTHON=${PYTHON:-python}
ENTRY_MAIN="main.py"
ENTRY_CLI="cli.py"

if [ -f "$ENTRY_MAIN" ]; then
  TARGET=$ENTRY_MAIN
elif [ -f "$ENTRY_CLI" ]; then
  TARGET=$ENTRY_CLI
else
  echo "No CLI entry (main.py/cli.py) found" >&2
  exit 2
fi

exec "$PYTHON" "$TARGET" "$CMD" "$@"
