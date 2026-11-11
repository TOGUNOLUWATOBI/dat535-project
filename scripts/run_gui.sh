#!/usr/bin/env bash
# Start the Streamlit Gold explorer (read-only) on all network interfaces.
# Open http://<server-ip>:8501 or http://localhost:8501 through SSH forwarding.
# Uses its own environment so the Spark processing environment is untouched:
#   python3 -m venv ~/dat535-gui-env && ~/dat535-gui-env/bin/pip install -r requirements.txt
# --restart stops this app on its configured port and starts it in the background.
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
gui_python="${DAT535_GUI_PYTHON:-$HOME/dat535-gui-env/bin/python}"
export DAT535_DATA_ROOT="${DAT535_DATA_ROOT:-/srv/dat535/data}"
gui_port="${DAT535_GUI_PORT:-8501}"
if [[ ! "$gui_port" =~ ^[0-9]+$ ]]; then
  echo "DAT535_GUI_PORT must be a port number" >&2
  exit 1
fi
gui_command=("$gui_python" -m streamlit run app/gold_app.py --server.port "$gui_port"
  --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false)

if [[ "${1:-}" == "--restart" ]]; then
  shift
  "$gui_python" -c 'import streamlit'  # Check the environment before stopping the app.
  pattern="[s]treamlit run app/gold_app[.]py --server[.]port ${gui_port}([[:space:]]|$)"
  pkill -TERM -u "$(id -u)" -f "$pattern" || true
  for attempt in {1..10}; do
    pgrep -u "$(id -u)" -f "$pattern" >/dev/null || break
    sleep 1
  done
  if pgrep -u "$(id -u)" -f "$pattern" >/dev/null; then
    echo "Previous GUI has not stopped; refusing to start a duplicate" >&2
    exit 1
  fi

  mkdir -p "$DAT535_DATA_ROOT/metadata"
  gui_log="$DAT535_DATA_ROOT/metadata/gui.log"
  # Detach all standard streams and clear tracking only for the GUI process,
  # so Actions' end-of-job process cleanup leaves this requested server running.
  RUNNER_TRACKING_ID="" nohup "${gui_command[@]}" "$@" >>"$gui_log" 2>&1 </dev/null &
  gui_pid=$!
  sleep 2
  if ! kill -0 "$gui_pid" 2>/dev/null; then
    tail -n 20 "$gui_log" >&2
    exit 1
  fi
  echo "GUI listening on 0.0.0.0:$gui_port (PID $gui_pid); open http://<server-ip>:$gui_port or http://localhost:$gui_port through SSH forwarding; log: $gui_log"
  exit 0
fi

exec "${gui_command[@]}" "$@"
