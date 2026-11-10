#!/usr/bin/env bash
# Start the Streamlit Gold explorer (read-only). Open http://localhost:8501
# (VS Code forwards the port automatically when connected over SSH).
# Uses its own environment so the Spark processing environment is untouched:
#   python3 -m venv ~/dat535-gui-env && ~/dat535-gui-env/bin/pip install -r requirements-gui.txt
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
gui_python="${DAT535_GUI_PYTHON:-$HOME/dat535-gui-env/bin/python}"
export DAT535_DATA_ROOT="${DAT535_DATA_ROOT:-/srv/dat535/data}"
exec "$gui_python" -m streamlit run app/gold_app.py --server.port "${DAT535_GUI_PORT:-8501}" \
  --server.address 127.0.0.1 --server.headless true --browser.gatherUsageStats false "$@"
