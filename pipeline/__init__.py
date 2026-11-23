"""DAT535 timetable pipeline."""

from pathlib import Path

_SRC_PIPELINE_DIR = Path(__file__).resolve().parent.parent / "src" / "pipeline"
if _SRC_PIPELINE_DIR.is_dir() and str(_SRC_PIPELINE_DIR) not in __path__:
    __path__.append(str(_SRC_PIPELINE_DIR))
