"""Tests for the current src layout; importing tests makes the package available."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
