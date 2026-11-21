"""Bootstrap sys.path so unittest discover works from repo root with zero setup."""

from pathlib import Path
import sys

SDK_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = SDK_ROOT.parent.parent

if str(SDK_ROOT) not in sys.path:
    sys.path.insert(0, str(SDK_ROOT))
