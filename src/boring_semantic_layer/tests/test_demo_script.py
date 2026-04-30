from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "demo_bsl_v2.py"


def test_demo_bsl_v2_script_runs_successfully():
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Flights per origin:" in result.stdout
    assert "Market share per origin:" in result.stdout
    assert "Cases per spend by segment:" in result.stdout
    assert "Rolling average:" in result.stdout
