"""Regenerate expected functional outputs for fixtures that emit a line_item table.

Replays each fixture's EXISTING cassette (unchanged) through the component and republishes
the produced outputs + snapshot as the expected fixture. Used to refresh the line_item PK
manifest change without altering scenario content or hand-editing manifests.

Run from repo root:  uv run python tests/setup/regen_line_item_pk.py
"""

import os
import shutil
import sys
from pathlib import Path
from runpy import run_path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from keboola.datadirtest.vcr import VCRRecorder, save_output_snapshot  # noqa: E402

COMPONENT_SCRIPT = REPO_ROOT / "src" / "component.py"
FIXTURES = [
    "09_orders_real_data",
    "11_orders_customer_journey",
    "12_orders_line_item_children",
]


def regen(fixture: str) -> None:
    fixture_dir = REPO_ROOT / "tests" / "functional" / fixture
    source_data = fixture_dir / "source" / "data"
    expected_out = fixture_dir / "expected" / "data" / "out"

    out_dir = source_data / "out"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    (out_dir / "tables").mkdir(parents=True, exist_ok=True)

    recorder = VCRRecorder.from_test_dir(test_data_dir=source_data, freeze_time_at="auto")

    def runner() -> None:
        os.environ["KBC_DATADIR"] = str(source_data)
        run_path(str(COMPONENT_SCRIPT), run_name="__main__")

    recorder.replay(runner)

    if expected_out.exists():
        shutil.rmtree(expected_out)
    expected_out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(out_dir, expected_out)

    save_output_snapshot(source_data)
    print(f"Regenerated {fixture}")


if __name__ == "__main__":
    for f in FIXTURES:
        regen(f)
