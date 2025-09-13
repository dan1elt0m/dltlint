from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from dltlint.core import find_pipeline_files


def write(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f


def run_cli(tmp_path: Path, *args: str) -> subprocess.CompletedProcess:
    # Run the installed console script. During dev, `pip install -e .`
    return subprocess.run(
        [sys.executable, "-m", "dltlint.cli", *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_silent_success_when_clean(tmp_path: Path):
    yaml_text = """
name: ocpp_bronze
catalog: main
schema: bronze
pipelines.trigger.interval: "1 hour"
"""
    write(tmp_path, "clean.pipeline.yml", yaml_text)

    # sanity: discovery finds the file
    found = find_pipeline_files([str(tmp_path)])
    assert len(found) == 1

    cp = run_cli(tmp_path, str(tmp_path))
    assert cp.returncode == 0, f"stdout={cp.stdout}\nstderr={cp.stderr}"
    # pretty format should not print a 'no files found' message when a file matched and linted clean
    assert "no matching .pipeline" not in (cp.stdout + cp.stderr).lower()


def test_cli_finds_and_reports_findings(tmp_path: Path):
    yaml_text = """
name: ocpp_bronze
catalog: main
schema: bronze
target: legacy_db  # conflict with modern form -> expect DLT300
"""
    write(tmp_path, "conflict.pipeline.yml", yaml_text)

    cp = run_cli(tmp_path, str(tmp_path))
    assert cp.returncode == 1  # findings triggered failure
    assert "DLT300" in (cp.stdout + cp.stderr)
