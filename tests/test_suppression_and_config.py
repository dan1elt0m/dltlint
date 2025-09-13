from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from dltlint.config import ToolConfig, load_config
from dltlint.core import lint_paths
from dltlint.models import Severity


def w(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f


# ----------------------------
# Inline suppressions (default token)
# ----------------------------


def test_inline_suppressions_disable_specific_rules(tmp_path: Path):
    # This file would normally emit:
    # - DLT010 (unknown field 'bogus')
    # - DLT400 (missing 'name')
    y = """
# dltlint: disable=DLT010,DLT400
catalog: c
schema: s
bogus: true
"""
    w(tmp_path, "suppr.pipeline.yml", y)
    findings = lint_paths([str(tmp_path)], cfg=ToolConfig())
    codes = {f.code for f in findings}
    # Both disabled => no findings
    assert "DLT010" not in codes
    assert "DLT400" not in codes


def test_inline_suppressions_do_not_hide_other_rules(tmp_path: Path):
    # Suppress only DLT400; keep DLT010
    y = """
# dltlint: disable=DLT400
catalog: c
schema: s
bogus: true
"""
    w(tmp_path, "suppr2.pipeline.yml", y)
    findings = lint_paths([str(tmp_path)], cfg=ToolConfig())
    codes = {f.code for f in findings}
    assert "DLT400" not in codes
    assert "DLT010" in codes


# ----------------------------
# Custom suppression token via pyproject
# ----------------------------


def test_custom_inline_token_from_pyproject(tmp_path: Path):
    # Configure a custom token and use it in the file
    pp = """
[tool.dltlint]
inline_disable_token = "lint-off"
"""
    y = """
# lint-off=DLT010
catalog: c
schema: s
bogus: true
"""
    w(tmp_path, "pyproject.toml", pp)
    w(tmp_path, "suppr_custom.pipeline.yml", y)

    cfg = load_config(tmp_path)
    assert cfg.inline_disable_token == "lint-off"

    findings = lint_paths([str(tmp_path)], cfg=cfg)
    codes = {f.code for f in findings}
    assert "DLT010" not in codes  # suppressed by custom token
    # DLT400 still present because 'name' is missing and we didn't disable it
    assert "DLT400" in codes


# ----------------------------
# pyproject: ignore list
# ----------------------------


def test_pyproject_ignore_list(tmp_path: Path):
    # Ignore DLT010 and DLT400 from config
    pp = """
[tool.dltlint]
ignore = ["DLT010", "DLT400"]
"""
    y = """
catalog: c
schema: s
bogus: true  # would trigger DLT010
"""
    w(tmp_path, "pyproject.toml", pp)
    w(tmp_path, "ignored.pipeline.yml", y)

    cfg = load_config(tmp_path)
    findings = lint_paths([str(tmp_path)], cfg=cfg)
    codes = {f.code for f in findings}
    assert "DLT010" not in codes
    assert "DLT400" not in codes


# ----------------------------
# pyproject: severity overrides
# ----------------------------


def test_pyproject_severity_override(tmp_path: Path):
    # Promote DLT400 from WARNING to ERROR
    pp = """
[tool.dltlint.severity_overrides]
DLT400 = "error"
"""
    y = """
catalog: c
schema: s
"""
    w(tmp_path, "pyproject.toml", pp)
    w(tmp_path, "sev_override.pipeline.yml", y)

    cfg = load_config(tmp_path)
    findings = lint_paths([str(tmp_path)], cfg=cfg)
    # There should be exactly one DLT400, and it should be ERROR now
    dlt400 = [f for f in findings if f.code == "DLT400"]
    assert dlt400, "Expected a DLT400 for missing 'name'"
    assert all(f.severity == Severity.ERROR for f in dlt400)


# ----------------------------
# pyproject: require fields
# ----------------------------


def test_pyproject_require_fields_standalone(tmp_path: Path):
    # Require 'catalog' and 'schema'; the file omits 'catalog'
    pp = """
[tool.dltlint]
require = ["catalog", "schema"]
"""
    y = """
name: n
schema: s
"""
    w(tmp_path, "pyproject.toml", pp)
    w(tmp_path, "require_fail.pipeline.yml", y)

    cfg = load_config(tmp_path)
    findings = lint_paths([str(tmp_path)], cfg=cfg)
    msgs = [f.message for f in findings]
    assert any("Missing required field 'catalog'" in m for m in msgs)


def test_pyproject_require_fields_bundle(tmp_path: Path):
    # Same requirement, but bundle style under resources.pipelines
    pp = """
[tool.dltlint]
require = ["catalog", "schema"]
"""
    y = """
resources:
  pipelines:
    p1:
      name: n
      catalog: c
"""
    w(tmp_path, "pyproject.toml", pp)
    w(tmp_path, "bundle_require.pipeline.yml.resources", y)

    cfg = load_config(tmp_path)
    findings = lint_paths([str(tmp_path)], cfg=cfg)
    msgs = [f.message for f in findings]
    # schema is missing inside the pipeline object
    assert any("Missing required field 'schema'" in m for m in msgs)


# ----------------------------
# CLI doc generation smoke test
# ----------------------------


@pytest.mark.parametrize("fname", ["RULES.md", "docs_rules.md"])
def test_cli_gen_rules(tmp_path: Path, fname: str):
    out_path = tmp_path / fname
    cp = subprocess.run(
        [sys.executable, "-m", "dltlint.cli", "--gen-rules", str(out_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert cp.returncode == 0, cp.stderr
    assert out_path.exists(), "RULES.md should be created"
    content = out_path.read_text(encoding="utf-8")
    # Sanity: the markdown table header and a known rule code should be present
    assert "| Code | Title | Default Severity |" in content
    assert "`DLT010`" in content
