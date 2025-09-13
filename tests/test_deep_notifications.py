from __future__ import annotations

from pathlib import Path

from dltlint.core import lint_paths


def write(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f


def test_notifications_ok(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
notifications:
  - email_recipients: ["a@example.com","b@example.com"]
    on_update_failure: true
    on_flow_failure: true
"""
    write(tmp_path, "notif_ok.pipeline.yml", y)
    findings = lint_paths([str(tmp_path)])
    assert findings == [], [f"{x.code}:{x.path}" for x in findings]


def test_notifications_fail(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
notifications:
  - "not_an_object"
  - email_recipients: []
  - email_recipients: ["x@example.com"]
    on_update_start: "yes"
"""
    write(tmp_path, "notif_bad.pipeline.yml", y)
    codes = {f.code for f in lint_paths([str(tmp_path)])}
    assert "DLT440" in codes  # non-object
    assert "DLT450" in codes  # recipients list invalid/empty
    assert "DLT451" in codes  # flag must be boolean
