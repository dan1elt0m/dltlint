from __future__ import annotations
from pathlib import Path
from dltlint.core import lint_paths

def write(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f

def test_libraries_ok_various_kinds(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
libraries:
  - notebook: { path: ./nb }
  - file: { path: ./f.py }
  - jar: dbfs:/jars/x.jar
  - whl: { path: ./dist/x.whl }
  - maven: { coordinates: "org.slf4j:slf4j-api:2.0.9", exclusions: ["ch.qos.logback:logback-classic"], repo: "https://repo1.maven.org/maven2" }
  - pypi: { package: "duckdb==1.0.0", repo: "https://pypi.org/simple" }
"""
    write(tmp_path, "libs_ok.pipeline.yml", y)
    findings = lint_paths([str(tmp_path)])
    assert findings == [], [f"{x.code}:{x.path}" for x in findings]

def test_libraries_fail_missing_path_and_multi_kind(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
libraries:
  - notebook: {}
  - { notebook: { path: nb.py }, jar: "dbfs:/jars/a.jar" }  # multiple kinds
  - maven: {}
  - pypi: { repo: "https://pypi.org/simple" }
  - 123
"""
    write(tmp_path, "libs_bad.pipeline.yml", y)
    codes = {f.code for f in lint_paths([str(tmp_path)])}
    assert "DLT422" in codes  # missing path
    assert "DLT423" in codes  # multiple kinds
    assert "DLT425" in codes  # bad maven
    assert "DLT426" in codes  # bad pypi
    assert "DLT420" in codes  # non-object entry
