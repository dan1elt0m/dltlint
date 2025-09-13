from __future__ import annotations
from pathlib import Path
from dltlint.core import lint_paths

def write(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f

def test_clusters_ok_num_workers(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
clusters:
  - num_workers: 2
    node_type_id: i3.xlarge
    spark_conf: { "spark.sql.shuffle.partitions": "4" }
    custom_tags: { env: dev }
"""
    write(tmp_path, "cl_ok.pipeline.yml", y)
    findings = lint_paths([str(tmp_path)])
    assert findings == [], [f"{x.code}:{x.path}" for x in findings]

def test_clusters_ok_autoscale(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
clusters:
  - autoscale: { min_workers: 1, max_workers: 3 }
"""
    write(tmp_path, "cl_ok2.pipeline.yml", y)
    findings = lint_paths([str(tmp_path)])
    assert findings == [], [f"{x.code}:{x.path}" for x in findings]

def test_clusters_fail_cases(tmp_path: Path):
    y = """
name: n
catalog: c
schema: s
clusters:
  - spark_version: "14.3"     # forbidden -> DLT431
  - num_workers: -1           # DLT461
  - autoscale: "x"            # DLT462
  - autoscale: { min_workers: 4, max_workers: 2 }  # DLT465
  - num_workers: 2
    autoscale: { min_workers: 1, max_workers: 3 }  # DLT466
  - node_type_id: 123         # DLT467
  - spark_conf: { a: 1 }      # DLT468 (values must be strings)
"""
    write(tmp_path, "cl_bad.pipeline.yml", y)
    codes = {f.code for f in lint_paths([str(tmp_path)])}
    assert "DLT431" in codes
    assert "DLT461" in codes
    assert "DLT462" in codes or "DLT463" in codes
    assert "DLT465" in codes
    assert "DLT466" in codes
    assert "DLT467" in codes
    assert "DLT468" in codes
