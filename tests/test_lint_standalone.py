from __future__ import annotations

from pathlib import Path

from dltlint.core import lint_paths


def write(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f


def test_standalone_classic_ok(tmp_path: Path):
    # Classic, single pipeline file: dotted keys at top-level
    yaml_text = """
name: ocpp_bronze
catalog: main
schema: bronze
pipelines.trigger.interval: "1 hour"
libraries:
  - file:
      path: ./pipeline.py
configuration:
  foo: "bar"
"""
    write(tmp_path, "ok.pipeline.yml", yaml_text)

    findings = lint_paths([str(tmp_path)])
    assert findings == [], f"Expected no findings, got: {[f.code for f in findings]}"


def test_standalone_legacy_vs_modern_conflict(tmp_path: Path):
    # Using both modern (catalog/schema) and legacy (target/storage) should emit DLT300
    yaml_text = """
name: ocpp_bronze
catalog: main
schema: bronze
target: legacy_db
libraries: []
"""
    write(tmp_path, "conflict.pipeline.yml", yaml_text)

    findings = lint_paths([str(tmp_path)])
    codes = [f.code for f in findings]
    assert "DLT300" in codes, f"Expected DLT300, got {codes}"


def test_standalone_accept_resources_key(tmp_path: Path):
    # Accept presence of top-level `resources` even on standalone files
    yaml_text = """
name: ocpp_bronze
catalog: main
schema: bronze
resources:
  anything: {}
"""
    write(tmp_path, "standalone_with_resources.pipeline.yml", yaml_text)

    findings = lint_paths([str(tmp_path)])
    # Should not warn on the 'resources' key itself
    codes = [f.code for f in findings]
    assert "DLT010" not in codes, f"Unexpected unknown-field warning(s): {codes}"


def test_standalone_scrambled_mixed_modes_bad_types_and_trigger(tmp_path: Path):
    # Intentionally scrambled standalone config:
    # - Both modern (catalog/schema) and legacy (target/storage) -> DLT300
    # - Wrong types: name is a list -> DLT100
    # - trigger (nested) interval invalid -> DLT202
    # - libraries entry not an object -> DLT420
    # - clusters contains forbidden spark_version -> DLT431
    # - configuration value is a list (non-scalar) -> DLT411 (warning)
    # - edition invalid -> DLT201
    # - channel invalid -> DLT200
    text = """
name:
  - not_a_string
catalog: main
schema: bronze
target: legacy_db
storage: /mnt/legacy
channel: nightlies
edition: ENTERPRISE
trigger:
  interval: "often"
libraries:
  - bad  # not an object -> should error
clusters:
  - spark_version: "14.3.x-scala2.12"
configuration:
  key1:
    - list_not_scalar
"""
    write(tmp_path, "scrambled_standalone.pipeline.yml", text)

    findings = lint_paths([str(tmp_path)])
    codes = {f.code for f in findings}

    assert "DLT300" in codes, f"Expected legacy/modern conflict (DLT300). Got: {codes}"
    assert "DLT100" in codes, f"Expected bad type for 'name' (DLT100). Got: {codes}"
    assert "DLT202" in codes, f"Expected invalid trigger interval (DLT202). Got: {codes}"
    assert "DLT420" in codes, f"Expected libraries entry must be object (DLT420). Got: {codes}"
    assert "DLT431" in codes, f"Expected forbidden cluster field spark_version (DLT431). Got: {codes}"
    assert "DLT411" in codes, f"Expected non-scalar configuration value warning (DLT411). Got: {codes}"
    assert "DLT201" in codes, f"Expected invalid edition (DLT201). Got: {codes}"
    assert "DLT200" in codes, f"Expected invalid channel (DLT200). Got: {codes}"


def test_standalone_scrambled_dotted_trigger_and_integers(tmp_path: Path):
    # - Dotted trigger interval invalid -> DLT202
    # - pipelines.numUpdateRetryAttempts wrong type -> DLT102
    text = """
name: ok
catalog: main
schema: bronze
pipelines.trigger.interval: "every once in a while"
configuration:
    pipelines.numUpdateRetryAttempts: "not_an_int"
"""
    write(tmp_path, "scrambled_standalone2.pipeline.yml", text)

    findings = lint_paths([str(tmp_path)])
    codes = {f.code for f in findings}
    assert "DLT202" in codes, f"Expected invalid dotted trigger interval (DLT202). Got: {codes}"
    assert "DLT102" in codes, f"Expected integer type for numUpdateRetryAttempts (DLT102). Got: {codes}"


def test_standalone_uppercase_channel_values_ok(tmp_path: Path):
    # Test that uppercase channel values (CURRENT and PREVIEW) are accepted
    text_current = """
name: test_pipeline
catalog: main
schema: bronze
channel: CURRENT
libraries: []
"""
    write(tmp_path, "uppercase_current.pipeline.yml", text_current)

    text_preview = """
name: test_pipeline2
catalog: main
schema: bronze
channel: PREVIEW
libraries: []
"""
    write(tmp_path, "uppercase_preview.pipeline.yml", text_preview)

    findings = lint_paths([str(tmp_path)])
    codes = {f.code for f in findings}
    
    # Should NOT contain DLT200 (invalid channel)
    assert "DLT200" not in codes, f"Expected no DLT200 errors for uppercase channel values. Got: {codes}"


def test_standalone_lowercase_channel_values_ok(tmp_path: Path):
    # Test that lowercase channel values (current and preview) are still accepted
    text_current = """
name: test_pipeline
catalog: main
schema: bronze
channel: current
libraries: []
"""
    write(tmp_path, "lowercase_current.pipeline.yml", text_current)

    text_preview = """
name: test_pipeline2
catalog: main
schema: bronze
channel: preview
libraries: []
"""
    write(tmp_path, "lowercase_preview.pipeline.yml", text_preview)

    findings = lint_paths([str(tmp_path)])
    codes = {f.code for f in findings}
    
    # Should NOT contain DLT200 (invalid channel)
    assert "DLT200" not in codes, f"Expected no DLT200 errors for lowercase channel values. Got: {codes}"
