from __future__ import annotations

from pathlib import Path

from dltlint.core import lint_paths


def write(p: Path, name: str, text: str) -> Path:
    f = p / name
    f.write_text(text, encoding="utf-8")
    return f


def test_bundle_pipeline_ok_undotted_and_nested_trigger(tmp_path: Path):
    # Bundle-style: pipeline lives under resources.pipelines.<id>
    # Use UNDOTTED retry keys and nested trigger object.
    yaml_text = """
resources:
  pipelines:
    ocpp_bronze_pipeline:
      name: ocpp_bronze
      catalog: main
      schema: bronze
      numUpdateRetryAttempts: 1
      maxFlowRetryAttempts: 3
      trigger:
        interval: "30 minutes"
      libraries:
        - file:
            path: ./bronze.py
"""
    write(tmp_path, "bundle_ok.pipeline.yml.resources", yaml_text)

    findings = lint_paths([str(tmp_path)])
    assert findings == [], f"Expected no findings, got: {[f.code for f in findings]}"


def test_bundle_misplaced_scalar_under_pipelines(tmp_path: Path):
    # If someone accidentally puts a scalar setting directly under resources.pipelines,
    # it should raise DLT002 (pipeline entry must be an object).
    yaml_text = """
resources:
  pipelines:
    ocpp_bronze_pipeline:
      name: ok
    pipelines.numUpdateRetryAttempts: 1
"""
    write(tmp_path, "bundle_misplaced.pipeline.yml.resources", yaml_text)

    findings = lint_paths([str(tmp_path)])
    codes = [f.code for f in findings]
    assert "DLT002" in codes, f"Expected DLT002 for misplaced scalar, got: {codes}"


def test_bundle_missing_name_warns_at_pipeline_level(tmp_path: Path):
    # Missing 'name' INSIDE the pipeline object should warn with DLT400 at the pipeline path
    yaml_text = """
resources:
  pipelines:
    ocpp_bronze_pipeline:
      catalog: main
      schema: bronze
"""
    write(tmp_path, "bundle_missing_name.pipeline.yml.resources", yaml_text)

    findings = lint_paths([str(tmp_path)])
    codes = [f.code for f in findings]
    assert "DLT400" in codes, f"Expected DLT400 warning, got: {codes}"
    # Verify the path points at the pipeline object (not top-level)
    paths = [f.path for f in findings if f.code == "DLT400"]
    assert any(".resources.pipelines.ocpp_bronze_pipeline" in p for p in paths)


def test_bundle_pipeline_not_ok_undotted_and_nested_trigger_wrong_interval(tmp_path: Path):
    # Bundle-style: pipeline lives under resources.pipelines.<id>
    # Use UNDOTTED retry keys and nested trigger object.
    yaml_text = """
resources:
  pipelines:
    ocpp_bronze_pipeline:
      name: ocpp_bronze
      catalog: main
      schema: bronze
      numUpdateRetryAttempts: 1
      maxFlowRetryAttempts: 3
      trigger:
        interval: "30 mintes"
      libraries:
        - file:
            path: ./bronze.py
"""
    write(tmp_path, "bundle_ok.pipeline.yml.resources", yaml_text)

    findings = lint_paths([str(tmp_path)])
    assert findings != [], f"Expected no findings, got: {[f.code for f in findings]}"

    # Bundle-style scrambled:
    # - Missing 'name' inside pipeline object -> DLT400 (on pipeline object path)
    # - trigger present but wrong type (string) -> DLT104
    # - nested trigger interval invalid -> DLT202
    # - pipelines.numUpdateRetryAttempts misplaced directly under resources.pipelines -> DLT002
    # - libraries entry not object -> DLT420
    # - clusters has forbidden spark_version -> DLT431
    text = """
resources:
  pipelines:
    ocpp_bronze_pipeline:
      catalog: main
      schema: bronze
      trigger: "not_a_mapping"
      libraries:
        - 123
      clusters:
        - spark_version: "14.3.x-scala2.12"
    pipelines.numUpdateRetryAttempts: 5
"""
    write(tmp_path, "scrambled_bundle1.pipeline.yml.resources", text)

    findings = lint_paths([str(tmp_path)])
    codes = {f.code for f in findings}
    paths = {f.path for f in findings}

    assert "DLT400" in codes, f"Expected missing name warning (DLT400). Got: {codes}"
    assert any(
        ".resources.pipelines.ocpp_bronze_pipeline" in p
        for p in paths
        if "DLT400" in {f.code for f in findings if f.path == p}
    ), f"Expected DLT400 at pipeline object path. Got: {paths}"
    assert "DLT104" in codes, f"Expected 'trigger' must be mapping (DLT104). Got: {codes}"
    assert "DLT002" in codes, f"Expected misplaced scalar under resources.pipelines (DLT002). Got: {codes}"
    assert "DLT420" in codes, f"Expected libraries entry must be object (DLT420). Got: {codes}"
    assert "DLT431" in codes, f"Expected forbidden cluster field (DLT431). Got: {codes}"


def test_bundle_scrambled_bad_values_inside_pipeline(tmp_path: Path):
    # - Allow undotted keys but make them invalid by type/format:
    #   * maxFlowRetryAttempts: negative -> DLT401
    #   * numUpdateRetryAttempts: wrong type (string) -> DLT102
    #   * trigger.interval: invalid string -> DLT202
    #   * edition/channel invalid -> DLT201 / DLT200
    #   * configuration key non-string -> DLT410
    text = """
resources:
  pipelines:
    p1:
      name: bronze
      catalog: main
      schema: bronze
      trigger:
        interval: "frequently"
      edition: ENTERPRISE
      channel: nightlies
      configuration:
        123: "value"
"""
    write(tmp_path, "scrambled_bundle2.pipeline.yaml.resources", text)

    findings = lint_paths([str(tmp_path)])
    codes = {f.code for f in findings}

    assert "DLT202" in codes, f"Expected invalid trigger interval (DLT202). Got: {codes}"
    assert "DLT201" in codes, f"Expected invalid edition (DLT201). Got: {codes}"
    assert "DLT200" in codes, f"Expected invalid channel (DLT200). Got: {codes}"
    assert "DLT410" in codes, f"Expected configuration key must be string (DLT410). Got: {codes}"
