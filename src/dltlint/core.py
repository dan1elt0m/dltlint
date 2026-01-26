from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .config import ToolConfig, read_inline_suppressions
from .models import Finding, Severity

try:
    import yaml  # PyYAML
except Exception as e:  # pragma: no cover
    raise RuntimeError("PyYAML is required. Install with: pip install pyyaml") from e


class LintConfig(BaseModel):
    # Reserved for future strictness toggles or rule config.
    pass


# ---- Spec / constants ------------------------------------------------------

CHANNEL_VALUES = {"current", "preview"}
EDITION_VALUES = {"CORE", "PRO", "ADVANCED"}
TRIGGER_INTERVAL_RE = re.compile(r"^\s*(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days)\s*$")

# Top-level fields for a **standalone** pipeline file.
KNOWN_FIELDS_STANDALONE: dict[str, Any] = {
    "id": str,
    "name": str,
    "configuration": dict,
    "libraries": list,
    "clusters": list,
    "development": bool,
    "notifications": list,
    "continuous": bool,
    "catalog": str,
    "schema": str,
    "target": str,
    "storage": str,
    "channel": str,
    "edition": str,
    "photon": bool,
    "trigger": dict,
    "resources": dict,
    "pipelines.trigger.interval": str,
    "root_path": str,
}


# Fields expected **inside** a bundle pipeline object at `resources.pipelines.<id>`
KNOWN_FIELDS_PIPELINE_OBJ: dict[str, Any] = {
    "id": str,
    "name": str,
    "configuration": dict,
    "libraries": list,
    "clusters": list,
    "development": bool,
    "notifications": list,
    "continuous": bool,
    "catalog": str,
    "schema": str,
    "target": str,
    "storage": str,
    "channel": str,
    "edition": str,
    "photon": bool,
    "maxFlowRetryAttempts": int,
    "numUpdateRetryAttempts": int,
    "trigger": dict,
    "serverless": bool,
    "environment": dict,
    "root_path": str,
}

CLUSTER_FORBIDDEN_FIELDS = {
    "cluster_name",
    "data_security_mode",
    "access_mode",
    "spark_version",
    "autotermination_minutes",
    "runtime_engine",
    "effective_spark_version",
    "cluster_source",
    "docker_image",
    "workload_type",
}

KNOWN_FIELDS_PIPELINE_CONFIGURATION_OBJ = {
    "pipelines.maxFlowRetryAttempts": int,
    "pipelines.numUpdateRetryAttempts": int,
    "pipelines.trigger.interval": str,
}

# ---- IO utilities ----------------------------------------------------------


def _load_doc(path: Path) -> tuple[Any, str]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text), "json"
    return yaml.safe_load(text), "yaml"


def _type_name(x: Any) -> str:  # noqa ANN401
    return type(x).__name__


# ---- Deep validators  ---


def _validate_libraries(doc: dict[str, Any], root: str) -> list[Finding]:
    f: list[Finding] = []
    libs = doc.get("libraries")
    if not isinstance(libs, list):
        return f
    kinds = ["notebook", "file", "jar", "whl", "maven", "pypi"]
    for i, item in enumerate(libs):
        loc = f"{root}.libraries[{i}]"
        if not isinstance(item, dict):
            f.append(Finding(code="DLT420", message="libraries entries must be objects", path=loc))
            continue
        present = [k for k in kinds if k in item]
        if len(present) == 0:
            f.append(
                Finding(
                    code="DLT421",
                    message="library should specify one of: notebook|file|jar|whl|maven|pypi",
                    path=loc,
                    severity=Severity.WARNING,
                )
            )
            continue
        if len(present) > 1:
            f.append(
                Finding(
                    code="DLT423",
                    message=f"library must specify exactly one kind, found {present}",
                    path=loc,
                    severity=Severity.WARNING,
                )
            )

        kind = present[0]

        def _require_path(obj: Any, where: str) -> None:  # noqa ANN401
            if isinstance(obj, str):
                return
            if not isinstance(obj, dict) or not isinstance(obj.get("path"), str):
                f.append(
                    Finding(code="DLT422", message="library requires a string or an object with 'path'", path=where)
                )

        if kind in ("notebook", "file"):
            o = item[kind]
            if not (isinstance(o, dict) and isinstance(o.get("path"), str)):
                f.append(Finding(code="DLT422", message=f"{kind} must be an object with 'path'", path=loc))
        elif kind in ("jar", "whl"):
            _require_path(item[kind], loc)
        elif kind == "maven":
            o = item[kind]
            if not isinstance(o, dict) or not isinstance(o.get("coordinates"), str):
                f.append(
                    Finding(
                        code="DLT425",
                        message="maven requires object with 'coordinates' (e.g., group:artifact:version)",
                        path=loc,
                    )
                )
            else:
                if "exclusions" in o and (
                    not isinstance(o["exclusions"], list) or not all(isinstance(x, str) for x in o["exclusions"])
                ):
                    f.append(
                        Finding(
                            code="DLT425",
                            message="maven.exclusions must be a list of strings",
                            path=f"{loc}.maven.exclusions",
                        )
                    )
                if "repo" in o and not isinstance(o["repo"], str):
                    f.append(Finding(code="DLT425", message="maven.repo must be a string", path=f"{loc}.maven.repo"))
        elif kind == "pypi":
            o = item[kind]
            if not isinstance(o, dict) or not isinstance(o.get("package"), str):
                f.append(
                    Finding(
                        code="DLT426", message="pypi requires object with 'package' (e.g., 'duckdb==1.0.0')", path=loc
                    )
                )
            elif "repo" in o and not isinstance(o["repo"], str):
                f.append(Finding(code="DLT426", message="pypi.repo must be a string", path=f"{loc}.pypi.repo"))
    return f


def _validate_notifications(doc: dict[str, Any], root: str) -> list[Finding]:
    f: list[Finding] = []
    notifs = doc.get("notifications")
    if not isinstance(notifs, list):
        return f
    for i, n in enumerate(notifs):
        loc = f"{root}.notifications[{i}]"
        if not isinstance(n, dict):
            f.append(Finding(code="DLT440", message="notification entry must be an object", path=loc))
            continue
        recipients = n.get("email_recipients")
        if not (isinstance(recipients, list) and recipients and all(isinstance(x, str) for x in recipients)):
            f.append(
                Finding(
                    code="DLT450", message="notification.email_recipients must be a non-empty list of strings", path=loc
                )
            )
        for flag in ("on_update_start", "on_update_success", "on_update_failure", "on_flow_failure"):
            if flag in n and not isinstance(n[flag], bool):
                f.append(Finding(code="DLT451", message=f"notification.{flag} must be a boolean", path=f"{loc}.{flag}"))
    return f


def _validate_clusters(doc: dict[str, Any], root: str) -> list[Finding]:
    f: list[Finding] = []
    clusters = doc.get("clusters")
    if not isinstance(clusters, list):
        return f
    for i, cl in enumerate(clusters):
        loc = f"{root}.clusters[{i}]"
        if not isinstance(cl, dict):
            f.append(Finding(code="DLT430", message="clusters entries must be objects", path=loc))
            continue
        forbidden = CLUSTER_FORBIDDEN_FIELDS.intersection(cl.keys())
        if forbidden:
            f.append(
                Finding(
                    code="DLT431",
                    message="These cluster fields are managed by Lakeflow and must not be set: "
                    + ", ".join(sorted(forbidden)),
                    path=loc,
                )
            )

        nw = cl.get("num_workers")
        as_ = cl.get("autoscale")
        if nw is not None and not isinstance(nw, int):
            f.append(Finding(code="DLT460", message="num_workers must be an integer", path=f"{loc}.num_workers"))
        if isinstance(nw, int) and nw < 0:
            f.append(Finding(code="DLT461", message="num_workers must be >= 0", path=f"{loc}.num_workers"))

        if as_ is not None and not isinstance(as_, dict):
            f.append(
                Finding(
                    code="DLT462",
                    message="autoscale must be an object with 'min_workers' and 'max_workers'",
                    path=f"{loc}.autoscale",
                )
            )
        elif isinstance(as_, dict):
            mw = as_.get("min_workers")
            xw = as_.get("max_workers")
            if not (isinstance(mw, int) and isinstance(xw, int)):
                f.append(
                    Finding(
                        code="DLT463",
                        message="autoscale.min_workers and autoscale.max_workers must be integers",
                        path=f"{loc}.autoscale",
                    )
                )
            else:
                if mw < 0 or xw < 0:
                    f.append(
                        Finding(
                            code="DLT464", message="autoscale min/max workers must be >= 0", path=f"{loc}.autoscale"
                        )
                    )
                if mw > xw:
                    f.append(
                        Finding(
                            code="DLT465",
                            message="autoscale.min_workers must be <= autoscale.max_workers",
                            path=f"{loc}.autoscale",
                        )
                    )
        if isinstance(nw, int) and isinstance(as_, dict):
            f.append(
                Finding(
                    code="DLT466",
                    message="Specify either 'num_workers' or 'autoscale', not both",
                    path=loc,
                    severity=Severity.WARNING,
                )
            )

        for fld in ("node_type_id", "driver_node_type_id", "policy_id"):
            if fld in cl and not isinstance(cl[fld], str):
                f.append(Finding(code="DLT467", message=f"{fld} must be a string", path=f"{loc}.{fld}"))

        for mapfld in ("spark_conf", "custom_tags"):
            m = cl.get(mapfld)
            if m is not None:  # noqa SIM102
                if (
                    not isinstance(m, dict)
                    or not all(isinstance(k, str) for k in m)
                    or not all(isinstance(v, str) for v in m.values())
                ):
                    f.append(
                        Finding(
                            code="DLT468",
                            message=f"{mapfld} must be a mapping of string->string",
                            path=f"{loc}.{mapfld}",
                        )
                    )
    return f


ValueType = str | bool | int | list | dict


# ---- Rule runner -----------------------------------------------------------
def check_expected_type(v: ValueType, root: str, k: str, expected: ValueType, f: list[Finding]) -> bool:
    """Check if value is of expected type, append to findings if not. Returns True if type matches."""

    if expected is str and not isinstance(v, str):
        f.append(
            Finding(code="DLT100", message=f"Field '{k}' must be a string, got {_type_name(v)}", path=f"{root}.{k}")
        )
    elif expected is bool and not isinstance(v, bool):
        f.append(
            Finding(code="DLT101", message=f"Field '{k}' must be a boolean, got {_type_name(v)}", path=f"{root}.{k}")
        )
    elif expected is int and not isinstance(v, int):
        f.append(
            Finding(
                code="DLT102",
                message=f"Field '{k}' must be an integer, got {_type_name(v)}",
                path=f"{root}.{k}",
            )
        )
    elif expected is list and not isinstance(v, list):
        f.append(
            Finding(
                code="DLT103",
                message=f"Field '{k}' must be a list/array, got {_type_name(v)}",
                path=f"{root}.{k}",
            )
        )
    elif expected is dict and not isinstance(v, dict):
        f.append(
            Finding(
                code="DLT104",
                message=f"Field '{k}' must be a mapping/object, got {_type_name(v)}",
                path=f"{root}.{k}",
            )
        )


def _lint_schema(doc: dict[str, Any], known_fields: dict[str, ValueType], *, root: str) -> list[Finding]:
    f: list[Finding] = []

    for k, v in doc.items():
        if k not in known_fields:
            f.append(
                Finding(
                    code="DLT010",
                    message=f"Unknown top-level field '{k}'",
                    path=f"{root}.{k}",
                    severity=Severity.WARNING,
                )
            )
        else:
            check_expected_type(v, root, k, known_fields[k], f)

    if "channel" in doc and isinstance(doc["channel"], str):  # noqa SIM102
        if doc["channel"] not in CHANNEL_VALUES:
            f.append(
                Finding(
                    code="DLT200", message=f"channel must be one of {sorted(CHANNEL_VALUES)}", path=f"{root}.channel"
                )
            )

    if "edition" in doc and isinstance(doc["edition"], str):  # noqa SIM102
        if doc["edition"] not in EDITION_VALUES:
            f.append(
                Finding(
                    code="DLT201", message=f"edition must be one of {sorted(EDITION_VALUES)}", path=f"{root}.edition"
                )
            )

    if "pipelines.trigger.interval" in doc and isinstance(doc["pipelines.trigger.interval"], str):  # noqa SIM102
        if not TRIGGER_INTERVAL_RE.match(doc["pipelines.trigger.interval"]):
            f.append(
                Finding(
                    code="DLT202",
                    message="pipelines.trigger.interval must be like '10 minutes' | '1 hour' | '30 seconds'",
                    path=f"{root}.pipelines.trigger.interval",
                )
            )

    if "trigger" in doc:
        if not isinstance(doc["trigger"], dict):
            f.append(Finding(code="DLT104", message="Field 'trigger' must be a mapping/object", path=f"{root}.trigger"))
        else:
            ti = doc["trigger"].get("interval")
            if isinstance(ti, str) and not TRIGGER_INTERVAL_RE.match(ti):
                f.append(
                    Finding(
                        code="DLT202",
                        message="trigger.interval must be like '10 minutes' | '1 hour' | '30 seconds'",
                        path=f"{root}.trigger.interval",
                    )
                )

    has_modern = ("catalog" in doc) or ("schema" in doc)
    has_legacy = ("target" in doc) or ("storage" in doc)
    if has_modern and has_legacy:
        f.append(
            Finding(
                code="DLT300",
                message="Use either modern (catalog/schema) or legacy (target/storage) publishing, not both",
                path=root,
            )
        )

    if "name" not in doc:
        f.append(
            Finding(code="DLT400", message="Missing recommended field 'name'", path=root, severity=Severity.WARNING)
        )

    for key in (
        "pipelines.maxFlowRetryAttempts",
        "pipelines.numUpdateRetryAttempts",
    ):
        if key in doc and isinstance(doc[key], int) and doc[key] < 0:
            f.append(Finding(code="DLT401", message=f"{key} must be >= 0", path=f"{root}.{key}"))

    if isinstance(doc.get("configuration"), dict):
        for ck, cv in doc["configuration"].items():
            if not isinstance(ck, str):
                f.append(
                    Finding(code="DLT410", message="configuration keys must be strings", path=f"{root}.configuration")
                )
                break

            if ck in KNOWN_FIELDS_PIPELINE_CONFIGURATION_OBJ:
                check_expected_type(cv, root, ck, KNOWN_FIELDS_PIPELINE_CONFIGURATION_OBJ[ck], f)

            elif not isinstance(cv, str | int | float | bool):
                f.append(
                    Finding(
                        code="DLT411",
                        message=f"configuration value for '{ck}' should be a scalar (string/number/bool)",
                        path=f"{root}.configuration.{ck}",
                        severity=Severity.WARNING,
                    )
                )

    f.extend(_validate_libraries(doc, root))
    f.extend(_validate_notifications(doc, root))
    f.extend(_validate_clusters(doc, root))
    return f


def lint_pipeline(doc: Any, *, root: str = "$") -> list[Finding]:  # noqa ANN401
    findings: list[Finding] = []

    if not isinstance(doc, dict):
        findings.append(Finding(code="DLT001", message="Top-level must be a mapping/object", path=root))
        return findings

    resources = doc.get("resources")
    if isinstance(resources, dict) and isinstance(resources.get("pipelines"), dict):
        pipelines = resources["pipelines"]
        if not pipelines:
            return findings
        for pid, pobj in pipelines.items():
            if not isinstance(pobj, dict):
                findings.append(
                    Finding(
                        code="DLT002",
                        message=f"Pipeline '{pid}' must be an object",
                        path=f"{root}.resources.pipelines.{pid}",
                    )
                )
                continue
            findings.extend(_lint_schema(pobj, KNOWN_FIELDS_PIPELINE_OBJ, root=f"{root}.resources.pipelines.{pid}"))
        return findings

    findings.extend(_lint_schema(doc, KNOWN_FIELDS_STANDALONE, root=root))
    return findings


# ---- File discovery & orchestration ----------------------------------------


def find_pipeline_files(start_paths: Iterable[str]) -> list[Path]:
    suffixes = (".pipeline.yml", ".pipeline.yaml", ".pipeline.yml.resources", ".pipeline.yaml.resources")
    files: list[Path] = []
    for sp in start_paths:
        p = Path(sp)
        if p.is_file():
            if p.name.endswith(suffixes):
                files.append(p)
        elif p.is_dir():
            for suf in suffixes:
                files.extend(p.rglob(f"*{suf}"))
    return sorted(set(files), key=lambda x: str(x))


def lint_paths(paths: Iterable[str], *, cfg: ToolConfig | None = None) -> list[Finding]:
    """
    Lint and return findings, after applying:
      - inline suppressions (file-level)
      - config.ignore
      - config.severity_overrides
      - config.require (fields required; missing => DLT400-style warning/error depending on override)
    """
    cfg = cfg or ToolConfig()
    all_findings: list[Finding] = []

    for path in find_pipeline_files(paths):
        suppress_codes = set(read_inline_suppressions(path, cfg.inline_disable_token))
        doc, _ = _load_doc(path)
        findings = lint_pipeline(doc, root=str(path))

        # Apply 'require' (simple existence check at the object level(s))
        if isinstance(doc, dict) and cfg.require:
            res = doc.get("resources")
            if isinstance(res, dict) and isinstance(res.get("pipelines"), dict):
                for pid, pobj in res["pipelines"].items():
                    if not isinstance(pobj, dict):
                        continue
                    for need in cfg.require:
                        if need not in pobj:
                            findings.append(
                                Finding(
                                    code="DLT400",
                                    message=f"Missing required field '{need}'",
                                    path=f"{path}.resources.pipelines.{pid}",
                                    severity=Severity.ERROR,
                                )
                            )
            else:
                for need in cfg.require:
                    if need not in doc:
                        findings.append(
                            Finding(
                                code="DLT400",
                                message=f"Missing required field '{need}'",
                                path=str(path),
                                severity=Severity.ERROR,
                            )
                        )

        # Inline suppressions (file-level)
        findings = [f for f in findings if f.code.upper() not in suppress_codes]

        # Ignore list
        if cfg.ignore:
            ig = {c.upper() for c in cfg.ignore}
            findings = [f for f in findings if f.code.upper() not in ig]

        # Severity overrides
        if cfg.severity_overrides:
            so = {k.upper(): v for k, v in cfg.severity_overrides.items()}
            for f in findings:
                if f.code.upper() in so:
                    f.severity = so[f.code.upper()]

        all_findings.extend(findings)

    return all_findings


def severity_rank(s: Severity | str) -> int:
    if isinstance(s, str):
        s = Severity(s)
    return {Severity.INFO: 0, Severity.WARNING: 1, Severity.ERROR: 2}[s]
