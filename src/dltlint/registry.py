from __future__ import annotations

from dataclasses import dataclass

from .models import Severity


@dataclass(frozen=True)
class RuleInfo:
    code: str
    title: str
    default_severity: Severity
    description: str


# Minimal registry for the rules we emit today.
# (If you add new rules, declare them here once; they'll show up in RULES.md.)
RULES: dict[str, RuleInfo] = {
    "DLT001": RuleInfo(
        "DLT001", "Top-level must be mapping", Severity.ERROR, "The root of the YAML/JSON must be an object (mapping)."
    ),
    "DLT002": RuleInfo(
        "DLT002",
        "Pipeline entry must be an object",
        Severity.ERROR,
        "Each key under resources.pipelines must map to a pipeline object.",
    ),
    "DLT010": RuleInfo("DLT010", "Unknown field", Severity.WARNING, "Field not recognized for this schema level."),
    "DLT100": RuleInfo("DLT100", "Type error: string", Severity.ERROR, "Value must be a string."),
    "DLT101": RuleInfo("DLT101", "Type error: boolean", Severity.ERROR, "Value must be a boolean."),
    "DLT102": RuleInfo("DLT102", "Type error: integer", Severity.ERROR, "Value must be an integer."),
    "DLT103": RuleInfo("DLT103", "Type error: list/array", Severity.ERROR, "Value must be a list/array."),
    "DLT104": RuleInfo("DLT104", "Type error: mapping/object", Severity.ERROR, "Value must be a mapping/object."),
    "DLT200": RuleInfo("DLT200", "Invalid channel", Severity.ERROR, "Channel must be one of: current, preview, CURRENT, PREVIEW."),
    "DLT201": RuleInfo("DLT201", "Invalid edition", Severity.ERROR, "Edition must be one of: CORE, PRO, ADVANCED."),
    "DLT202": RuleInfo(
        "DLT202",
        "Invalid trigger interval",
        Severity.ERROR,
        "Trigger interval must be '<n> <unit>' (seconds/minutes/hours/days).",
    ),
    "DLT300": RuleInfo(
        "DLT300",
        "Legacy vs modern conflict",
        Severity.ERROR,
        "Use either modern (catalog/schema) or legacy (target/storage), not both.",
    ),
    "DLT400": RuleInfo(
        "DLT400", "Missing recommended field 'name'", Severity.WARNING, "Provide a pipeline name for clarity."
    ),
    "DLT401": RuleInfo("DLT401", "Negative numeric not allowed", Severity.ERROR, "Retries must be >= 0."),
    "DLT410": RuleInfo(
        "DLT410", "configuration keys must be strings", Severity.ERROR, "All configuration keys must be strings."
    ),
    "DLT411": RuleInfo(
        "DLT411",
        "configuration values should be scalars",
        Severity.WARNING,
        "Prefer string/number/bool values for configuration.",
    ),
    "DLT420": RuleInfo(
        "DLT420", "libraries entry must be object", Severity.ERROR, "Each libraries item must be a mapping."
    ),
    "DLT421": RuleInfo(
        "DLT421", "library kind missing", Severity.WARNING, "Specify one of notebook|file|jar|whl|maven|pypi."
    ),
    "DLT422": RuleInfo("DLT422", "library requires path", Severity.ERROR, "Notebook/file/jar/whl require a path."),
    "DLT423": RuleInfo(
        "DLT423", "multiple library kinds", Severity.WARNING, "Specify exactly one library kind per item."
    ),
    "DLT425": RuleInfo(
        "DLT425",
        "invalid maven spec",
        Severity.ERROR,
        "Maven must include 'coordinates'; optional 'exclusions' (list[str]) and 'repo' (str).",
    ),
    "DLT426": RuleInfo(
        "DLT426", "invalid pypi spec", Severity.ERROR, "PyPI must include 'package'; optional 'repo' (str)."
    ),
    "DLT431": RuleInfo(
        "DLT431", "forbidden cluster field", Severity.ERROR, "Field is managed by Lakeflow and must not be set."
    ),
    "DLT440": RuleInfo(
        "DLT440", "notification entry must be object", Severity.ERROR, "Each notification must be a mapping."
    ),
    "DLT450": RuleInfo(
        "DLT450", "invalid email_recipients", Severity.ERROR, "Provide a non-empty list of string recipients."
    ),
    "DLT451": RuleInfo(
        "DLT451", "notification flag must be boolean", Severity.ERROR, "Flags like on_update_* must be true/false."
    ),
    "DLT460": RuleInfo("DLT460", "num_workers must be int", Severity.ERROR, "Cluster num_workers must be an integer."),
    "DLT461": RuleInfo(
        "DLT461", "num_workers must be >= 0", Severity.ERROR, "Cluster num_workers must be non-negative."
    ),
    "DLT462": RuleInfo(
        "DLT462",
        "autoscale must be object",
        Severity.ERROR,
        "autoscale must be an object with min_workers and max_workers.",
    ),
    "DLT463": RuleInfo(
        "DLT463", "autoscale min/max must be int", Severity.ERROR, "both min_workers and max_workers must be integers."
    ),
    "DLT464": RuleInfo(
        "DLT464", "autoscale min/max must be >= 0", Severity.ERROR, "min/max_workers must be non-negative."
    ),
    "DLT465": RuleInfo("DLT465", "autoscale min <= max", Severity.ERROR, "min_workers must be <= max_workers."),
    "DLT466": RuleInfo(
        "DLT466",
        "num_workers vs autoscale conflict",
        Severity.WARNING,
        "Specify either num_workers or autoscale, not both.",
    ),
    "DLT467": RuleInfo(
        "DLT467",
        "node/policy must be string",
        Severity.ERROR,
        "node_type_id/driver_node_type_id/policy_id must be strings.",
    ),
    "DLT468": RuleInfo(
        "DLT468",
        "mapping must be str->str",
        Severity.ERROR,
        "spark_conf/custom_tags must map string keys to string values.",
    ),
}


def rules_markdown() -> str:
    lines: list[str] = ["# dltlint Rules", "", "| Code | Title | Default Severity | Description |", "|---|---|---|---|"]
    for code in sorted(RULES.keys()):
        r = RULES[code]
        lines.append(f"| `{r.code}` | {r.title} | {r.default_severity.value} | {r.description} |")
    return "\n".join(lines)
