from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import sys
from pathlib import Path

from .config import load_config
from .core import find_pipeline_files, lint_paths, severity_rank
from .models import Finding, Severity
from .registry import rules_markdown

__version__ = importlib.metadata.version("dltlint")


def _pretty(findings: list[Finding]) -> None:
    for x in findings:
        sym = {Severity.ERROR: "✖", Severity.WARNING: "⚠", Severity.INFO: "ℹ"}[Severity(x.severity)]
        print(f"{sym} {x.code} {x.path}: {x.message}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Linter for Databricks Lakeflow (DLT) pipeline YAML/JSON configs")
    p.add_argument(
        "paths",
        nargs="*",
        help="Files or directories. Files must end with .pipeline.yml/.pipeline.yaml; directories are searched recursively.",
    )
    p.add_argument("--format", choices=["pretty", "json"], default="pretty", help="Output format (default: pretty)")
    p.add_argument(
        "--fail-on",
        choices=[s.value for s in Severity],
        help="Exit non-zero if any finding at or above this severity is present (default: from config or 'error')",
    )
    p.add_argument("--quiet", action="store_true", help="Suppress 'no files found' message (still exits 0)")
    p.add_argument("--ok", action="store_true", help="Print a success message when no findings are found")
    p.add_argument("--version", action="store_true", help="Print version and exit")
    p.add_argument("--gen-rules", metavar="PATH", help="Write RULES.md to PATH and exit")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(__version__)
        return 0

    if args.gen_rules:
        out = Path(args.gen_rules)
        out.write_text(rules_markdown(), encoding="utf-8")
        print(f"Wrote rules to {out}")
        return 0

    # Load config from nearest pyproject.toml
    cfg = load_config(Path.cwd())

    # CLI override of fail_on
    fail_on = Severity(args.fail_on) if args.fail_on else cfg.fail_on

    # Force root scan if invoked via: pre-commit run --all-files
    input_paths = ["."] if os.getenv("PRE_COMMIT_RUN_ALL_FILES") == "true" else args.paths if args.paths else ["."]

    # 1) Find matching files first
    matched_files = find_pipeline_files(input_paths)
    if not matched_files:
        if not args.quiet and args.format == "pretty":
            print("dltlint: no matching .pipeline.yml/.pipeline.yaml files found")
        return 0  # pre-commit friendly

    # 2) Lint with config applied (config.ignore, severity_overrides, inline suppressions, require)
    try:
        findings = lint_paths(args.paths, cfg=cfg)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 2

    # 3) Output
    if not findings:
        if args.ok and args.format == "pretty":
            print(f"✔ No issues found in {len(matched_files)} pipeline file(s)")
        return 0

    if args.format == "json":
        print(json.dumps([x.model_dump() for x in findings], indent=2))
    else:
        _pretty(findings)

    worst = max([severity_rank(x.severity) for x in findings], default=-1)
    threshold = severity_rank(fail_on)
    return 1 if (findings and worst >= threshold) else 0


if __name__ == "__main__":
    raise SystemExit(main())
