from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .models import Severity


@dataclass
class ToolConfig:
    fail_on: Severity = Severity.ERROR
    ignore: List[str] = field(default_factory=list)           # e.g., ["DLT010", "DLT400"]
    require: List[str] = field(default_factory=list)          # e.g., ["catalog", "schema"]
    severity_overrides: Dict[str, Severity] = field(default_factory=dict)  # {"DLT400": "info"}

    inline_disable_token: str = "dltlint: disable"            # comment token for inline suppression


def _coerce_severity(x: str) -> Severity:
    return Severity(x.lower())


def _read_pyproject(start: Path) -> Optional[dict]:
    """
    Load nearest pyproject.toml and return parsed dict or None.
    Uses tomllib (3.11+) or tomli (<3.11).
    """
    # walk up directories
    cur = start.resolve()
    for parent in [cur, *cur.parents]:
        pp = parent / "pyproject.toml"
        if pp.exists():
            if sys.version_info >= (3, 11):
                import tomllib
                with pp.open("rb") as f:
                    return tomllib.load(f)
            else:
                import tomli  # type: ignore
                with pp.open("rb") as f:
                    return tomli.load(f)
    return None


def load_config(cwd: Path) -> ToolConfig:
    data = _read_pyproject(cwd) or {}
    table = data.get("tool", {}).get("dltlint", {})
    cfg = ToolConfig()

    if not isinstance(table, dict):
        return cfg

    if isinstance(table.get("fail_on"), str):
        cfg.fail_on = _coerce_severity(table["fail_on"])
    if isinstance(table.get("ignore"), list):
        cfg.ignore = [str(x).strip() for x in table["ignore"] if isinstance(x, (str, int))]
    if isinstance(table.get("require"), list):
        cfg.require = [str(x).strip() for x in table["require"] if isinstance(x, str)]
    if isinstance(table.get("severity_overrides"), dict):
        out: Dict[str, Severity] = {}
        for k, v in table["severity_overrides"].items():
            try:
                out[str(k).strip()] = _coerce_severity(str(v))
            except Exception:
                continue
        cfg.severity_overrides = out

    token = table.get("inline_disable_token")
    if isinstance(token, str) and token.strip():
        cfg.inline_disable_token = token.strip()

    return cfg


def read_inline_suppressions(path: Path, token: str) -> List[str]:
    """
    File-level inline suppressions: any line containing e.g.
      # dltlint: disable=DLT010,DLT400
    will disable those codes for the entire file.

    (Line-scoped suppression would require YAML node line tracking; we keep it simple for now.)
    """
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    codes: List[str] = []
    for line in txt.splitlines():
        if token in line:
            # extract after token; allow comma or space separated
            idx = line.index(token) + len(token)
            frag = line[idx:].strip()
            if frag.startswith("="):
                frag = frag[1:].strip()
            for part in frag.replace(",", " ").split():
                if part.upper().startswith("DLT"):
                    codes.append(part.upper())
    return codes
