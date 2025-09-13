# dltlint

Lint Databricks **Lakeflow (DLT)** pipeline YAML/JSON files.

## Installation

```bash
pip install dltlint
```

## Usage CLI
```shell
# Lint current repo recursively
dltlint 

# JSON output for tooling
dltlint --format json 

# Fail build on warnings or worse
dltlint --fail-on warning 

# Print a success message when clean (otherwise silent on success)
dltlint --ok 
```

Exit codes
- 0 → clean OR no matching files
- 1 → findings at/above threshold (--fail-on)
- 2 → fatal error (e.g., unreadable file)

dltlint discovers:
- *.pipeline.yml,
- *.pipeline.yaml
- *.pipeline.yml.resources,
- *.pipeline.yaml.resources

# Pre-commit
Add to your repo’s .pre-commit-config.yaml:
```
repos:
  - repo: https://github.com/dan1elt0m/dltlint
    rev: v0.0.1        # tag or commit
    hooks:
      - id: dltlint
        # By default the hook runs from repo root and scans recursively.
        # If you prefer to pass staged files only, uncomment:
        # pass_filenames: true
        # args: ["--fail-on", "error"]
```

## Config (pyproject.toml)

```toml
[tool.dltlint]
fail_on = "warning"                       # default: "error"
ignore = ["DLT010", "DLT400"]             # suppress specific rules
require = ["catalog", "schema"]           # fields that must be present
inline_disable_token = "dltlint: disable" # comment token (see below)

[tool.dltlint.severity_overrides]
DLT400 = "info"
```

## Inline suppressions
Add a comment anywhere in a file to suppress rules for that file:
```yaml
# dltlint: disable=DLT010,DLT400
resources:
  pipelines:
    my_pipe:
      name: n
      catalog: c
      schema: s
```

Line-scoped suppressions require YAML line tracking and are not supported yet.
