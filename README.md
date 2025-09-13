# dltlint

Lint Databricks **Lakeflow (DLT)** pipeline YAML/JSON files.

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
