# dltlint Rules

| Code | Title | Default Severity | Description |
|---|---|---|---|
| `DLT001` | Top-level must be mapping | error | The root of the YAML/JSON must be an object (mapping). |
| `DLT002` | Pipeline entry must be an object | error | Each key under resources.pipelines must map to a pipeline object. |
| `DLT010` | Unknown field | warning | Field not recognized for this schema level. |
| `DLT100` | Type error: string | error | Value must be a string. |
| `DLT101` | Type error: boolean | error | Value must be a boolean. |
| `DLT102` | Type error: integer | error | Value must be an integer. |
| `DLT103` | Type error: list/array | error | Value must be a list/array. |
| `DLT104` | Type error: mapping/object | error | Value must be a mapping/object. |
| `DLT200` | Invalid channel | error | Channel must be one of: current, preview, CURRENT, PREVIEW. |
| `DLT201` | Invalid edition | error | Edition must be one of: CORE, PRO, ADVANCED. |
| `DLT202` | Invalid trigger interval | error | Trigger interval must be '<n> <unit>' (seconds/minutes/hours/days). |
| `DLT300` | Legacy vs modern conflict | error | Use either modern (catalog/schema) or legacy (target/storage), not both. |
| `DLT400` | Missing recommended field 'name' | warning | Provide a pipeline name for clarity. |
| `DLT401` | Negative numeric not allowed | error | Retries must be >= 0. |
| `DLT410` | configuration keys must be strings | error | All configuration keys must be strings. |
| `DLT411` | configuration values should be scalars | warning | Prefer string/number/bool values for configuration. |
| `DLT420` | libraries entry must be object | error | Each libraries item must be a mapping. |
| `DLT421` | library kind missing | warning | Specify one of notebook|file|jar|whl|maven|pypi|glob. |
| `DLT422` | library requires path | error | Notebook/file/jar/whl require a path. |
| `DLT423` | multiple library kinds | warning | Specify exactly one library kind per item. |
| `DLT425` | invalid maven spec | error | Maven must include 'coordinates'; optional 'exclusions' (list[str]) and 'repo' (str). |
| `DLT426` | invalid pypi spec | error | PyPI must include 'package'; optional 'repo' (str). |
| `DLT427` | invalid glob spec | error | Glob must include 'include' with a path ending with '**'. |
| `DLT431` | forbidden cluster field | error | Field is managed by Lakeflow and must not be set. |
| `DLT440` | notification entry must be object | error | Each notification must be a mapping. |
| `DLT450` | invalid email_recipients | error | Provide a non-empty list of string recipients. |
| `DLT451` | notification flag must be boolean | error | Flags like on_update_* must be true/false. |
| `DLT460` | num_workers must be int | error | Cluster num_workers must be an integer. |
| `DLT461` | num_workers must be >= 0 | error | Cluster num_workers must be non-negative. |
| `DLT462` | autoscale must be object | error | autoscale must be an object with min_workers and max_workers. |
| `DLT463` | autoscale min/max must be int | error | both min_workers and max_workers must be integers. |
| `DLT464` | autoscale min/max must be >= 0 | error | min/max_workers must be non-negative. |
| `DLT465` | autoscale min <= max | error | min_workers must be <= max_workers. |
| `DLT466` | num_workers vs autoscale conflict | warning | Specify either num_workers or autoscale, not both. |
| `DLT467` | node/policy must be string | error | node_type_id/driver_node_type_id/policy_id must be strings. |
| `DLT468` | mapping must be str->str | error | spark_conf/custom_tags must map string keys to string values. |