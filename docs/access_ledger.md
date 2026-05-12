# Access Ledger

Whenever you read or write data through `datacat`, the access ledger automatically records the activity. This gives catalog administrators visibility into who is using which datasets and when, without requiring any extra steps from you as a user.

---

## What gets logged

Every `get_dataframe()` or `read_blobs()` call records a single entry containing:

| Field | Example | Description |
|---|---|---|
| `timestamp` | `2026-05-12T14:01:03` | When the access occurred (ISO-8601, UTC) |
| `username` | `jsmith` | Your OS username |
| `dataset` | `public.stf.nhsn_hrd_prelim.extract` | The dataset and stage you accessed |
| `action` | `read` | `read` or `write` |

Logging is automatic and transparent — you do not need to call anything yourself.

---

## Where the logs live

Logs are stored in Azure Blob Storage alongside the data, under a path defined by the catalog administrator (e.g. `_access/<catalog_name>/ledger/`). A new file is created each day:

```
_access/<catalog_name>/ledger/
├── 2026-05-10.jsonl
├── 2026-05-11.jsonl
└── 2026-05-12.jsonl
```

Each file is plain [JSONL](https://jsonlines.org/) — one JSON record per line — so it can be read directly with pandas or polars:

```python
import polars as pl

log = datacat.<catalog>.<dataset>.extract._ledger_endpoint.get_dataframe("pl")
```

A typical file looks like:

```jsonl
{"timestamp": "2026-05-12T14:01:03", "username": "jsmith", "dataset": "public.stf.nhsn_hrd_prelim.extract", "action": "read"}
{"timestamp": "2026-05-12T14:05:17", "username": "alopez", "dataset": "public.stf.nhsn_hrd_prelim.load", "action": "write"}
```

