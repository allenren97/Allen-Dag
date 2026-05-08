# Export connectors

Export connectors take a local parquet file (the path returned by an
import connector via XCom) and upload it to a remote object store.
The work is split across the same three layers as the import side so
adding a new export target is also a one-file change.

```text
common_lib/connector_class/
├── __init__.py                # auto-discovers concrete connectors → IMPORT_CONNECTORS / EXPORT_CONNECTORS
├── bases/
│   ├── connector.py           #   BaseConnector — root identity layer
│   └── export_connector.py    #   BaseExportConnector — template-method upload
└── wasb.py                    # AzureExportConnector (EXPORT = "azure_blob")
```

## Class hierarchy

### `BaseConnector` — `bases/connector.py`

Same root identity layer used by the import side. Owns
`connection_id`, `database`, `schema`, `table`, `full_table_name`, and
the per-instance logger; rejects missing identifiers with a single
error listing every absent field.

See [IMPORT_CONNECTORS.md](IMPORT_CONNECTORS.md#baseconnector--basesconnectorpy)
for the full breakdown — the contract is identical.

### `BaseExportConnector` — `bases/export_connector.py`

Engine-agnostic orchestration. `upload(local_parquet_path, **context)`
is the public entry point and is **never overridden** — it runs the
same flow for every target:

1. Validate the local file: present, regular file, non-empty.
2. Derive the remote key via `_build_blob_name(local, **context)`. The
   default layout is
   `<dag_id>/<database>/<schema>/<table>/<filename>`, which mirrors
   the on-disk landing layout. Override only if the target needs a
   different key shape.
3. Build a human-readable destination URI via `_build_target_uri`.
4. Call the engine-specific `_upload_to_target` to actually transfer
   the bytes.
5. Log start (with size + overwrite flag) and completion (with URI).
6. If `delete_local=True`, remove the local parquet — but only after
   a successful upload, and a failure to delete is logged at
   `WARNING`, not raised.
7. Return the URI string for XCom.

Constructor signature is fixed:

```python
__init__(self, connection_id_export, database, schema, table,
         container_name=None, overwrite=True, delete_local=False)
```

`container_name` falls back to `DEFAULT_CONTAINER` (a class attribute
each export sets) and is required to be non-empty after that fallback.
Use it to mean whatever fits your target — bucket, filesystem prefix,
container, etc.

### Concrete connectors — one file per target

Each declares `EXPORT = "<name>"` and overrides exactly two methods:

| Method | Override responsibility |
| --- | --- |
| `_build_target_uri(blob_name) -> str` | Human-readable URI (`wasb://…`, `s3://…`, …); used for logs and as the return value of `upload`. |
| `_upload_to_target(local, blob_name, **context) -> None` | The actual provider-specific transfer. |

May also override:

| Method / attribute | When to override |
| --- | --- |
| `DEFAULT_CONTAINER` | Set the fallback container / bucket. Read from an env var if you want it deployment-driven. |
| `_build_blob_name(local, **context) -> str` | Only if the target needs a different remote key layout from the default `<dag_id>/<database>/<schema>/<table>/<filename>`. |

**Provider hooks are imported lazily** inside the override methods —
the package keeps loading even when an optional Airflow provider isn't
installed.

Initial set:

| File | Class | `EXPORT` | Underlying client |
| --- | --- | --- | --- |
| `wasb.py` | `AzureExportConnector` | `azure_blob` | `WasbHook.load_file` (apache-airflow-providers-microsoft-azure) |

`AzureExportConnector` reads `AZURE_BLOB_CONTAINER` (default `"blob"`)
for `DEFAULT_CONTAINER`, and defensively calls `create_container`
before upload — narrowly swallowing the "already exists" error so
real credential / permission failures still surface.

## Auto-discovery / registry

`__init__.py` walks every concrete sibling module at import time
(skipping `bases/` and any module whose optional dep is missing), then
walks every subclass of `BaseConnector` and builds the registry keyed
by `EXPORT`:

```python
EXPORT_CONNECTORS: dict[str, type[BaseExportConnector]]
# {"azure_blob": AzureExportConnector, ...}
```

Duplicate `EXPORT` values raise at import time. The generated
`task/upload.py` reads the optional `export_engine` field from each
YAML; when only one export is registered the field may be omitted and
the single registered export is inferred. Once a **second** export
exists every YAML must explicitly declare its `export_engine`.

## Adding a new export connector

1. Drop a new file alongside `wasb.py` named `<engine>.py` (e.g.
   `s3.py`).
2. Subclass `BaseExportConnector`, set `EXPORT = "<engine>"`, and
   implement `_build_target_uri` and `_upload_to_target`. Import the
   provider hook lazily inside `_upload_to_target`.
3. Optionally set `DEFAULT_CONTAINER` (an env var read at class scope
   is a good pattern for deployment-driven defaults).
4. Add `export_engine: <engine>` to any YAML that should use it (or
   leave it off if you delete the previous export and only the new
   one remains).

No edits to `__init__.py`, scaffold templates, existing `upload.py`
files, or generated DAG files are needed. See the root [README.md](../../README.md#adding-a-new-export-connector)
for a worked S3 example.

## Required contract — quick reference

| Element | Value |
| --- | --- |
| Base class | Subclass of `BaseExportConnector` |
| Class attribute | `EXPORT: str` — non-empty, unique, lowercase |
| Methods to override | `_build_target_uri`, `_upload_to_target` |
| Methods you may override | `DEFAULT_CONTAINER`, `_build_blob_name` |
| Methods inherited (do **not** override unless you must) | `upload`, `__init__` |
| Constructor | `__init__(connection_id_export, database, schema, table, container_name=None, overwrite=True, delete_local=False)` |
