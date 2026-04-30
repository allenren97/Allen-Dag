# Allen Dag — MSSQL → Parquet → Azure Blob (Airflow)

This repo contains a small Airflow DAG that:

- **Extracts** a table from **Microsoft SQL Server** into a **local Parquet** file
- **Uploads** that Parquet file into **Azure Blob Storage** (WASB)

The pipeline is implemented with two connectors in `connector_class/` and one DAG in `DAG/`.

## Repository layout

- `DAG/mssql_to_azure_blob_dag.py`: the Airflow DAG (two `PythonOperator` tasks)
- `DAG/mock_dataset.py`: example `DAG_CONFIG` used by the DAG (static dict)
- `connector_class/mssql.py`: `MSSQLImportConnector` (MSSQL → Pandas → Parquet)
- `connector_class/wasb.py`: `AzureExportConnector` (local Parquet → Azure Blob)
- `requirements.txt`: minimal pinned dependencies for this project

## What happens when a “dataset comes in”

In this project, “a dataset comes in” means **an Airflow DAG run starts** (scheduled or manually triggered). Airflow does **not** ingest files into this DAG; it *pulls* from MSSQL on each run.

### Phase 0 — DAG parse time (scheduler/webserver imports the file)

When Airflow scans your DAGs folder, it imports `DAG/mssql_to_azure_blob_dag.py`:

- Imports `DAG_CONFIG` from `DAG/mock_dataset.py`
  - This is a **normal Python dict** created at import time (not generated dynamically per run).
- Creates a `DAG(...)` object with:
  - `dag_id = DAG_CONFIG["name"]`
  - tags like `["mssql", "azure_blob", "etl"]`
- Defines two tasks:
  - `extract_to_parquet` → runs `_extract_to_parquet`
  - `upload_to_azure_blob` → runs `_upload_to_azure_blob`

### Phase 1 — Task 1: extract (`extract_to_parquet`)

Airflow executes `_extract_to_parquet(**context)` from `DAG/mssql_to_azure_blob_dag.py`.

**Call chain and key functions**

1. `_extract_to_parquet` reads configuration from `DAG_CONFIG`
2. It instantiates `connector_class.mssql.MSSQLImportConnector(...)`
3. It calls `MSSQLImportConnector.to_parquet(**context)` which:
   - Lazily imports and creates an Airflow **`MsSqlHook`**
     - `from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook`
   - Builds a query with `_build_query()`
     - `SELECT * FROM [schema].[table]` + optional `WHERE <predicate>`
   - Executes the query into a Pandas DataFrame:
     - `hook.get_pandas_df(sql=query)`
   - Chooses an output path with `_resolve_output_path(**context)`
     - Default directory: `<repo_root>/data`
     - Can be overridden by `AIRFLOW_LANDING_DIR`
   - Writes Parquet via Pandas + PyArrow:
     - `df.to_parquet(..., engine="pyarrow", index=False)`
4. The task returns the Parquet file path as a string

**How the Parquet path moves to the next task**

Airflow automatically stores a PythonOperator’s return value in **XCom** under the key `return_value`.

So the extract task’s return value becomes an XCom entry that the next task can pull.

### Phase 2 — Task 2: upload (`upload_to_azure_blob`)

Airflow executes `_upload_to_azure_blob(**context)` from `DAG/mssql_to_azure_blob_dag.py`.

**Call chain and key functions**

1. `_upload_to_azure_blob` pulls the Parquet path from XCom:
   - `ti = context["ti"]` (task instance)
   - `local_parquet_path = ti.xcom_pull(task_ids="extract_to_parquet")`
2. It instantiates `connector_class.wasb.AzureExportConnector(...)`
   - `container_name` is set to the **DAG id** (for example `mssql_customers_to_azure_blob`)
3. It calls `AzureExportConnector.upload(local_parquet_path=..., **context)` which:
   - Lazily imports and creates an Airflow **`WasbHook`**
     - `from airflow.providers.microsoft.azure.hooks.wasb import WasbHook`
   - Ensures the container exists:
     - `hook.create_container(container_name=<dag_id>)`
   - Builds the blob name with:
     - `"/".join([dag_id, database, schema, table, filename])`
   - Uploads with:
     - `hook.load_file(file_path=..., container_name=..., blob_name=..., overwrite=True)`
   - Optionally deletes the local Parquet after upload (`delete_local=True` in the DAG)
4. The task returns a `wasb://...` URI for the uploaded object (also stored in XCom)

## Packages / providers used

Installed from `requirements.txt`:

- `apache-airflow`
- `apache-airflow-providers-microsoft-mssql`
  - Provides `MsSqlHook`
- `apache-airflow-providers-microsoft-azure`
  - Provides `WasbHook`
- `pandas`
- `pyarrow` (Parquet engine used by Pandas)

## Configuration

### 1) Airflow Connections (must exist in the Airflow UI)

The example `DAG_CONFIG` references these connection IDs:

- **MSSQL**: `mssql_default` (type: `mssql`)
- **Azure Blob**: `wasb_default` (type: `wasb`)

These must be created in Airflow under **Admin → Connections** with the correct credentials.

### 2) Environment variables (optional)

- `AIRFLOW_LANDING_DIR`
  - Overrides where Parquet is written locally (default: `<repo_root>/data`)
- `AZURE_BLOB_CONTAINER`
  - Default container name if not provided; this repo currently **overrides** it by passing `container_name=<dag_id>` from the DAG.

## Where to look when something fails

- **DAG not showing in UI**: check **DAG Import Errors** in Airflow UI
- **`ContainerNotFound`**: the upload step creates the container, but your Azure credentials must allow container creation
- **XCom / file path issues**: if tasks run on different workers with separate disks, local paths won’t be visible to the upload task; you’ll need shared storage or a single combined task

