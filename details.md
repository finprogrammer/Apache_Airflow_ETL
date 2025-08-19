Here’s a clean, drop-in README.md for your repo. You can paste it at the project root.

ML Data Pipeline (Airflow + Astronomer)

A lightweight, production-style data pipeline orchestrated with Apache Airflow (via Astronomer).
The pipeline runs data ingestion → validation → transformation on a schedule, with fast DAG parsing and XCom’d artifact paths.

✨ Features

Airflow 2 DAG (ml_data_pipeline) with clear task chain:

data_ingestion → data_validation → data_transformation


Lazy imports inside tasks → keeps DagBag load fast.

XCom only for small JSON (file paths/flags) — no giant objects.

Config-driven paths with timestamped output in Artifacts/<timestamp>/....

Works with MongoDB Atlas or self-hosted Mongo.

🧱 Project Structure
.
├─ dags/
│  └─ etlpipeline.py                # DAG: defines tasks & orchestration
├─ networksecurity/
│  ├─ components/
│  │  ├─ data_ingestion.py          # Mongo → CSV + train/test split (streaming cursor)
│  │  ├─ data_validation.py         # schema & drift (KS test)
│  │  └─ data_transformation.py     # imputers / preprocessing
│  ├─ constant/
│  │  └─ training_pipeline.py       # constants (paths, names, ratios)
│  ├─ entity/                       # config & artifact dataclasses
│  ├─ exception/                    # NetworkSecurityException
│  └─ utils/                        # yaml/io helpers
├─ data_schema/
│  └─ schema.yaml                   # expected columns, target, etc.
├─ .env                             # contains MONGODB_URL_KEY=...
├─ Dockerfile
├─ requirements.txt
└─ README.md


Note: On Windows, avoid spaces in the project path (prefer E:\udemy\trial_etl over E:\udemy\trial etl) to prevent Docker mount quirks.

⚙️ Prerequisites

Docker Desktop (with WSL2 on Windows)

Astronomer CLI (astro)

Git (optional but recommended)

🚀 Quickstart

Create/Update .env (project root)

# Example: MongoDB Atlas
MONGODB_URL_KEY=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/<db>?retryWrites=true&w=majority

# Example: Local/Container Mongo
# MONGODB_URL_KEY=mongodb://mongodb:27017/<db>


If you edit .env, make sure it’s not in .dockerignore, then restart containers to pick up changes.

Install deps & start Airflow

astro dev start --wait 180


Open Airflow UI at http://localhost:8080
, unpause/trigger the DAG ml_data_pipeline.

Verify env inside the container (optional)

astro dev bash scheduler
printenv | grep MONGODB_URL_KEY

🧩 Configuration (key bits)

networksecurity/constant/training_pipeline.py:

ARTIFACT_DIR = "Artifacts" (folder where outputs are stored; case-sensitive)

TARGET_COLUMN = "Cover_Type"

Ingestion/validation/transformation directory names & filenames

SCHEMA_FILE_PATH = "data_schema/schema.yaml"

data_schema/schema.yaml (example structure):

columns:
  - col_a
  - col_b
  - ...
target_column: Cover_Type


The validator compares column names from columns: to the CSV headers.

🏃 DAG: What runs

data_ingestion

Streams Mongo collection → DataFrame (no noCursorTimeout; Atlas-friendly)

Writes feature store CSV

Stratified train/test split by TARGET_COLUMN

XCom: {"train_csv": ".../train.csv", "test_csv": ".../test.csv"}

data_validation

Verifies schema (expected columns present)

KS-test drift report (YAML)

Writes validated train/test CSVs

XCom: paths + validation_status

data_transformation

KNN imputer pipeline (or your richer preprocessor)

Writes NumPy arrays & preprocessor.pkl

XCom: transformed file paths

📦 Outputs (where to find files)

All artifacts are written under a timestamped folder:

Artifacts/<MM_DD_YYYY_HH_MM_SS>/
  ├─ data_ingestion/
  │   ├─ feature_store/...
  │   └─ ingested/{train.csv, test.csv}
  ├─ data_validation/
  │   ├─ validated/{train.csv, test.csv}
  │   └─ drift_report/report.yaml
  └─ data_transformation/
      ├─ transformed/{train.npy, test.npy}
      └─ transformed_object/preprocessor.pkl


On the host, that’s:
<your project root>/Artifacts/<timestamp>/...
Inside the container: /usr/local/airflow/Artifacts/<timestamp>/...

Exact paths are also visible in Airflow → Task → XCom and logs.

🔍 Useful Commands
# Start/stop
astro dev start --wait 180
astro dev stop

# Logs
astro dev logs --scheduler
astro dev logs --webserver
astro dev logs --api-server

# Shell into the running container (scheduler acts as worker in local mode)
astro dev bash scheduler

# List containers/process status
astro dev ps

🧯 Troubleshooting

Health check timeout on astro dev start

Try --wait 180

Check logs: astro dev logs --scheduler / --webserver / --api-server

Ensure Docker has enough CPU/RAM; free port 8080

Avoid spaces in project path on Windows

Mongo connection refused

If Airflow runs in Docker, localhost points to the container.
Use mongodb (service name) in Compose, host.docker.internal on Mac/Windows, or Atlas URI.

Atlas error: noTimeout cursors are disallowed

We don’t use no_cursor_timeout here; the ingestion uses a normal streaming cursor with batch_size.

Artifacts not visible on host

Check exact XCom paths.

astro dev bash scheduler → ls -lah /usr/local/airflow/Artifacts/...

If visible in container but not on host, it’s a mount issue (common with spaces in path).

🔐 Security

Keep secrets in .env; make sure .gitignore excludes:

.env
Artifacts/
.astro/
logs/
venv/ .venv/
__pycache__/ *.pyc
airflow.db airflow.cfg


If you accidentally committed secrets, rotate them immediately and consider purging history (git filter-repo).

🛠️ Development Notes

Heavy imports live inside task callables to keep DagBag parsing fast.

Tasks return JSON-serializable payloads only (paths/flags).

If you modify .env or Python code, restart:

astro dev stop && astro dev start --wait 180

🧪 Manual Run (from UI)

Open http://localhost:8080

Unpause ml_data_pipeline

Trigger DAG → watch Graph view

Inspect Logs and XCom to see artifact paths

If you want this README tailored with your exact schema.yaml sample or current Mongo collection/DB names, I can add a “Configuration Examples” section with concrete values.