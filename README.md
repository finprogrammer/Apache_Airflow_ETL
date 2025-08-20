This repo contains a lightweight Airflow 2 pipeline (run locally with Astronomer/Docker) that orchestrates data ingestion → validation → transformation. It streams data from MongoDB to CSV, checks schema and data drift, then preprocesses and saves transformed arrays and a preprocessor object. All outputs are written to timestamped folders under Artifacts/, and small JSON (paths/flags) is passed between tasks via XCom.

![alt text](dag_final-1.png)

## Prerequisites
- Docker Desktop (WSL2 on Windows recommended)
- Astronomer CLI (`astro`)
- (Optional) A MongoDB instance (Atlas or local)

## .env example
Create a `.env` at the project root:
```env
MONGODB_URL_KEY=mongodb+srv://USER:PASS@cluster.mongodb.net/DB?retryWrites=true&w=majority
# or local/compose:
# MONGODB_URL_KEY=mongodb://mongodb:27017/DB
.
├─ dags/
│  └─ etlpipeline.py                # Airflow DAG: ingestion → validation → transformation
├─ networksecurity/
│  ├─ components/
│  │  ├─ data_ingestion.py
│  │  ├─ data_validation.py
│  │  └─ data_transformation.py
│  ├─ constant/
│  │  └─ training_pipeline.py       # constants (paths, names, ratios)
│  ├─ entity/                       # config & artifact dataclasses
│  ├─ exception/                    # NetworkSecurityException
│  └─ utils/                        # YAML/IO helpers
├─ data_schema/
│  └─ schema.yaml                   # expected columns, target_column
├─ .env                             # MONGODB_URL_KEY=...
├─ Dockerfile
├─ requirements.txt
└─ README.md