# GDELT Data Pipeline

> **TL;DR**: A GCP pipeline that ingests GDELT geopolitical events, moves them from Cloud Storage to BigQuery, and builds a weekly data mart to predict deteriorating media tone by country. The model is a distributed XGBoost classifier running on Spark/Dataproc.

## Stack and Flow

`GDELT events` -> `GCS` -> `BigQuery staging` -> `warehouse` -> `ML data mart` -> `Spark XGBoost`

- **Infrastructure**: Terraform, Cloud Storage, BigQuery, Dataproc.
- **Ingestion**: Daily GDELT `CSV.zip` files are converted to `gzip` and uploaded to `gs://<bucket>/data/`.
- **Processing**: BigQuery loads, types, and aggregates data by week and country.
- **Training**: A PySpark batch reads the data mart from BigQuery and writes metrics and a JSON model to Cloud Storage.

## Quick Start

Prerequisites: Python 3, Terraform >= 1.5, authenticated Google Cloud CLI access, and permissions to provision a GCP project and attach its billing account.

```powershell
python -m venv env
.\env\Scripts\Activate.ps1
pip install -r requirements.txt
gcloud auth application-default login
```

Provision the infrastructure. First create `infra/terraform.tfvars` from [`infra/terraform.tfvars.example`](infra/terraform.tfvars.example) and supply the project ID, billing account, region, bucket, dataset, and table names.

```powershell
cd infra
terraform init
terraform apply
cd ..
```

Create a `.env` file in the repository root:

```dotenv
GCP_PROJECT_ID=<project-id>
GCP_BUCKET_NAME=<bucket-name>
BIG_QUERY_DATASET=<dataset-id>
STAGING_TABLE_NAME=events_raw
WH_TABLE_NAME=events
DM_TABLE_NAME=events_ml
```

Ingest a GDELT date range, then build the warehouse and data mart:

```powershell
python create_data_lake.py --start 2024-01-01 --end 2024-01-31
python create_dw_dm.py
```

## Data

The staging layer retains the tab-delimited GDELT feed, including event, actor, geography, and source fields; all fields initially enter as `STRING`. The warehouse applies analysis-ready types: `DATE`, `FLOAT64` (`GoldsteinScale`, `AvgTone`), `INT64` (`Total_sources`), and country/class codes as `STRING`.

The data mart contains one row per week and country, with weighted tone and Goldstein-scale averages, source volume, proportions across the four `QuadClass` values, and five-week rolling statistics. It keeps countries with at least 60% temporal coverage. The binary `Target` is `1` when the following week's `WeightedAvgTone` declines.

## Model

[`notebooks/ml_boosting.py`](notebooks/ml_boosting.py) trains a `SparkXGBClassifier` with a temporal split: training before `2025-01-01`, testing from that date onward. Features exclude `Target`, `Country_code`, and `week`; training uses eight workers, `hist`, maximum depth 6, learning rate 0.1, subsample 0.8, and `scale_pos_weight` 1.61.

The job exports AUC-ROC, accuracy, and specificity to `gs://<bucket>/metrics/xgboost_metrics.json`, and stores the model at `gs://<bucket>/models/xgboost_model.json`. [`notebooks/ml.ipynb`](notebooks/ml.ipynb) contains the Dataproc submission command.
