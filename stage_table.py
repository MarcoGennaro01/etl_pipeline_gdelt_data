import os
from dotenv import load_dotenv
from src.loading import create_data_wh, load_data_bq

if __name__ == "__main__":
    load_dotenv()

    STAGING_TABLE_NAME = os.getenv("STAGING_TABLE_NAME")
    WH_TABLE_NAME = os.getenv("WH_TABLE_NAME")
    DATASET_ID = os.getenv("BIG_QUERY_DATASET")
    BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")

    STAGING_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_NAME}"
    WH_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{WH_TABLE_NAME}"

    # load_data_bq(STAGING_TABLE_ID, BUCKET_NAME)
    create_data_wh(STAGING_TABLE_ID, WH_TABLE_ID, BUCKET_NAME)
