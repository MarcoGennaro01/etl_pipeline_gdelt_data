import argparse
import os
from dotenv import load_dotenv
from src.extraction import extract_load_data

if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, required=True)
    parser.add_argument("--end", type=str, required=True)

    args = parser.parse_args()

    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

    if not BUCKET_NAME:
        raise ValueError("Variable BUCKE_NAME needs to be defined in .env file")

    extract_load_data(args.start, args.end, BUCKET_NAME)
