import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
project_id = os.getenv("GCP_PROJECT_ID")
print(project_id)

client = bigquery.Client()

sql = """
SELECT 
    PARSE_DATE('%Y%m%d', CAST(DIV(DATE,10000) AS STRING)) AS date,
    SPLIT(V2Locations)
"""
