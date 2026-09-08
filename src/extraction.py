import os
<<<<<<< HEAD
import zipfile

from gzip import GzipFile
from io import BytesIO
from google.cloud import storage
from urllib.request import urlopen
from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor


def get_dates_between(start_date, end_date):
    """
    Gets two dates in the format "YYYY-m-d"
    and returns a list with date class objects of days between the two dates
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    return [
        (start_date + timedelta(days=i))
        for i in range((end_date - start_date).days + 1)
    ]


def extract_single_date(date_obj, BUCKET_NAME):
    file_name = "{}.export.CSV.zip".format(date_obj.strftime("%Y%m%d"))
    dw_link = f"https://data.gdeltproject.org/events/{file_name}"
    try:
        with urlopen(dw_link) as data:
            data = BytesIO(data.read())
        with zipfile.ZipFile(data) as z:
            data = z.read(z.namelist()[0])
        gz = BytesIO()
        with GzipFile(fileobj=gz, mode="wb") as gz_out:
            gz_out.write(data)
        gz.seek(0)
        file_name = file_name.replace("zip", "gz")
        upload_to_bucket(gz, file_name, BUCKET_NAME)
    except Exception as e:
        print(e)


def extract_load_data(start_date, end_date, BUCKET_NAME, max_workers=8):
    try:
        dates = get_dates_between(start_date, end_date)
        with ThreadPoolExecutor(max_workers) as mt:
            mt.map(lambda date: extract_single_date(date, BUCKET_NAME), dates)
    except Exception as e:
        print(e)


def upload_to_bucket(gz_buffer, filename, BUCKET_NAME):
    destination = f"data/{filename}"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination)
    blob.upload_from_file(gz_buffer, content_type="application/gzip")
=======
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
>>>>>>> b46710e388055767a325bffed16c31f837d5fa98
