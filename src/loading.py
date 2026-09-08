from google.cloud import bigquery


def load_data_bq(table_id, bucket_name):
    client = bigquery.Client()
    uris = f"gs://{bucket_name}/data/*.gz"
    schema = [
        bigquery.SchemaField("GLOBALEVENTID", "STRING"),
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("MonthYear", "STRING"),
        bigquery.SchemaField("Year", "STRING"),
        bigquery.SchemaField("FractionDate", "STRING"),
        bigquery.SchemaField("Actor1Code", "STRING"),
        bigquery.SchemaField("Actor1Name", "STRING"),
        bigquery.SchemaField("Actor1CountryCode", "STRING"),
        bigquery.SchemaField("Actor1KnownGroupCode", "STRING"),
        bigquery.SchemaField("Actor1EthnicCode", "STRING"),
        bigquery.SchemaField("Actor1Religion1Code", "STRING"),
        bigquery.SchemaField("Actor1Religion2Code", "STRING"),
        bigquery.SchemaField("Actor1Type1Code", "STRING"),
        bigquery.SchemaField("Actor1Type2Code", "STRING"),
        bigquery.SchemaField("Actor1Type3Code", "STRING"),
        bigquery.SchemaField("Actor2Code", "STRING"),
        bigquery.SchemaField("Actor2Name", "STRING"),
        bigquery.SchemaField("Actor2CountryCode", "STRING"),
        bigquery.SchemaField("Actor2KnownGroupCode", "STRING"),
        bigquery.SchemaField("Actor2EthnicCode", "STRING"),
        bigquery.SchemaField("Actor2Religion1Code", "STRING"),
        bigquery.SchemaField("Actor2Religion2Code", "STRING"),
        bigquery.SchemaField("Actor2Type1Code", "STRING"),
        bigquery.SchemaField("Actor2Type2Code", "STRING"),
        bigquery.SchemaField("Actor2Type3Code", "STRING"),
        bigquery.SchemaField("IsRootEvent", "STRING"),
        bigquery.SchemaField("EventCode", "STRING"),
        bigquery.SchemaField("EventBaseCode", "STRING"),
        bigquery.SchemaField("EventRootCode", "STRING"),
        bigquery.SchemaField("QuadClass", "STRING"),
        bigquery.SchemaField("GoldsteinScale", "STRING"),
        bigquery.SchemaField("NumMentions", "STRING"),
        bigquery.SchemaField("NumSources", "STRING"),
        bigquery.SchemaField("NumArticles", "STRING"),
        bigquery.SchemaField("AvgTone", "STRING"),
        bigquery.SchemaField("Actor1Geo_Type", "STRING"),
        bigquery.SchemaField("Actor1Geo_FullName", "STRING"),
        bigquery.SchemaField("Actor1Geo_CountryCode", "STRING"),
        bigquery.SchemaField("Actor1Geo_ADM1Code", "STRING"),
        bigquery.SchemaField("Actor1Geo_Lat", "STRING"),
        bigquery.SchemaField("Actor1Geo_Long", "STRING"),
        bigquery.SchemaField("Actor1Geo_FeatureID", "STRING"),
        bigquery.SchemaField("Actor2Geo_Type", "STRING"),
        bigquery.SchemaField("Actor2Geo_FullName", "STRING"),
        bigquery.SchemaField("Actor2Geo_CountryCode", "STRING"),
        bigquery.SchemaField("Actor2Geo_ADM1Code", "STRING"),
        bigquery.SchemaField("Actor2Geo_Lat", "STRING"),
        bigquery.SchemaField("Actor2Geo_Long", "STRING"),
        bigquery.SchemaField("Actor2Geo_FeatureID", "STRING"),
        bigquery.SchemaField("ActionGeo_Type", "STRING"),
        bigquery.SchemaField("ActionGeo_FullName", "STRING"),
        bigquery.SchemaField("ActionGeo_CountryCode", "STRING"),
        bigquery.SchemaField("ActionGeo_ADM1Code", "STRING"),
        bigquery.SchemaField("ActionGeo_Lat", "STRING"),
        bigquery.SchemaField("ActionGeo_Long", "STRING"),
        bigquery.SchemaField("ActionGeo_FeatureID", "STRING"),
        bigquery.SchemaField("DATEADDED", "STRING"),
        bigquery.SchemaField("SOURCEURL", "STRING"),
    ]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        field_delimiter="\t",
    )
    load_job = client.load_table_from_uri(uris, table_id, job_config=job_config)
    load_job.result()


def create_data_wh(staging_table_id, wh_table_id, bucket_name):
    client = bigquery.Client()
    query = f"""
       CREATE OR REPLACE TABLE `{wh_table_id}`
       AS SELECT
       PARSE_DATE("%Y%m%d",DATE) as date,
       SUBSTRING(EventCode,1,2) as EventCode,
       round(CAST(GoldsteinScale AS FLOAT64),4) as GoldsteinScale,
       CAST(NumMentions AS INT64) as Total_mentions,
       CAST(AvgTone as FLOAT64) as AvgTone,
       CAST(Actor1Geo_CountryCode as STRING) as Country_code
       FROM `{staging_table_id}`
       ORDER BY date;
    """
    res = client.query(query)
    res.result()


def create_ml_data_mart(dw_table_id, data_mart_table_id):
    client = bigquery.Client()

    query = f"""
        CREATE OR REPLACE TABLE `{data_mart_table_id}`
        AS SELECT
        DATE_TRUNC(date,`week`) as week,
        SAFE_DIVIDE(SUM(GoldsteinScale * Total_mentions),sum(Total_mentions)) as WeightedGoldsteinScale,
        SAFE_DIVIDE(SUM(AvgTone * Total_mentions),sum(Total_mentions)) as WeightedAvgTone,
        SUM(Total_mentions) as TotalMentions,
        Country_code
        FROM `{dw_table_id}`
        GROUP BY week, Country_code
        ORDER BY week, Country_code;
        """
    job = client.query(query)
    job.result()
