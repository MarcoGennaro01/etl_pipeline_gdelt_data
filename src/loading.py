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
    print("Loading data in BigQuery...")
    load_job = client.load_table_from_uri(uris, table_id, job_config=job_config)
    print("Data successfully loaded")
    load_job.result()


def create_data_wh(staging_table_id, wh_table_id, bucket_name):
    client = bigquery.Client()
    query = f"""
       CREATE OR REPLACE TABLE `{wh_table_id}`
       AS SELECT
       PARSE_DATE("%Y%m%d",DATE) as date,
       round(CAST(GoldsteinScale AS FLOAT64),4) as GoldsteinScale,
       CAST(NumSources AS INT64) as Total_sources,
       CAST(AvgTone as FLOAT64) as AvgTone,
       CAST(ActionGeo_CountryCode as STRING) as Country_code,
       CAST(QuadClass as STRING) as QuadClass
       FROM `{staging_table_id}`
       WHERE PARSE_DATE("%Y%m%d", DATE) >= '2015-01-01'
       ORDER BY date;
    """
    print("Creating events table")
    res = client.query(query)
    print("Events table successfully created")
    res.result()


def create_ml_data_mart(dw_table_id, data_mart_table_id):
    client = bigquery.Client()
    query = f"""
        CREATE OR REPLACE TABLE `{data_mart_table_id}` AS

        WITH

        WEEKS AS (
            SELECT DISTINCT
                DATE_TRUNC(date, ISOWEEK) AS week
            FROM `{dw_table_id}`
            WHERE date IS NOT NULL
        ),

        TOTAL_WEEKS AS (
            SELECT COUNT(*) AS total_weeks FROM WEEKS
        ),

        COUNTRY_COVERAGE AS (
            SELECT
                Country_code,
                COUNT(DISTINCT DATE_TRUNC(date, ISOWEEK)) AS covered_week
            FROM `{dw_table_id}`
            WHERE Country_code IS NOT NULL AND date IS NOT NULL
            GROUP BY Country_code
        ),

        COUNTRIES AS (
            SELECT cc.Country_code
            FROM COUNTRY_COVERAGE cc
            CROSS JOIN TOTAL_WEEKS tw
            WHERE (cc.covered_week / tw.total_weeks) >= 0.60
        ),

        GRID AS (
            SELECT
                w.week,
                c.Country_code
            FROM WEEKS w
            CROSS JOIN COUNTRIES c
        ),

        GENERAL_METRICS AS (
            SELECT
                DATE_TRUNC(date, ISOWEEK) AS week,
                Country_code,
                SAFE_DIVIDE(SUM(GoldsteinScale * Total_sources), SUM(Total_sources)) AS WeightedGoldsteinScale,
                SAFE_DIVIDE(SUM(AvgTone * Total_sources), SUM(Total_sources)) AS WeightedAvgTone,
                SUM(Total_sources) AS TotalSources
            FROM `{dw_table_id}`
            WHERE Country_code IS NOT NULL
            GROUP BY 1, 2
        ),

        PIVOTED_EVENTS AS (
            SELECT * FROM (
                SELECT
                    DATE_TRUNC(date, ISOWEEK) AS week,
                    Country_code,
                    CAST(QuadClass AS STRING) AS QuadClass,
                    Total_sources
                FROM `{dw_table_id}`
                WHERE Country_code IS NOT NULL AND QuadClass IS NOT NULL
            )
            PIVOT(
                SUM(Total_sources) AS sources
                FOR QuadClass IN ('1', '2', '3', '4')
            )
        ),
        
        PROCESSED_DATA AS (
        SELECT
            g.week,
            g.Country_code,
            ROUND(m.WeightedGoldsteinScale,4)WeightedGoldsteinScale,
            ROUND(m.WeightedAvgTone,4) as WeightedAvgTone,
            COALESCE(m.TotalSources, 0) AS TotalSources,
            COALESCE(p.sources_1, 0) AS sources_verbal_cooperation,
            COALESCE(p.sources_2, 0) AS sources_material_cooperation,
            COALESCE(p.sources_3, 0) AS sources_verbal_conflict,
            COALESCE(p.sources_4, 0) AS sources_material_conflict,
            ROUND(SAFE_DIVIDE(COALESCE(p.sources_1, 0), m.TotalSources),4) AS pct_verbal_cooperation,
            ROUND(SAFE_DIVIDE(COALESCE(p.sources_2, 0), m.TotalSources),4) AS pct_material_cooperation,
            ROUND(SAFE_DIVIDE(COALESCE(p.sources_3, 0), m.TotalSources),4) AS pct_verbal_conflict,
            ROUND(SAFE_DIVIDE(COALESCE(p.sources_4, 0), m.TotalSources),4) AS pct_material_conflict
        FROM GRID g
        LEFT JOIN GENERAL_METRICS m
            ON g.week = m.week AND g.Country_code = m.Country_code
        LEFT JOIN PIVOTED_EVENTS p
            ON g.week = p.week AND g.Country_code = p.Country_code
        ORDER BY g.Country_code, g.week
        )

        SELECT
        *,
        LAG(WeightedAvgTone,1) OVER (PARTITION by Country_code ORDER BY week) as Prev1_WeightedAvgTone,
        LAG(WeightedAvgTone,2) OVER (PARTITION by Country_code ORDER BY week) as Prev2_WeightedAvgTone,
        LAG(WeightedGoldsteinScale,1) OVER (PARTITION by Country_code ORDER BY week) as Prev1_WeightedGoldsteinScale,
        LAG(WeightedGoldsteinScale,2) OVER (PARTITION by Country_code ORDER BY week) as Prev2_WeightedGoldsteinScale,
        LEAD(WeightedAvgTone,1) OVER (PARTITION BY Country_code ORDER BY week) as Target_AvgTone
        FROM PROCESSED_DATA
        ORDER BY Country_code, week;
    """
    print("Creating events_ml table")
    job = client.query(query)
    print("Data mart successfully created")
    job.result()
