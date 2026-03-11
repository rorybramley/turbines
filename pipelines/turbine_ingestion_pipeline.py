from pyspark import pipelines as dp
from pyspark.sql import functions as F, types as T
from src.turbine_transforms import bronze_enrich, silver_transform, gold_summary_transform, gold_anomalies_transform, RAW_PATH

@dp.table(
    name="bronze_turbine_readings",
    comment="Raw turbine readings ingested from CSV via Auto Loader"
)
def bronze_turbine_readings():
    raw = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(RAW_PATH))
    return bronze_enrich(raw)


@dp.table(
    name="silver_turbine_readings",
    comment="Cleaned and deduplicated turbine readings"
)
@dp.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dp.expect_or_drop("valid_turbine_id", "turbine_id IS NOT NULL")
@dp.expect_or_drop("valid_wind_speed", "wind_speed IS NOT NULL AND wind_speed >= 0")
@dp.expect_or_drop("valid_wind_direction", "wind_direction IS NOT NULL AND wind_direction >= 0 and wind_direction < 360")
@dp.expect_or_drop("valid_power_output", "power_output IS NOT NULL AND power_output >= 0")
def silver_turbine_readings():
    bronze = spark.readStream.table("bronze_turbine_readings")
    return silver_transform(bronze)


@dp.materialized_view(
    name="gold_turbine_24h_summary",
    comment="24h min/max/avg/stddev power metrics by turbine"
)
def gold_turbine_24h_summary():
    silver = spark.read.table("silver_turbine_readings")
    return gold_summary_transform(silver)


@dp.materialized_view(
    name="gold_turbine_24h_anomalies",
    comment="Readings outside mean ± 2 stddev for each turbine over 24h"
)
def gold_turbine_24h_anomalies():
    silver = spark.read.table("silver_turbine_readings")
    summary = spark.read.table("gold_turbine_24h_summary")
    return gold_anomalies_transform(silver, summary)
    
