from pyspark.sql import DataFrame, functions as F, types as T

RAW_PATH = "/Volumes/workspace/colibri/turbine_readings"


def bronze_enrich(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_ingest_date", F.to_date(F.current_timestamp()))
    )


def silver_transform(df: DataFrame) -> DataFrame:
    recast = (
        df.withColumn("event_time", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("event_date", F.to_date("timestamp"))
          .select(
              F.col("event_time"),
              F.col("event_date"),
              F.col("turbine_id").cast("string"),
              F.col("wind_speed").cast("double"),
              F.col("wind_direction").cast("double"),
              F.col("power_output").cast("double"),
              F.col("_source_file"),
              F.col("_ingest_ts"),
              F.col("_ingest_date"),
              F.col("_rescued_data")
          )
    )

    return (
        recast.withWatermark("event_time", "3 days")
              .dropDuplicates([
                  "event_time",
                  "event_date",
                  "turbine_id",
                  "wind_speed",
                  "wind_direction",
                  "power_output"
              ])
    )


def gold_summary_transform(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(
            "turbine_id",
            F.window("event_time", "24 hours").alias("time_window")
        )
        .agg(
            F.min("power_output").alias("min_power"),
            F.max("power_output").alias("max_power"),
            F.avg("power_output").alias("avg_power"),
            F.stddev("power_output").alias("stddev_power"),
            F.count("*").alias("total_readings"),
        )
        .select(
            "turbine_id",
            F.to_date(F.col("time_window.start")).alias("window_start"),
            F.to_date(F.col("time_window.end")).alias("window_end"),
            "min_power",
            "max_power",
            F.col("avg_power").cast(T.DecimalType(3, 2)).alias("avg_power"),
            F.col("stddev_power").cast(T.DecimalType(3, 2)).alias("stddev_power"),
            "total_readings"
        )
    )


def gold_anomalies_transform(silver_df: DataFrame, summary_df: DataFrame) -> DataFrame:
    anomalies = (
        silver_df.withColumn("window_start", F.col("event_date"))
                 .withColumn("window_end", F.date_add("event_date", 1))
                 .join(
                     summary_df,
                     on=["turbine_id", "window_start", "window_end"],
                     how="inner"
                 )
                 .withColumn(
                     "is_anomaly",
                     F.abs(F.col("power_output") - F.col("avg_power")) > 2 * F.col("stddev_power")
                 )
    )

    return (
        anomalies.select(
            "turbine_id",
            "window_start",
            "window_end",
            "event_time",
            "wind_speed",
            "wind_direction",
            "power_output",
            "min_power",
            "max_power",
            "avg_power",
            "stddev_power",
            "is_anomaly"
        )
        .where(F.col("is_anomaly"))
    )