from pyspark.sql import Row, types as T

from turbine_transforms import bronze_enrich


def test_bronze_enrich(spark):
    schema = T.StructType([
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("turbine_id", T.StringType(), True),
        T.StructField("wind_speed", T.StringType(), True),
        T.StructField("wind_direction", T.StringType(), True),
        T.StructField("power_output", T.StringType(), True),
        T.StructField(
            "_metadata",
            T.StructType([
                T.StructField("file_path", T.StringType(), True)
            ]),
            True
        ),
        T.StructField("_rescued_data", T.StringType(), True),
    ])

    df = spark.createDataFrame([
        ("2026-03-10 12:00:00", "T1", "12.3", "180", "2.5", Row(file_path="/tmp/file1.csv"), None),
    ], schema=schema)

    result = bronze_enrich(df)

    row = result.select("_source_file", "_ingest_ts", "_ingest_date").collect()[0]

    assert row["_source_file"] == "/tmp/file1.csv"
    assert row["_ingest_ts"] is not None
    assert row["_ingest_date"] is not None