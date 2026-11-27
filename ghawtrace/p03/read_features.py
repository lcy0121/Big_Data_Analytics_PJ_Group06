# -*- coding: utf-8 -*-
"""Read published P03 Parquet tables using the classroom Spark 2 SQL types."""
from __future__ import print_function

import sys


TIMESTAMPS = {
    "workflow_profile": ("first_seen_utc", "last_seen_utc"),
    "workflow_event_mix": (),
    "workflow_week": (),
    "workflow_asof": ("cutoff_utc", "window_start_utc", "window_end_utc"),
}


def read_features(spark, batch, name):
    """Load a published table; Spark 2 exposes Parquet TIMESTAMP_MICROS as bigint."""
    if name not in TIMESTAMPS:
        raise ValueError("Unknown P03 table: " + name)
    table = spark.read.parquet(batch.rstrip("/") + "/" + name + ".parquet")
    types = dict(table.dtypes)
    for column in TIMESTAMPS[name]:
        if types[column] == "bigint":
            # spark-submit --py-files ghawtrace/p02/clean_runs.py
            from clean_runs import _micros_to_timestamp_col
            table = table.withColumn(column, _micros_to_timestamp_col(column))
        elif types[column] != "timestamp":
            raise ValueError("Unexpected type for " + column + ": " + types[column])
    return table


def main():
    import json
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    if len(sys.argv) != 2:
        raise SystemExit("usage: read_features.py P03_PARQUET_BATCH")
    spark = SparkSession.builder.appName("GHAWTrace P03 table readback").getOrCreate()
    try:
        for name, columns in sorted(TIMESTAMPS.items()):
            table = read_features(spark, sys.argv[1], name)
            stats = [F.count("*").alias("rows")]
            for column in columns:
                stats.extend([F.min(column).cast("string").alias(column + "_min"),
                              F.max(column).cast("string").alias(column + "_max")])
            print("P03_TABLE", name, json.dumps(table.agg(*stats).first().asDict(), sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
