# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = (
    SparkSession.builder
    .appName("P02-T02-GroupBy-Smoke")
    .config("spark.executor.instances", "2")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
sc = spark.sparkContext

df = (
    spark.read.option("sep", "\t")
    .csv(input_path)
    .toDF("grp_key", "val_str")
    .withColumn("val_num", F.col("val_str").cast("long"))
)

agg_df = (
    df.groupBy("grp_key")
    .agg(
        F.count("*").alias("row_cnt"),
        F.sum("val_num").alias("val_sum"),
    )
    .orderBy("grp_key")
)

print("=== APPLICATION ID ===", sc.applicationId)
agg_df.show(truncate=False)
agg_df.write.mode("overwrite").parquet(output_path)

spark.stop()
