import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

def process_gold_table(snapshot_date_str, silver_directory, gold_directory, spark, dpd, mob):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # Connect to silver datalake:
    partition_name = "silver_loan_daily_" + snapshot_date_str.replace("-", "_") + ".parquet"
    file_path = silver_directory + partition_name

    # Read parquet file:
    df = spark.read.parquet(file_path)

    # Filter to include only loans at mob
    df = df.filter(col("mob") == mob)

    # Create label & label definition columns
    df = df.withColumn("label", F.when(col("dpd") >= dpd, 1).otherwise(0).cast(IntegerType()))
    df = df.withColumn("label_def", F.lit(str(dpd) + "dpd_" + str(mob)+ "mob").cast(StringType()))

    # Select columns to save
    df = df.select("loan_id", "customer_id", "label", "label_def", "snapshot_date")

    # Save files to datalake
    partition_name = "gold_label_" + snapshot_date_str.replace("-", "_") + ".parquet"
    file_path = gold_directory + partition_name
    df.write.mode("overwrite").parquet(file_path)
    print("Saved to: ", file_path)

    return df 

