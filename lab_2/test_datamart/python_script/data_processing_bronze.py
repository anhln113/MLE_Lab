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


def process_bronze_table(snapshot_date_str, bronze_directory, spark):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # Connect to source data (IRL: backend source system)
    csv_file_path = "data/lms_loan_daily.csv"

    # Load data and filter by snapshot date
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True) \
        .filter(col("snapshot_date") == snapshot_date)
        
    print(snapshot_date_str + "Number of records: ", df.count())

    # Save bronze table to datamart (IRL: connect to database)
    partition_name = "bronze_loan_daily_" + snapshot_date_str.replace("-","_") + ".csv"
    save_path = bronze_directory + partition_name
    df.toPandas().to_csv(save_path, index=False)
    print("Saved to: " + save_path)

    return df

