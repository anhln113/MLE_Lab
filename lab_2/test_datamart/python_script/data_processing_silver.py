
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

def process_silver_table(snapshot_date_str, bronze_directory,silver_directory, spark):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # Connect to bronze table
    csv_file_path = "bronze_loan_daily_" + snapshot_date_str.replace("-", "_") + ".csv"

    # Load data
    df = spark.read.csv(bronze_directory + csv_file_path, header=True, inferSchema=True)

    # Create a dictionary specifying columns & their datatypes
    column_type_map = {
        "loan_id" : StringType(),
        "customer_id" : StringType(),
        "loan_start_date" : DateType(),
        "tenure" : IntegerType(),
        "installment_num" : IntegerType(),
        "loan_amt" : FloatType(),
        "due_amt" : FloatType(),
        "paid_amt" : FloatType(),
        "overdue_amt" : FloatType(),
        "balance" : FloatType(),
        "snapshot_date" : DateType()
    }

    # Enforce schema
    for column_name, data_type in column_type_map.items():
        df = df.withColumn(column_name, col(column_name).cast(data_type))

    # Create Month On Book (mob) column:
    df = df.withColumn("mob", col("installment_num").cast(IntegerType()))

    # Create Day pass due (dpd) column:
    # Calculate the number of missed installment: 
    df = df.withColumn("missed_installments", \
                       F.ceil(col("overdue_amt") / col("due_amt")).cast(IntegerType())).fillna(0)

    # Compute the first missed date:
    df = df.withColumn("first_missed_date", \
                       F.when(col("missed_installments") > 0, \
                              F.add_months(col("snapshot_date"), -1 * col("missed_installments")).cast(DateType())))

    # Compute day past due (dpd) as the difference between snapshot date and first missed date:
    df = df.withColumn("dpd", \
                       F.when(col("overdue_amt") > 0.0, \
                              F.datediff(col("snapshot_date"), col("first_missed_date"))) \
                       .otherwise(0).cast(IntegerType()))

    # Save silver table to datamart
    partition_name = "silver_loan_daily_" + snapshot_date_str.replace("-", "_") + ".parquet"
    file_path = silver_directory + partition_name
    df.write.mode("overwrite").parquet(file_path)
    print("Saved to: ", file_path)

    return df