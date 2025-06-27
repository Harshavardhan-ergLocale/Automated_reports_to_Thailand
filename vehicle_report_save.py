"""
report_save.py

Extracts B2C battery swap and mileage data from MySQL,
aggregates the distance and swap count, and saves to DBFS and local path.

Requirements:
- pymysql
- pandas
- pyspark
- logging
- os
"""

import pymysql
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

# Spark session
spark = SparkSession.builder.appName("B2C_3_Bikes_Report").getOrCreate()

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("B2C_3_Bikes_Report")

# DB Config (fill securely before use)
DB_CONFIG = {
    'host': 'your-db-host',
    'user': 'your-db-user',
    'password': 'your-db-password',
    'database': 'your-db-name'
}

# Dynamic Date Extraction for Start and End_dates in the Query
def get_date_range():
    today = datetime.now()
    end_date = today - timedelta(days=1)
    start_date = end_date - relativedelta(months=1) + timedelta(days=1)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# Core Distance Logic
# Constants
KWH_PER_100_CHARGE = 1.6
KM_PER_KWH = 40

def calculate_distance(take_soc, return_soc):
    used_kwh = ((take_soc - return_soc) / 100) * KWH_PER_100_CHARGE
    return round(used_kwh * KM_PER_KWH, 2)


def execute_and_transform_query(start_date, end_date, query):
    query = query.format(start_date=start_date, end_date=end_date)
    connection = pymysql.connect(**DB_CONFIG)
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)

        if df.empty:
            logger.warning("No data found.")
            return None

        df["return_soc"] = df["return_soc"].astype(float)  # Extract return_soc from the query to df column
        df["take_soc"] = df["take_soc"].astype(float)  # Extract take_soc from query to df column
        df["total_distance_km"] = df.apply(lambda r: calculate_distance(r["take_soc"], r["return_soc"]), axis=1) # Caluclate the distance using the above core distance Logic

        report_df = df.groupby(["date", "vehicle_code"]).agg(
            total_distance_km=pd.NamedAgg(column="total_distance_km", aggfunc="sum"),
            battery_swap_count=pd.NamedAgg(column="vehicle_code", aggfunc="count")
        ).reset_index()

        spark_df = spark.createDataFrame(report_df)
        output_dir = f"dbfs:/FileStore/reports/B2C/B2C_3_Bikes_Report_{start_date}_to_{end_date}"
        spark_df.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")
        logger.info(f"Report saved to DBFS: {output_dir}")
        return output_dir

    finally:
        connection.close()


def download_and_save_locally(dbfs_path, local_filename):
    try:
        spark_df = spark.read.option("header", "true").csv(dbfs_path)
        local_dir = os.path.expanduser("~/shared_reports")
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, local_filename)
        spark_df.toPandas().to_csv(local_path, index=False)
        logger.info(f"Saved to: {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed saving locally: {e}")
        return None


def main():
    start_date, end_date = get_date_range()
    query = """YOUR SQL QUERY HERE"""
    dbfs_path = execute_and_transform_query(start_date, end_date, query)
    if dbfs_path:
        download_and_save_locally(dbfs_path, f"B2C_3_Bikes_Report_{start_date}_to_{end_date}.csv")


if __name__ == "__main__":
    main()
