"""
report_save.py

Extracts electricity usage data from a MySQL source, processes it, and appends it to a Delta table in Databricks.
"""

import logging
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col, when
import mysql.connector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ElectricityUsage-ETL")

# MySQL Database Configuration (Secure this in prod)
DB_CONFIG = {
    'host': '<MYSQL_HOST>',
    'user': '<MYSQL_USER>',
    'password': '<MYSQL_PASSWORD>',
    'database': '<MYSQL_DB>'
}

# Databricks table to append data
DATABRICKS_TABLE = "poc_workspace.log_data.electricity_usage_log_th"

# SQL Query to extract cabinet electricity usage
CUSTOM_QUERY = """
WITH cabinet_info AS (
    SELECT c.name AS cabinet_name, cl.cabinet_code, cl.latest_time, cl.ammeter_numerical
    FROM bsa_th.cabinet_latest cl
    LEFT JOIN cabinet c ON cl.cabinet_code = c.code
),
merchant_info AS (
    SELECT ms.name AS merchant_site_name, cmsr.cabinet_code
    FROM merchant_site ms
    JOIN cabinet_merchant_site_rela cmsr ON ms.sole_code = cmsr.site_code
)
SELECT ci.cabinet_name, mi.merchant_site_name, 
       ci.cabinet_code, ci.latest_time, ci.ammeter_numerical
FROM cabinet_info ci
LEFT JOIN merchant_info mi ON ci.cabinet_code = mi.cabinet_code
"""

def run_schedule_job():
    """
    Runs the ETL job: extracts data from MySQL, transforms it, and appends to Databricks Delta table.
    """
    try:
        logger.info("Connecting to MySQL...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(CUSTOM_QUERY)
        records = cursor.fetchall()
        cursor.close()
        conn.close()

        if not records:
            logger.warning("No data fetched.")
            return False

        df = pd.DataFrame(records)

        # Initialize Spark
        spark = SparkSession.builder \
            .appName("ElectricityUsageETL") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .getOrCreate()

        spark_df = spark.createDataFrame(df)

        spark_df = spark_df.selectExpr(
            "cabinet_name",
            "merchant_site_name",
            "cabinet_code",
            "latest_time",
            "CAST(ammeter_numerical AS DECIMAL(38,9)) AS ammeter_numerical"
        ).withColumn(
            "ammeter_numerical",
            when(col("ammeter_numerical") == 0, lit("0.000000000").cast("decimal(38,9)"))
            .otherwise(col("ammeter_numerical"))
        ).withColumn("category", lit(None).cast("string")) \
         .withColumn("datetime_recorded", current_timestamp().cast("timestamp_ntz"))

        logger.info("Appending to Databricks Delta table...")
        spark_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(DATABRICKS_TABLE)

        logger.info("Data appended successfully.")
        return True

    except Exception as e:
        logger.error(f"ETL failed: {e}")
        return False

if __name__ == "__main__":
    success = run_schedule_job()
    exit(0 if success else 1)
