from pyspark.sql.functions import when, col
from config import SILVER_PATH, FRAUD_PATH, HIGH_AMOUNT_THRESHOLD
from utils.helpers import ensure_dir


def run_fraud_detection(spark):
    print("Running Fraud Detection...")

    ensure_dir(FRAUD_PATH)

    df = spark.read.parquet(SILVER_PATH)

    flagged = df.withColumn(
        "high_amount_flag"
    )

    return flagged
