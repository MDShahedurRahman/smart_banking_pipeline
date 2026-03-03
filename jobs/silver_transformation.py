from pyspark.sql.functions import col, to_date
from config import BRONZE_PATH, SILVER_PATH
from utils.helpers import ensure_dir
from utils.validators import validate_transactions


def run_silver_job(spark):
    print("Running Silver Transformation...")

    ensure_dir(SILVER_PATH)

    df = spark.read.parquet(BRONZE_PATH)

    cleaned = (
        validate_transactions(df)
    )

    return cleaned
