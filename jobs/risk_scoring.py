from pyspark.sql.functions import when, col
from config import FRAUD_PATH, SILVER_PATH
from config import FRAUD_RISK_THRESHOLD


def run_risk_scoring(spark):
    print("Running Risk Scoring...")

    df = spark.read.parquet(FRAUD_PATH)

    scored = df.withColumn(
        "risk_score",
        when(col("high_amount_flag") == 1, 3)
        .when(col("transaction_type") == "TRANSFER", 2)
        .otherwise(1)
    )

    scored.write.mode("overwrite").parquet(SILVER_PATH + "risk_scored/")

    return scored
