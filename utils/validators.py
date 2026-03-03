from pyspark.sql.functions import col


def validate_transactions(df):
    return df.filter(
        (col("amount") > 0) &
        col("account_id").isNotNull() &
        col("customer_id").isNotNull()
    )
