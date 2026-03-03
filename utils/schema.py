from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType
)


def transaction_schema():
    return StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True)
    ])
