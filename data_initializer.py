from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace


class DataInitializer:

    def __init__(self, spark: SparkSession, data_path: str = "data"):
        self.spark = spark
        self.data_path = data_path

    def load_transactions(self) -> DataFrame:
        transactions_df = self.spark.read.csv(
            f"{self.data_path}/Transactions.csv",
            header=True,
            sep=";",
            inferSchema=True
        )

        rename_mapping = {
            "TRANSACTION_ID": "transaction_id",
            "GUARANTORS: ARRAY[JSON(NAME: STRING, PERCENTAGE: DECIMAL)]": "guarantors",
            "SENDER_NAME": "sender",
            "RECEIVER_NAME": "receiver",
            "TYPE: CLASS_A | CLASS_B | CLASS_C | CLASS_D": "type",
            "COUNTRY": "country",
            "CURRENCY": "currency",
            "AMOUNT: DECIMAL": "amount",
            "STATUS: PENDING | OPEN | CLOSED": "status",
            "DATE": "date",
            "PARTITION_DATE (PARTITIONING COLUMN)": "partition_date",
            "IS_VALID": "is_valid"
        }

        for old_name, new_name in rename_mapping.items():
            transactions_df = transactions_df.withColumnRenamed(old_name, new_name)

        transactions_df.createOrReplaceTempView("transactions")
        return transactions_df

    def load_exchange_rates(self) -> DataFrame:
        exchange_rates_df = self.spark.read.csv(
            f"{self.data_path}/Exchange Rates Registry.csv",
            header=True,
            sep=";",
            inferSchema=True
        )

        rename_mapping = {
            "FROM_CURRENCY": "from_currency",
            "TO_CURRENCY": "to_currency",
            "RATE: DECIMAL": "rate",
            "PARTITION_DATE (PARTITIONING COLUMN)": "partition_date",
            "ISVALID": "is_valid"
        }
        for old_name, new_name in rename_mapping.items():
            exchange_rates_df = exchange_rates_df.withColumnRenamed(old_name, new_name)

        exchange_rates_df = exchange_rates_df.withColumn("rate", regexp_replace(col("rate"), ",", ".").cast("float"))
        
        exchange_rates_df.createOrReplaceTempView("exchange_rates")
        return exchange_rates_df

    def load_receivers(self) -> DataFrame:
        receivers_df = self.spark.read.csv(
            f"{self.data_path}/Receivers Registry.csv",
            header=True,
            sep=";",
            inferSchema=True
        )

        rename_mapping = {
            "RECEIVER_NAME": "receiver_name",
            "COUNTRY": "country",
            "PARTITION_DATE (PARTITIONING COLUMN)": "partition_date",
            "ISVALID": "is_valid"
        }
        for old_name, new_name in rename_mapping.items():
            receivers_df = receivers_df.withColumnRenamed(old_name, new_name)

        receivers_df.createOrReplaceTempView("receivers_registry")
        return receivers_df

    def load_all_files(self):
        transactions_df = self.load_transactions()
        exchange_rates_df = self.load_exchange_rates()
        receivers_df = self.load_receivers()
        return transactions_df, exchange_rates_df, receivers_df
