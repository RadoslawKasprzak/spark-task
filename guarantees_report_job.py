from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, sum as _sum, avg, explode, from_json,
    lit, current_date, date_sub, max as _max, regexp_replace
)
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import udf
import json
import re


class GuaranteesReportJob:
    def __init__(self, spark: SparkSession, report_date: str):
        self.spark = spark
        self.report_date = report_date

    def get_latest_partition(self, df: DataFrame) -> DataFrame:
        max_date = df.agg(_max("partition_date").alias("max_date")).collect()[0]["max_date"]
        return df.filter(col("partition_date") == max_date)

    def filter_transactions(self, transactions_df: DataFrame) -> DataFrame:
        # usuwanie dyplikatow
        transactions_df = transactions_df.dropDuplicates(["transaction_id"])
        # Only transactions within last 30 days OR with status OPEN OR with is_valid IS NULL (is_valid technical column
        # indicating if row is valid or not (valid row = null value in is_valid)
        transactions_df = transactions_df.filter(
            ((col("status") == "OPEN") | (col("date") >= date_sub(current_date(), 30))) &
            (col("is_valid").isNull())
        )
        return transactions_df

    def join_with_receivers(self, transactions_df: DataFrame, receivers_df: DataFrame) -> DataFrame:
        
        latest_receivers = self.get_latest_partition(receivers_df)

        transactions_alias = transactions_df.alias("t")
        receivers_alias = latest_receivers.alias("r")

        joined_df = transactions_alias.join(
            receivers_alias,
            col("t.receiver") == col("r.receiver_name"),
            "inner"
        )
        joined_df = joined_df.select("t.*")
        return joined_df

    def join_with_exchange_rates(self, transactions_df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:

        latest_exchange_rates = self.get_latest_partition(exchange_rates_df)
        latest_exchange_rates = latest_exchange_rates.filter(col("to_currency") == "EUR")

        transactions_alias = transactions_df.alias("t")
        exchange_rates_alias = latest_exchange_rates.alias("e")

        joined_df = transactions_alias.join(
            exchange_rates_alias,
            col("t.currency") == col("e.from_currency"),
            "left"
        )

        joined_df = joined_df.select("t.*", col("e.rate").alias("rate"))
        return joined_df

    def convert_amount_to_eur(self, transactions_df: DataFrame) -> DataFrame:

        transactions_df = transactions_df.withColumn("amount_eur", col("amount") * col("rate"))
        return transactions_df

    def explode_guarantors(self, transactions_df: DataFrame) -> DataFrame:


        def fix_and_parse(guarantors_str):
            if guarantors_str is None:
                return None
            try:
                fixed = re.sub(r'(\w+):', r'"\1":', guarantors_str)
                fixed = re.sub(r'("NAME":)\s*([A-Za-z0-9_]+)', r'\1 "\2"', fixed)
                fixed = fixed.strip()
                return json.loads(fixed)
            except Exception as e:
                return None

        guarantors_schema = ArrayType(StructType([
            StructField("NAME", StringType(), True),
            StructField("PERCENTAGE", DoubleType(), True)
        ]))

        fix_and_parse_udf = udf(fix_and_parse, guarantors_schema)

        df_parsed = transactions_df.withColumn("guarantors_parsed", fix_and_parse_udf(F.col("guarantors")))

        df_exploded = df_parsed.withColumn("guarantor", F.explode("guarantors_parsed"))

        df_exploded = df_exploded.withColumn("guarantor_name", F.col("guarantor.NAME")) \
            .withColumn("guarantor_percentage", F.col("guarantor.PERCENTAGE"))

        df_exploded = df_exploded.drop("guarantors", "guarantors_parsed", "guarantor")

        return df_exploded

    def compute_guarantee_amount(self, transactions_df: DataFrame) -> DataFrame:

        transactions_df = transactions_df.withColumn("guarantee_eur", col("amount_eur") * (col("guarantor_percentage")))
        return transactions_df

    def aggregate_guarantees(self, transactions_df: DataFrame) -> DataFrame:

        agg_df = transactions_df.groupBy("country", "guarantor_name") \
            .agg(
            _sum(when(col("type") == "CLASS_A", col("guarantee_eur")).otherwise(lit(0))).alias("class_a"),
            _sum(when(col("type") == "CLASS_B", col("guarantee_eur")).otherwise(lit(0))).alias("class_b"),
            _sum(when(col("type") == "CLASS_C", col("guarantee_eur")).otherwise(lit(0))).alias("class_c"),
            _sum(when(col("type") == "CLASS_D", col("guarantee_eur")).otherwise(lit(0))).alias("class_d"),
            avg(when(col("type") == "CLASS_A", col("guarantee_eur"))).alias("avg_class_a"),
            avg(when(col("type") == "CLASS_B", col("guarantee_eur"))).alias("avg_class_b"),
            avg(when(col("type") == "CLASS_C", col("guarantee_eur"))).alias("avg_class_c"),
            avg(when(col("type") == "CLASS_D", col("guarantee_eur"))).alias("avg_class_d")
        )
        agg_df = agg_df.withColumn("partition_date", lit(self.report_date))
        agg_df = agg_df.select("guarantor_name", "class_a", "class_b", "class_c", "class_d",
                               "avg_class_a", "avg_class_b", "avg_class_c", "avg_class_d",
                               "country", "partition_date")
        agg_df = agg_df.orderBy("guarantor_name")
        return agg_df

    def generate_report_df(self,
                           transactions_df: DataFrame,
                           receivers_df: DataFrame,
                           exchange_rates_df: DataFrame) -> DataFrame:

        filtered_tx = self.filter_transactions(transactions_df)
        tx_with_receivers = self.join_with_receivers(filtered_tx, receivers_df)
        tx_with_rates = self.join_with_exchange_rates(tx_with_receivers, exchange_rates_df)
        tx_converted = self.convert_amount_to_eur(tx_with_rates)
        tx_exploded = self.explode_guarantors(tx_converted)
        tx_guarantee = self.compute_guarantee_amount(tx_exploded)
        report_df = self.aggregate_guarantees(tx_guarantee)
        return report_df
