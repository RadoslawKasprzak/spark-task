import unittest
from guarantees_report_job import GuaranteesReportJob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import to_date


class GuaranteesReportJobTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[*]") \
            .appName("GuaranteesReportJobTest") \
            .getOrCreate()

    def test_generate_report(self):

        transactions_data = [
            {
                "transaction_id": "tx1",
                "guarantors": '[{NAME: G1, PERCENTAGE: 0.5}, {NAME: G2, PERCENTAGE: 0.3}]',
                "sender_name": "S1",
                "receiver": "R1",
                "type": "CLASS_A",
                "country": "Poland",
                "currency": "USD",
                "amount": 100.0,
                "status": "OPEN",
                "date": "2025-01-31",
                "partition_date": "2025-01-31",
                "is_valid": None
            }
        ]
        transactions_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("guarantors", StringType(), True),
            StructField("sender_name", StringType(), True),
            StructField("receiver", StringType(), True),
            StructField("type", StringType(), True),
            StructField("country", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("date", StringType(), True),
            StructField("partition_date", StringType(), True),
            StructField("is_valid", StringType(), True)
        ])
        transactions_df = self.spark.createDataFrame(transactions_data, schema=transactions_schema)
        transactions_df = transactions_df.withColumn("date", to_date("date", "yyyy-MM-dd"))

        receivers_data = [
            {
                "receiver_name": "R1",
                "country": "Poland",
                "partition_date": "2025-01-31"
            }
        ]
        receivers_schema = StructType([
            StructField("receiver_name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("partition_date", StringType(), True)
        ])
        receivers_df = self.spark.createDataFrame(receivers_data, schema=receivers_schema)

        exchange_rates_data = [
            {
                "from_currency": "USD",
                "to_currency": "EUR",
                "rate": 0.9,
                "partition_date": "2025-01-31"
            }
        ]
        exchange_rates_schema = StructType([
            StructField("from_currency", StringType(), True),
            StructField("to_currency", StringType(), True),
            StructField("rate", DoubleType(), True),
            StructField("partition_date", StringType(), True)
        ])
        exchange_rates_df = self.spark.createDataFrame(exchange_rates_data, schema=exchange_rates_schema)

        job = GuaranteesReportJob(self.spark, report_date="2025-01-31")
        report_df = job.generate_report_df(transactions_df, receivers_df, exchange_rates_df)
        report_data = report_df.collect()

        # We expect two rows - one for guarantee G1 and another for guarantee G2
        self.assertEqual(len(report_data), 2)

        for row in report_data:
            if row["guarantor_name"] == "G1":
                self.assertAlmostEqual(row["class_a"], 45.0, places=2)
                self.assertAlmostEqual(row["avg_class_a"], 45.0, places=2)
            elif row["guarantor_name"] == "G2":
                self.assertAlmostEqual(row["class_a"], 27.0, places=2)
                self.assertAlmostEqual(row["avg_class_a"], 27.0, places=2)

        expected_columns = {"guarantor_name", "class_a", "class_b", "class_c", "class_d",
                            "avg_class_a", "avg_class_b", "avg_class_c", "avg_class_d", "country", "partition_date"}
        self.assertTrue(expected_columns.issubset(set(report_df.columns)))


if __name__ == '__main__':
    unittest.main()
