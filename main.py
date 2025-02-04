from pyspark.sql import SparkSession
from data_initializer import DataInitializer
from guarantees_report_job import GuaranteesReportJob

spark = SparkSession.builder \
    .appName("PySpark_PyCharm_Setup") \
    .master("local[*]") \
    .getOrCreate()

data_loader = DataInitializer(spark, data_path="data")
report_job = GuaranteesReportJob(spark, "25.01.2024")
transactions_df, exchange_rates_df, receivers_df = data_loader.load_all_files()


report_df = report_job.generate_report_df(transactions_df, receivers_df, exchange_rates_df)
report_df.show(truncate=False)


spark.stop()