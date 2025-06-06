import sys
from lib import DataManipulation, DataReader, Utils, logger
from pyspark.sql.functions import *
from lib.logger import Log4j
import os

if __name__ == '__main__':
    # Prompt the user to enter the environment
    job_run_env = input("Please enter the environment (e.g., dev, test, prod): ").strip()

    # Validate user input
    if not job_run_env:
        print("Environment not provided. Exiting.")
        sys.exit(1)

    print("Creating Spark Session")

    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)
    logger.warn("Created Spark Session")

    orders_df = DataReader.read_orders(spark, job_run_env)
    orders_filtered = DataManipulation.filter_closed_orders(orders_df)

    customers_df = DataReader.read_customers(spark, job_run_env)
    joined_df = DataManipulation.join_orders_customers(orders_filtered, customers_df)

    aggregated_results = DataManipulation.count_orders_state(joined_df)
    aggregated_results.show(50)

    # print(aggregated_results.collect())
    logger.info("This is the end of main")

    #adding a new feature -feature1