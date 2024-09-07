import logging
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import to_date, min, when, col


RAW_DATA_PATH = "/opt/raw"
PROCESSED_DATA_PATH = "/opt/processed/"
PROCESSED_DATA_MERGED_PATH = "/opt/processed/merged/"
PROCESSED_DATA_MERGED_TEMP_PATH = "/opt/processed/merged_temp/"
PROCESSED_DATA_PATH_PARTITIONED = "/opt/processed/partitioned/"
LOG_PROCESSED_FILES_FILE_PATH = "/opt/processed/processed_files.log"
LOG_FILE_FILE_PATH = "/opt/processed/logs.log"

logging.basicConfig(
    filename=LOG_FILE_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_processed_files(log_file_path):
    try:
        with open(log_file_path, "r") as file:
            processed_files = set(line.strip() for line in file)
    except FileNotFoundError:
        processed_files = set()
    return processed_files


def update_processed_files(log_file_path, new_files):
    with open(log_file_path, "a") as file:
        for file_name in new_files:
            file.write(f"{file_name}\n")


def merge_data_with_existing(spark, processed_df):
    logging.info("Merging new data with existing...")
    try:
        existing_df = spark.read.parquet(PROCESSED_DATA_MERGED_PATH)
    except AnalysisException:
        existing_df = None

    try:
        if existing_df:
            combined_df = existing_df.union(processed_df)
            deduplicated_df = combined_df.dropDuplicates(["id"])
            deduplicated_df.write.mode("overwrite").parquet(
                PROCESSED_DATA_MERGED_TEMP_PATH
            )
            shutil.rmtree(PROCESSED_DATA_MERGED_PATH)
            shutil.copytree(PROCESSED_DATA_MERGED_TEMP_PATH, PROCESSED_DATA_MERGED_PATH)
            shutil.rmtree(PROCESSED_DATA_MERGED_TEMP_PATH)
        else:
            processed_df.write.mode("overwrite").parquet(PROCESSED_DATA_MERGED_PATH)
        logging.info("Successfully merged data!")
    except Exception as e:
        logging.exception(f"Failed to merge new data with existing: {e}")
        raise e


def transform_data(df):
    logging.info("Starting data transformation...")
    try:
        df_filtered_by_price = df.filter(df["price"] > 0)
        df_with_date = df_filtered_by_price.withColumn(
            "last_review", to_date(df_filtered_by_price["last_review"], "yyyy-MM-dd")
        )
        earliest_date = (
            df_with_date.select(min("last_review")).first()[0].strftime("%Y-%m-%d")
        )
        df_filled_last_review = df_with_date.fillna({"last_review": earliest_date})
        df_filled_reviews_per_month = df_filled_last_review.fillna(
            {"reviews_per_month": 0}
        )
        df_cleaned = df_filled_reviews_per_month.dropna(
            subset=["latitude", "longitude"]
        )
        df_with_price_range = df_cleaned.withColumn(
            "price_range",
            when(col("price") <= 100, "budget")
            .when((col("price") > 100) & (col("price") <= 300), "midrange")
            .otherwise("luxury"),
        )
        df_final = df_with_price_range.withColumn(
            "price_per_review",
            when(
                col("reviews_per_month") > 0, col("price") / col("reviews_per_month")
            ).otherwise(0),
        )
        logging.info("Successfully transformed data!")
        return df_final
    except Exception as e:
        logging.exception(f"Failed to transform data: {e}")
        raise e


def process_sql_queries(spark, df):
    logging.info("Processing SQL queries...")
    try:
        df.createOrReplaceTempView("listings")

        logging.info("Listings by Neighborhood Group:")
        query1 = """
            SELECT neighbourhood_group, COUNT(*) AS num_listings
            FROM listings
            GROUP BY neighbourhood_group
            ORDER BY num_listings DESC
        """
        result1 = spark.sql(query1)
        logging.info(result1._jdf.showString(20, 20, False))

        logging.info("Top 10 Most Expensive Listings:")
        query2 = """
            SELECT *
            FROM listings
            ORDER BY price DESC
            LIMIT 10
        """
        result2 = spark.sql(query2)
        logging.info(result2._jdf.showString(20, 20, False))

        logging.info("Average Price by Room Type:")
        query3 = """
            SELECT neighbourhood_group, room_type, AVG(price) AS avg_price
            FROM listings
            GROUP BY neighbourhood_group, room_type
        """
        result3 = spark.sql(query3)
        logging.info(result3._jdf.showString(20, 20, False))
    except Exception as e:
        logging.exception(f"Failed to execute SQL queries: {e}")
        raise e


def repartition_data(df):
    try:
        df_repartitioned = df.repartition("neighbourhood_group")
        df_repartitioned.write.partitionBy("neighbourhood_group").mode(
            "overwrite"
        ).parquet(PROCESSED_DATA_PATH_PARTITIONED)
        return df_repartitioned
    except Exception as e:
        logging.exception(f"Failed to repartition data: {e}")
        raise e


def check_data_quality(df, expected_count):

    critical_columns = ["price", "minimum_nights", "availability_365"]
    actual_count = df.count()

    if actual_count != expected_count:
        logging.exception(
            f"Row count validation failed: Expected {expected_count}, but found {actual_count}"
        )
    else:
        logging.info(f"Row count validation passed: {actual_count} records")

    for column in critical_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count == 0:
            logging.info(f"No NULL values in column '{column}'")
        else:
            logging.exception(
                f"NULL values found in column '{column}': {null_count} records"
            )


def main():
    spark = SparkSession.builder.appName("IncrementalProcessing").getOrCreate()

    all_files = set(os.listdir(RAW_DATA_PATH))
    processed_files = get_processed_files(LOG_PROCESSED_FILES_FILE_PATH)

    new_files = all_files - processed_files

    if not new_files:
        logging.info("No new files, skipping processing!")
        return

    logging.info(f"Processing {new_files}...")

    for file in new_files:
        logging.info(f"Processing {file}...")
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .load(str(Path(RAW_DATA_PATH) / file))
        )
        processed_df = transform_data(df)

        check_data_quality(processed_df, 48874)

        process_sql_queries(spark, processed_df)

        df_repartitioned = repartition_data(processed_df)

        merge_data_with_existing(spark, df_repartitioned)

        update_processed_files(LOG_PROCESSED_FILES_FILE_PATH, new_files)


if __name__ == "__main__":
    main()
