import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, countDistinct, count


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Transforms a text file into a word count CSV.
    
    This function:
    1. Reads the input text file
    2. Splits the text into words
    3. Normalizes words (lowercase, removes punctuation)
    4. Counts word occurrences
    5. Writes results to a CSV file
    
    Args:
        spark: SparkSession instance
        input_path: Path to the input text file (e.g., ./resources/word_count/words.txt)
        output_path: Path to the output CSV file (e.g., ./output/word_count/words_count.csv)
    """
    
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    # Step 1: Convert all text to lowercase
    logging.info("Normalizing text to lowercase")
    lowercase_df = input_df.withColumn("value", lower(col("value")))
    
    # Step 2: Remove punctuation and split into words
    logging.info("Splitting text into words and removing punctuation")
    # Replace punctuation with spaces, then split on whitespace
    words_df = lowercase_df.withColumn(
        "word",
        explode(split(regexp_replace(col("value"), r"[^\w\s]", " "), r"\s+"))
    )
    
    # Step 3: Filter out empty strings and non-meaningful words (optional: filter stopwords if needed)
    logging.info("Filtering empty strings")
    words_df = words_df.filter(col("word") != "")
    
    # Step 4: Count occurrences of each word
    logging.info("Counting word occurrences")
    word_counts = words_df.groupBy("word").count().withColumnRenamed("count", "count")
    
    # Step 5: Sort by count in descending order (optional, for better readability)
    word_counts = word_counts.orderBy(col("count").desc())
    
    # Step 6: Rename columns to match expected output format
    result_df = word_counts.select(
        col("word").alias("word"),
        col("count").alias("count")
    )

    logging.info("Writing csv to directory: %s", output_path)
    result_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)