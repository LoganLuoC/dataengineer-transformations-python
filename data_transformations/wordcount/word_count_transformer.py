import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, regexp_replace

logging.basicConfig(level=logging.INFO)

def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Transforms a text file into a word count CSV

    1. Reads a text file from the specified input path
    2. Counts the occurrences of each word in the text file
    3. Writes the word counts to a CSV file in the specified output path
    """
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    # Split the text into words and count them

    word_df = input_df.select(explode(
                        split(regexp_replace(input_df.value, r"[^\w\s]", " "), r"\s+")
                    ).alias("word"))
    word_df = word_df.filter(word_df.word != "")  # Filter out empty strings

    # Count the occurrences of each word
    word_count_df = word_df.groupBy("word").count()
    word_counts = word_count_df.orderBy(word_count_df["count"].desc())

    logging.info("Writing csv to directory: %s", output_path)

    word_counts.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
