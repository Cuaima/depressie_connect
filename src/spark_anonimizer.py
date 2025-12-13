import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from text_anonymizer import anonymize as ta_anonymize


DATA_DIR = "data"
OUTPUT_DIR = "output"


def create_spark():
    return (
        SparkSession.builder
        .appName("MessageTextAnonymization")
        .getOrCreate()
    )


def anonymize_text(text):
    if text is None:
        return None
    anonymized, _ = ta_anonymize(str(text))
    return anonymized


anonymize_text_udf = udf(anonymize_text, StringType())


def anonymize_messages_csv(
    input_csv="messages.csv",
    output_csv="messages_cleaned_anonymized.csv"
):
    spark = create_spark()

    input_path = os.path.join(DATA_DIR, input_csv)
    output_path = os.path.join(OUTPUT_DIR, output_csv)

    df = spark.read.csv(input_path, header=True, multiLine=True)

    if "MessageText" not in df.columns:
        raise RuntimeError("MessageText column not found")

    df = df.withColumn(
        "MessageText",
        anonymize_text_udf(col("MessageText"))
    )

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    spark.stop()
