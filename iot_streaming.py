from init_session import spark
from pyspark.sql.types import StructType
from pyspark.sql.functions import count_distinct


def run_once():
    # Read all the csv files written atomically in a directory
    userSchema = StructType().add("FirstName", "string").add("LastName", "string").add("Department", "string").add(
        "Location", "string")

    streamingInputDF = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv("data/iot_input/")  # Equivalent to format("csv").load("/path/to/directory")

    streamingCountsDF = (
        streamingInputDF
        .groupBy(streamingInputDF.Location)
        .count()
    )

    query = (
        streamingCountsDF
        .writeStream
        .format("memory")  # memory = store in-memory table (for testing only)
        .queryName("counts")  # counts = name of the in-memory table
        .outputMode("complete")  # complete = all the counts should be in the table
        .start()
    )

    spark.sql("select * from counts").show()  # interactively query in-memory table

    spark.streams.awaitAnyTermination()


def run_continuous():
    userSchema = StructType().add("FirstName", "string").add("LastName", "string").add("Department", "string").add(
        "Location", "string")

    spark \
        .readStream \
        .format("csv") \
        .option("sep", ",") \
        .schema(userSchema) \
        .load("data/iot_input/") \
        .selectExpr("COUNT(*)") \
        .writeStream \
        .format("memory") \
        .queryName("counts_query") \
        .outputMode("complete") \
        .trigger(continuous="1 second") \
        .start()


if __name__ == "__main__":
    # run_once()
    run_continuous()
