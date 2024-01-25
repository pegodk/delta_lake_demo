from init_session import spark
from pyspark.sql.types import StructType
from pyspark.sql.functions import count_distinct


def run_continuous():
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
        .format("console")  # memory = store in-memory table (for testing only)
        .queryName("agg_counts")  # counts = name of the in-memory table
        .outputMode("complete")  # complete = all the counts should be in the table
        .start()
    )

    query.awaitTermination()
    return "Done"


if __name__ == "__main__":
    run_continuous()
