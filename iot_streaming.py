import os
from init_session import spark
from pyspark.sql.types import StructType
from pyspark.sql.functions import count_distinct


def run_continuous():
    # Read all the csv files written atomically in a directory
    user_schema = (
        StructType()
        .add("FirstName", "string")
        .add("LastName", "string")
        .add("Department", "string")
        .add("Location", "string")
    )

    df_input = (
        spark
        .readStream
        .format("csv")
        .option("sep", ",")
        .option("header", True)
        .schema(user_schema)
        .load("data/iot_input")
    )

    df_aggregate = (
        df_input
        .groupBy(df_input.Location)
        .count()
    )

    query = (
        df_aggregate
        .writeStream
        .format("console")  # memory = store in-memory table (for testing only)
        .queryName("agg_counts")  # counts = name of the in-memory table
        .outputMode("complete")  # complete = all the counts should be in the table
        .option("checkpointLocation", "data/bronze")
        #.trigger(Trigger.ProcessingTime("15 minutes"))
        .start()
    )

    query.awaitTermination()
    return "Done"


if __name__ == "__main__":
    input_folder = os.path.join(os.getcwd(), "data", "iot_input")

    run_continuous()
