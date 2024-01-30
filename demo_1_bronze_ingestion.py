import os
from init_session import spark
from pyspark.sql.types import StructType
from pyspark.sql.functions import count_distinct


def bronze_ingestion_pipeline():
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
        .load("data/landing")
    )

    (
        df_input
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
    )

    query = (
        df_input
        .writeStream
        .format("delta")  # memory = store in-memory table (for testing only)
        .option("path", "data/bronze")
        .queryName("bronze_ingestion")  # counts = name of the in-memory table
        .outputMode("append")  # complete = all the counts should be in the table
        .option("checkpointLocation", "data/bronze")
        .start()
    )
    return query


if __name__ == "__main__":

    query = bronze_ingestion_pipeline()
    query.awaitTermination()
