import os
from init_session import spark
from pyspark.sql.types import StructType
from pyspark.sql.functions import count_distinct


def create_table():
    spark.sql(
        """
            CREATE TABLE IF NOT EXISTS silver (
                FirstName string NOT NULL,
                LastName string NOT NULL,
                Department string,
                Location string
            )
            USING DELTA
        """
    )


def upsertToDelta(microBatchOutputDF, batchId):
    microBatchOutputDF.createOrReplaceTempView("updates")
    microBatchOutputDF._jdf.sparkSession().sql(
        f"""
            MERGE INTO silver t
            USING updates s
            ON s.FirstName = t.FirstName
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    )


def silver_ingestion_pipeline():
    df_input = (
        spark
        .readStream
        .format("delta")
        .load("data/bronze")
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
        .format("console")  # memory = store in-memory table (for testing only)
        #.option("path", "spark-warehouse/silver")
        .foreachBatch(upsertToDelta)
        .queryName("silver_ingestion")  # counts = name of the in-memory table
        .outputMode("append")  # complete = all the counts should be in the table
        #.option("checkpointLocation", "spark-warehouse/silver")
        .start()
    )
    # query = (
    #     df_input
    #     .writeStream
    #     .format("delta")  # memory = store in-memory table (for testing only)
    #     .option("path", "spark-warehouse/silver")
    #     .foreachBatch(upsertToDelta)
    #     .queryName("bronze_ingestion")  # counts = name of the in-memory table
    #     .outputMode("append")  # complete = all the counts should be in the table
    #     .option("checkpointLocation", "spark-warehouse/silver")
    #     .start()
    # )
    return query


if __name__ == "__main__":
    create_table()
    query = silver_ingestion_pipeline()
    query.awaitTermination()
