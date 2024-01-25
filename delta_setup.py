from init_session import spark
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
# from pyspark.sql.functions import *

def write_data():
    # Create a spark dataframe and write as a delta table
    print("Starting Delta table creation")

    schema = StructType([
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("house", StringType(), True),
        StructField("location", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data = [
        ("Robert", "Baratheon", "Baratheon", "Storms End", 48),
        ("Eddard", "Stark", "Stark", "Winterfell", 46),
        ("Jamie", "Lannister", "Lannister", "Casterly Rock", 29)
    ]

    sample_dataframe = spark.createDataFrame(data=data, schema=schema)
    #sample_dataframe.write.mode(saveMode="append").format("delta").save("data/delta-table")

    # Start running the query that prints the running counts to the console
    query = sample_dataframe \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()


def read_data():
    flights_delta = spark.read.format("delta").load("data/delta-table")
    flights_delta.show()


if __name__ == "__main__":
    write_data()
    # read_data()
