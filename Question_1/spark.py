from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, map_from_arrays, collect_list
from pyspark.sql.types import LongType


def main():
    spark = SparkSession.builder.getOrCreate()

    # Read the csv 
    df = spark.read.option("header", True).csv("./data.csv")

    # Converting id to the right type
    df = df.withColumn("id", df.id.cast(LongType()))

    print("--- Initial DataFrame ---")
    df.show(truncate=False)

    # We need to implement a window function in other to only pick the latest records between id and name
    window = Window.orderBy(df.timestamp.desc()).partitionBy(["id", "name"])

    # Now we create the actual function based on the window above
    df_rn = df.withColumn("rn",row_number().over(window))

    # Remove everything that is not rn = 1 (ie. not the latest) and drop the row_number column since it was only needed for filtering
    df_filtered = df_rn.filter("rn = 1").drop("rn").orderBy(df_rn.timestamp.desc())

    # Groups all columns by id without losing name and value information to create the settings map
    # The grouping also makes sure that the settings events are ordered by latest timestamp first
    df_grouped = df_filtered.groupBy("id").agg(
        map_from_arrays(
            collect_list("name"),
            collect_list("value")
        ).alias("settings")
    ).orderBy("id")

    print("--- Solution ---")
    df_grouped.show(truncate=False)
    df_grouped.printSchema()

    return df_grouped


if __name__ == "__main__":
    main()