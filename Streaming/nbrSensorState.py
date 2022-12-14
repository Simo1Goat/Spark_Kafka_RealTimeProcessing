from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import confg as cfg


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Number of sensors in each state").getOrCreate()
    sc = spark.sparkContext

    # read the stream from kafka topic
    dataFrame = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.bootstrap) \
        .option("subscribe", cfg.topic_name) \
        .option("startingoffsets", cfg.auto_reset_offset) \
        .option("checkpointLocation", "tmpCheckPoint") \
        .load()

    print(f"the schema of our topic {cfg.topic_name} \n")
    dataFrame.printSchema()

    dataFrame = dataFrame.select(
        fn.from_json(fn.decode("value", "utf-8"),
                     schema=cfg.mySchema
                     ).alias("value")
    )

    print("the schema of our data after deserializing it \n")
    dataFrame.printSchema()

    selected_cols = dataFrame.select(fn.col("value.*"))\
        .select([
            fn.col("state"),
            fn.col("guid").alias("sensor")
        ])

    nbrSensorSate = selected_cols.groupBy(fn.col("state")) \
        .agg(fn.count("sensor").alias("total_sensors")) \
        .orderBy(fn.col("total_sensors").asc())

    nbrSensorSate.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("chechkpointLocation", "tmpCheckPoint") \
        .start() \
        .awaitTermination()
