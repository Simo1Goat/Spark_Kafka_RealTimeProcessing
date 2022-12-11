from pyspark.sql import SparkSession
import confg as cfg
import pyspark.sql.functions as fn

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("average temperature by each state") \
        .getOrCreate()

    sc = spark.sparkContext

    # read the stream from kafka topic
    dataFrame = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.bootstrap) \
        .option("subscribe", cfg.topic_name) \
        .option("startingoffsets", cfg.auto_reset_offset) \
        .load()

    print(f"the schema of our topic {cfg.topic_name}")
    dataFrame.printSchema()

    dataFrame = dataFrame.select(
        fn.from_json(fn.decode("value", "utf-8"),
                     schema=cfg.mySchema
                     ).alias("value")
    )

    selected_cols = dataFrame.select(fn.col("value.*")) \
        .select([fn.col("state"),
                 fn.col("payload.data.temperature").alias("temperature")
                 ])

    average_state = selected_cols.groupBy("state") \
        .agg(fn.mean(fn.col("temperature")).alias("avgTemperature")) \
        .orderBy(fn.col("avgTemperature").desc())

    average_state.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("chechkpointLocation", "tmpCheckPoint") \
        .start() \
        .awaitTermination()
