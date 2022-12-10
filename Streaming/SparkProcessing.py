import confg as cfg
import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn

findspark.init()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("PySpark Streaming").getOrCreate()
    sc = spark.sparkContext

    dataFrame = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", cfg.bootstrap) \
        .option("subscribe", cfg.topic_name) \
        .option("startingoffsets", cfg.auto_reset_offset) \
        .load()

    print(f"\n the schema of our topic {cfg.topic_name}")
    dataFrame.printSchema()

    value = dataFrame.selectExpr("CAST(value AS STRING)").alias("value")

    print("\n the schema of our topic after selecting & deserializing the value column")
    jsonValue = value.select(fn.from_json("value", cfg.mySchema).alias("value"))
    jsonValue.printSchema()

    selected_col = jsonValue.select("value.*")

    average_state = selected_col.groupBy("state") \
        .agg(fn.mean(fn.col("payload.data.temperature")).alias("sum_temperature")) \
        .orderBy(fn.col("sum_temperature").desc())

    average_state.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "tmpCheckPoint") \
        .start() \
        .awaitTermination()
