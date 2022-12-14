import confg as cfg
import findspark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn

findspark.init()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Average Temperature by Each State").getOrCreate()
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

    average_state = selected_col.rdd.map(lambda x: (x['state'], (x['payload']['data']['temperature'], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0]/x[1][1]))

    # average_state = selected_col.groupBy("state") \
    #    .agg(fn.mean(fn.col("payload.data.temperature")).alias("sum_temperature")) \
    #    .orderBy(fn.col("sum_temperature").desc())
    Schema = StructType([
        StructField("state", StringType(), True),
        StructField("average", StringType(), True)
    ])
    df_average = average_state.toDF(schema=Schema)
    df_average.show()

    # average_state.writeStream \
    #    .format("console") \
    #    .outputMode("complete") \
    #    .option("checkpointLocation", "tmpCheckPoint") \
    #    .start() \
    #    .awaitTermination()
