from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import os
import confg as cfg

if __name__ == '__main__':

    spark = SparkSession.builder \
       .appName("Transform Json To DF") \
       .getOrCreate()

    files = os.listdir(cfg.BASE_PATH)
    print(files)

    for file in files:
        print(f"""
            Transforming the file {file}
        """)

        df_multiline = spark.read \
            .option("multiline", "true") \
            .json(cfg.BASE_PATH) \
            .withColumn("value", fn.to_json(fn.struct(fn.col("*")))) \
            .withColumn("key", fn.lit("key")) \
            .withColumn("value", fn.encode(fn.col("value"), "utf-8").cast("binary")) \
            .withColumn("key", fn.encode(fn.col("key"), "utf-8").cast("binary")) \
            .limit(2000)

        print(f"the type of thid DF is {type(df_multiline)}")
        # df_multiline.show()
        df_multiline.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", cfg.bootstrap) \
            .option("topic", cfg.topic_name) \
            .option("checkpointLocation", "/tmpCheckPoint") \
            .save() \

    spark.stop()
