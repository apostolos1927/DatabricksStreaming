# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime as dt
import json


#####FIRST APPROACH####
connectionString = "....."
ehConf = {}
startOffset = "-1"
endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
startingEventPosition = {
    "offset": startOffset,
    "seqNo": -1,  # not in use
    "enqueuedTime": None,  # not in use
    "isInclusive": True,
}
endingEventPosition = {
    "offset": None,  # not in use
    "seqNo": -1,  # not in use
    "enqueuedTime": endTime,
    "isInclusive": True,
}
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)

ehConf[
    "eventhubs.connectionString"
] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "$Default"

json_schema = StructType(
    [
        StructField("DeviceID", IntegerType(), True),
        StructField("DeviceNumber", IntegerType(), True),
        StructField("DeviceCountry", StringType(), True),
    ]
)
df = spark.readStream.format("eventhubs").options(**ehConf).load()

df = df.withColumn("body", F.from_json(df.body.cast("string"), json_schema))

df = df.select(
    F.col("body.DeviceID"), F.col("body.DeviceNumber"), F.col("body.DeviceCountry")
)

df = df.filter(df.DeviceNumber > 10)

df.writeStream.outputMode("append").option(
    "checkpointLocation", "/tmp/..../_checkpoints/"  ##fill in DBFS checkpoint location
).format("delta").partitionBy("DeviceCountry").trigger(once=True).table(
    "delta_data"
).awaitTermination()


#####SECOND APPROACH####

(
    spark.readStream.format("eventhubs")
    .options(**ehConf)
    .load()
    .writeStream.format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation", "/tmp/...../_checkpoints/"
    )  ##fill in DBFS checkpoint location
    .trigger(once=True)
    .table("delta_data2")
)

df = spark.sql("""Select body  from delta_data2""")
df = df.withColumn("body", F.from_json(df.body.cast("string"), json_schema))

df = df.select(
    F.col("body.DeviceID"), F.col("body.DeviceNumber"), F.col("body.DeviceCountry")
)
display(df)
