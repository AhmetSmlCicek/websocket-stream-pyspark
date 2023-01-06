from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Set localhost socket parameters from ther server
localhost = "127.0.0.1"
local_port = 8776

# Create Spark session
spark = SparkSession.builder.appName("Coinbase Stream Reader").getOrCreate()

# Create streaming DataFrame from local socket
# delimiter added on server side
lines = spark.readStream.format("socket") \
    .option("host", localhost) \
    .option("port", local_port) \
    .option("delimiter", "/n") \
    .option("includeTimestamp", True) \
    .load()


#turn json format column into proper column format based on key name
df = lines.select(json_tuple(col("value"),"type","product_id","changes","time")) \
    .toDF("type","product_id","changes","time")



# Print stream to terminal
# truncate option is important to avoid not seeing anything in the terminal
query = df.writeStream.outputMode("append") \
    .option("truncate", False) \
    .format("console")\
    .start()\
    .awaitTermination()
