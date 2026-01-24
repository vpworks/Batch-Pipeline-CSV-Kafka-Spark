from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a session to initialize spark and enable DataFrame operations
spark = SparkSession.builder.appName("KafkaToSparkPipeline").getOrCreate()

# Read data from kafka     
kafka_df = spark.read.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe", "walmart-data").option("startingOffsets","earliest").load()

# Deserialize kafka data to JSON strings      
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# Define Schema
schema = StructType([
        StructField("Store", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Weekly_Sales", StringType(), True),
        StructField("Holiday_Flag", StringType(), True),
        StructField("Temperature", StringType(), True),
        StructField("Fuel_Price", StringType(), True),
        StructField("CPI", StringType(), True),
        StructField("Unemployment", StringType(), True)
        ])

# Parse JSON strings to Spark DataFrame
df = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Convert Datatypes
df = df.withColumn("Store", col("Store").cast("int")).withColumn("Date", to_date("Date", "dd-MM-yyyy")).withColumn("Weekly_Sales", col("Weekly_Sales").cast(DecimalType(10, 2))).withColumn("Holiday_Flag", col("Holiday_Flag").cast("int")).withColumn("Temperature", col("Temperature").cast("double")).withColumn("Fuel_Price", col("Fuel_Price").cast("double")).withColumn("CPI", col("CPI").cast("double")).withColumn("Unemployment", col("Unemployment").cast("double"))

# Transformations
# Defining DataFrame 1 
weekly_df = df.withColumn("Month", month(df["Date"])).withColumn("Week", weekofyear(df["Date"])).withColumn("Year", year(df["Date"])).withColumn("Weekend_Flag", when(dayofweek(df["Date"]).isin(1,7), 1).otherwise(0)).withColumn("Day_of_Week", dayofweek(df["Date"]))       

# Defining DataFrame 2
monthly_df = weekly_df.groupBy("Store", "Year", "Month").agg(sum("Weekly_Sales").alias("Monthly_Sales"))


# Write to storage
weekly_query = weekly_df.write.format("csv").option("header", "true").mode("overwrite").save("/Users/mularoe/Desktop/WALMART/weekly/weekly_data")
        
monthly_query = monthly_df.write.format("csv").option("header", "true").mode("overwrite").save("/Users/mularoe/Desktop/WALMART/monthly/monthly_data")



        
