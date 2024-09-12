import sys
from datetime import datetime, date
import random

import pandas as pd
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StructType, StructField, StringType, IntegerType, FloatType, \
    DateType
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def generate_random_nyc_data(amount=1000):
    fake = Faker()

    random_data = []
    for _ in range(amount):
        record = {
            'id': fake.random_int(min=1111111, max=sys.maxsize),
            'name': fake.sentence(nb_words=5),
            'host_id': fake.random_int(min=2348, max=274321313),
            'host_name': fake.first_name(),
            'neighbourhood_group': fake.random_element(
                elements=('Brooklyn', 'Manhattan', 'Queens', 'Bronx', 'Staten Island')),
            'neighbourhood': fake.city(),
            'latitude': round(random.uniform(40.5, 40.9), 5),
            'longitude': round(random.uniform(-74.0, -73.7), 5),
            'room_type': fake.random_element(elements=('Entire home/apt', 'Private room', 'Shared room')),
            'price': fake.random_int(min=0, max=10000),
            'minimum_nights': fake.random_int(min=0, max=500),
            'number_of_reviews': fake.random_int(min=0, max=1000),
            'last_review': str(fake.date_between(date(2011, 1, 1), date(2019, 12, 31))),
            'reviews_per_month': round(random.uniform(0.0, 5.0), 1),
            'calculated_host_listings_count': fake.random_int(min=1, max=60),
            'availability_365': fake.random_int(min=0, max=365)
        }
        random_data.append(record)

    now = datetime.now()
    formatted_now = now.strftime("%Y_%m_%d_%H_%M_%S")
    generated_filename = "raw/Generated_" + formatted_now + ".csv"
    pd.DataFrame(random_data).to_csv(generated_filename)


def process_batch(df, batch_id):
    log_message("New data generated with batch_id: " + str(batch_id))
    generate_random_nyc_data()
    pass


def log_message(message, level="INFO"):
    color_codes = {
        "INFO": "\033[94m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "RESET": "\033[0m"
    }

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    color = color_codes.get(level, color_codes["RESET"])
    formatted_message = f"{color}[{current_time}] [{level}] {message}{color_codes['RESET']}"

    print(formatted_message)


log_message("Creating spark session")
spark = SparkSession.builder.master("local").appName("PySpark_technical-tasks").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("OFF")

nyc_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("host_id", IntegerType(), True),
    StructField("host_name", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), False),
    StructField("room_type", StringType(), False),
    StructField("price", IntegerType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("last_review", StringType(), True),
    StructField("reviews_per_month", FloatType(), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
])

df = (
    spark.readStream
    .schema(nyc_schema)
    .format("csv")
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("maxFilesPerTrigger", 1)
    .load("./raw")
)

log_message("Status of streaming: " + str(df.isStreaming))

# Filtering data
df = (df.filter(col("id").isNotNull()).filter("price > 0").fillna(value="2011-03-28", subset=["last_review"]).fillna(
    value=0, subset=["reviews_per_month"]).filter(col("latitude").isNotNull()).filter(col("longitude").isNotNull()))
log_message("Data was filtered and transformed")

# Add price type column
df.withColumn(
    "price_range",
    F.when(df["price"] < 100, "budget")
    .when(df["price"] > 500, "luxury")
    .otherwise("mid-range"))
log_message("Added price_range column")

# Add
df.withColumn("price_per_review", df["price"] / df["number_of_reviews"])
log_message("price_per_review")

# Performing SQL
df.createOrReplaceTempView("airbnb_nyc")
sql_res = spark.sql(
    "select neighbourhood_group, count(*) as listing from airbnb_nyc group by neighbourhood_group order by listing desc")
spark.sql("select * from airbnb_nyc order by price desc limit 10")
spark.sql(
    "select neighbourhood_group, room_type, avg(price) from airbnb_nyc group by neighbourhood_group, room_type order by neighbourhood_group, room_type")

# Reparation
df_ng = df.repartition("neighbourhood_group")

# Saving
now = datetime.now()
formatted_now = now.strftime("%Y_%m_%d_%H_%M_%S")
transformed_filename = "Transformed_" + formatted_now + ".parquet"
ng_filename = "NG_" + formatted_now + ".parquet"
df_stream = df.writeStream.format("parquet").option(
    "checkpointLocation", "log/").option("path", "processed/").start(
    trancate=False)
df_ng_stream = df_ng.writeStream.format("parquet").outputMode("update").foreachBatch(process_batch).option(
    "checkpointLocation", "log/").option("path",
                                         "processed/").start(
    trancate=False)
df_ng_stream.awaitTermination(1000)
df_stream.awaitTermination(1000)
