from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, from_csv, window, expr, to_timestamp,
    coalesce, lit, sum as spark_sum
)

# ==== PARAMETRY ====
KAFKA_BOOTSTRAP_SERVERS = "172.28.1.10:9092"
KAFKA_TOPIC = "kafka-input"
AIRPORTS_CSV_PATH = "hdfs:///user/student/input/airports.csv"  # <- Upewnij się, że ścieżka jest poprawna w HDFS
MONGO_URI = "mongodb://mongoadmin:secret@mongodb:27017"

D = 60  # długość okresu w minutach
N = 30  # liczba samolotów
CHECK_INTERVAL = "10 minutes"
LOOKAHEAD = 30  # przesunięcie okna w minutach

# ==== 1. SparkSession ====
spark = SparkSession.builder \
    .appName("FlightDelaysStreaming") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ==== 2. Schemat danych ====
flight_schema = StructType([
    StructField("airline", StringType()),
    StructField("flightNumber", StringType()),
    StructField("tailNumber", StringType()),
    StructField("startAirport", StringType()),
    StructField("destAirport", StringType()),
    StructField("scheduledDepartureTime", StringType()),
    StructField("scheduledDepartureDayOfWeek", IntegerType()),
    StructField("scheduledFlightTime", IntegerType()),
    StructField("scheduledArrivalTime", StringType()),
    StructField("departureTime", StringType()),
    StructField("taxiOut", IntegerType()),
    StructField("distance", IntegerType()),
    StructField("taxiIn", IntegerType()),
    StructField("arrivalTime", StringType()),
    StructField("diverted", IntegerType()),
    StructField("cancelled", IntegerType()),
    StructField("cancellationReason", StringType()),
    StructField("airSystemDelay", IntegerType()),
    StructField("securityDelay", IntegerType()),
    StructField("airlineDelay", IntegerType()),
    StructField("lateAircraftDelay", IntegerType()),
    StructField("weatherDelay", IntegerType()),
    StructField("cancelationTime", StringType()),
    StructField("orderColumn", IntegerType()),
    StructField("infoType", StringType())
])

# ==== 3. Strumień z Kafki ====
raw_stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

flights_string_df = raw_stream_df.selectExpr("CAST(value AS STRING) as csv_line")

flights_parsed_df = flights_string_df.select(
    from_csv(col("csv_line"), flight_schema.simpleString()).alias("flight")
).select("flight.*")

# ==== 4. Parsowanie czasów ====
flights_parsed_df = flights_parsed_df \
    .withColumn("scheduledArrivalTimeTS", to_timestamp("scheduledArrivalTime", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("scheduledDepartureTimeTS", to_timestamp("scheduledDepartureTime", "yyyy-MM-dd HH:mm:ss"))

# ==== 5. ETL: opóźnienia wylotów ====
delay_cols = ["airSystemDelay", "securityDelay", "airlineDelay", "lateAircraftDelay", "weatherDelay"]
for dc in delay_cols:
    flights_parsed_df = flights_parsed_df.withColumn(dc, coalesce(col(dc), lit(0)))

flights_parsed_df = flights_parsed_df.withColumn(
    "totalDepartureDelay",
    sum([col(dc) for dc in delay_cols])
)

agg_df = flights_parsed_df.groupBy("scheduledDepartureDayOfWeek", "startAirport") \
    .agg(
        expr("count(*) as totalFlights"),
        expr("sum(totalDepartureDelay) as totalDepartureDelay")
    )

# ==== 6. Wykrywanie anomalii ====
in_flight_df = flights_parsed_df \
    .filter((col("infoType") == "A") & (col("scheduledArrivalTimeTS").isNotNull()))

anomaly_window_df = in_flight_df \
    .withWatermark("scheduledArrivalTimeTS", "5 minutes") \
    .groupBy(
        window(col("scheduledArrivalTimeTS"), f"{D} minutes", CHECK_INTERVAL),
        col("destAirport")
    ).agg(expr("count(*) as arrivingFlights"))

# ==== 7. Łączenie z airports.csv ====
airports_df = spark.read.option("header", True).csv(AIRPORTS_CSV_PATH)

airports_selected_df = airports_df.selectExpr(
    "IATA", "Name as airportName", "City as city", "State as state"
)

anomaly_enriched_df = anomaly_window_df.join(
    airports_selected_df,
    anomaly_window_df["destAirport"] == airports_selected_df["IATA"],
    how="left"
)

anomalies_df = anomaly_enriched_df.filter(col("arrivingFlights") >= N).select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    col("airportName"),
    col("IATA"),
    col("city"),
    col("state"),
    col("arrivingFlights")
)

# ==== 8. Wypisywanie strumieni ====
def write_to_mongo(df, epoch_id, collection_name):
    record_count = df.count()
    print(f"[MongoDB Sink] Epoch {epoch_id} - Writing {record_count} records to collection '{collection_name}'")

    if record_count == 0:
        print(f"[MongoDB Sink] Epoch {epoch_id} - No data to write, skipping.")
        return

    try:
        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("spark.mongodb.connection.uri", MONGO_URI) \
            .option("spark.mongodb.database", "flight_db") \
            .option("spark.mongodb.collection", collection_name) \
            .save()
        print(f"[MongoDB Sink] Epoch {epoch_id} - Successfully wrote to '{collection_name}'")
    except Exception as e:
        print(f"[MongoDB Sink] Epoch {epoch_id} - Failed to write to '{collection_name}': {e}")


agg_query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_mongo(df, eid, "flight_aggregates")) \
    .start()

anomaly_query = anomalies_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_mongo(df, eid, "flight_anomalies")) \
    .start()

agg_query.awaitTermination()
anomaly_query.awaitTermination()
