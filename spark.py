# spark_stream_to_sqlite.py
from pyspark.sql import SparkSession, functions as F, types as T
import sqlite3
# import logging

# logging.getLogger("py4j").setLevel(logging.WARN)
# logging.getLogger("pyspark").setLevel(logging.WARN)

# --- Configuración Spark ---
spark = SparkSession.builder \
    .appName("KappaPcInfo") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Definimos esquema del JSON esperado ---
schema = T.StructType() \
    .add('pc_id', T.StringType()) \
    .add('cpu_usage', T.FloatType()) \
    .add('ram_usage', T.FloatType()) \
    .add('packets_per_second', T.FloatType()) \
    .add('time', T.DoubleType()) \


# --- Leemos desde Kafka ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "datos") \
    .option("startingOffsets", "earliest") \
    .load()

# kafka_df tiene columnas: key, value (binary), topic, partition, offset, timestamp
json_str_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")


# parseamos el json_str según el esquema
#parse time
parsed = json_str_df.select(F.from_json(F.col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", F.from_unixtime("time").cast("timestamp")) \
    .withColumn("cpu_usage_category",F.when(F.col("cpu_usage") < 30, "bajo").when(F.col("cpu_usage") < 70, "medio").otherwise("alto"))
    # .withColumn("ingest_ts", F.current_timestamp())


ventana_df = parsed.groupBy(
    F.window("event_time", "10 seconds", "10 seconds"),  # ventana de 10s, cada 10s
    F.col("pc_id")
).agg(
    F.avg("cpu_usage").alias("avg_cpu"),
    F.stddev("cpu_usage").alias("stddev_cpu"),
    F.avg("ram_usage").alias("avg_ram"),
    F.avg("packets_per_second").alias("packets_avg"),
    F.stddev("packets_per_second").alias("stddev_packets"),
    F.count("*").alias("event_count")
)




# # Primero, seleccionamos la ventana y pc_id en ventana_df
# ventana_metrics = ventana_df.select(
#     F.col("window"),
#     F.col("pc_id"),
#     F.col("avg_cpu"),
#     F.col("stddev_cpu"),
#     F.col("packets_avg"),
#     F.col("stddev_packets")
# ).alias("vm")
#
# # Unión entre el streaming original y las métricas de ventana por ventana y pc_id
# joined = parsed.join(
#     ventana_metrics,
#     on=[
#         (parsed.event_time >= ventana_metrics.window.start) &
#         (parsed.event_time < ventana_metrics.window.end) &
#         (parsed.pc_id == ventana_metrics.pc_id)
#     ],
#     how="left"
# )
#
# # Detectamos anomalía: cpu_usage > avg_cpu + 3*stddev_cpu o cpu_usage < avg_cpu - 3*stddev_cpu
# anomalies = joined.withColumn(
#     "cpu_anomaly",
#     (F.col("cpu_usage") > (F.col("avg_cpu") + 3 * F.col("stddev_cpu"))) |
#     (F.col("cpu_usage") < (F.col("avg_cpu") - 3 * F.col("stddev_cpu")))
# ).withColumn(
#     "packets_anomaly",
#     (F.col("packets_per_second") > (F.col("packets_avg") + 3 * F.col("stddev_packets"))) |
#     (F.col("packets_per_second") < (F.col("packets_avg") - 3 * F.col("stddev_packets")))
# ).filter((F.col("cpu_anomaly") == True) | (F.col("packets_anomaly") == True))





# --- Creamos la tabla SQLite si no existe (driver sqlite3 de Python) ---
def ensure_sqlite_table(db_path="kappa_output.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS todos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pc_id TEXT,
        cpu_usage FLOAT,
        ram_usage FLOAT,
        packets_per_second INTEGER,
        cpu_usage_category TEXT,
        time INTEGER
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS promedioPorVentana (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pc_id TEXT,
        window_start TEXT,
        window_end TEXT,
        avg_cpu FLOAT,
        stddev_cpu FLOAT,
        avg_ram FLOAT,
        packets_avg INTEGER,
        stddev_packets FLOAT,
        event_count INTEGER
    )
    """)
    conn.commit()
    conn.close()

ensure_sqlite_table()

# --- Función que escribe cada micro-batch a SQLite ---
def foreach_batch_function(batch_df, batch_id):
    # Si no hay filas, salimos rápido
    if batch_df.rdd.isEmpty():
        return

    # Seleccionamos columnas que queremos guardar y convertimos a pandas (pequeños volúmenes)
    # Convertimos timestamp a string para compatibilidad con SQLite

    # print(batch_df)
    # batch_df.show(truncate=False)

    pdf = batch_df.toPandas()

    conn = sqlite3.connect("kappa_output.db")
    cursor = conn.cursor()
    # Convertimos filas en tuplas y usamos INSERT
    rows = [tuple(x) for x in pdf[["pc_id", "cpu_usage", "ram_usage", "packets_per_second", "cpu_usage_category", "time"]].to_numpy()]


    for idx, row in pdf.iterrows():
        pc_id = row["pc_id"]

        cursor.execute("""
            SELECT avg_cpu, stddev_cpu, packets_avg, stddev_packets FROM promedioPorVentana
            WHERE pc_id = ?
            ORDER BY window_end DESC LIMIT 1
        """, (pc_id,))
        last_window = cursor.fetchone()
        avg_cpu_last, stddev_cpu_last, packets_avg_last, stddev_packets_last = (None, None, None, None)
        if last_window:
            avg_cpu_last, stddev_cpu_last, packets_avg_last, stddev_packets_last = last_window
        if avg_cpu_last is not None and stddev_cpu_last is not None:
            if row["cpu_usage"] > avg_cpu_last + 3*stddev_cpu_last:
                print(f"Anomalia detectada en el uso de cpu del pc {pc_id}:\n\tuso del cpu %{row['cpu_usage']}")
        if packets_avg_last is not None and stddev_packets_last is not None:
            if row["packets_per_second"] > packets_avg_last + 3*stddev_packets_last:
                print(f"Anomalia detectada en el uso de red del pc {pc_id}:\n\tpaquetes enviados por seguno {row['packets_per_second']}")


    print("funcion", rows)
    with conn:
        conn.executemany(
            "INSERT INTO todos (pc_id, cpu_usage, ram_usage, packets_per_second, cpu_usage_category, time) VALUES (?, ?, ?, ?, ?, ?)",
            rows
        )
    conn.close()
    print(f"Batch {batch_id}: insertadas {len(rows)} filas en SQLite")

def foreach_batch_ventana(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    window_df= batch_df.withColumn("window_start", F.col("window.start").cast("string")) \
        .withColumn("window_end", F.col("window.end").cast("string")) \
        .drop("window")

    pdf = window_df.toPandas()
    conn = sqlite3.connect("kappa_output.db")
    rows = [tuple(x) for x in pdf[["pc_id", "window_start", "window_end", "avg_cpu", "stddev_cpu", "avg_ram", "packets_avg", "stddev_packets", "event_count"]].to_numpy()]
    print("ventana", rows)
    with conn:
        conn.executemany(
            "INSERT INTO promedioPorVentana (pc_id, window_start, window_end, avg_cpu, stddev_cpu, avg_ram, packets_avg, stddev_packets, event_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )

# def foreach_batch_anomalies(batch_df, batch_id):
#     if batch_df.rdd.isEmpty():
#         return
#     print("Anomalias detectadas: ")
#     batch_df.show(truncate=False)

# --- Lanzamos el streaming ---
query = parsed.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_kappa_checkpoint") \
    .start()

query_ventana = ventana_df.writeStream \
    .foreachBatch(foreach_batch_ventana) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_kappa_checkpoint_window") \
    .start()

# query_anomalies = anomalies.writeStream \
#     .foreachBatch(foreach_batch_anomalies) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/spark_kappa_checkpoint_anomalies") \
#     .start()

# query_anomalies.awaitTermination()
query_ventana.awaitTermination()
query.awaitTermination()
