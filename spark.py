from pyspark.sql import SparkSession, functions as F, types as T
import sqlite3

sqlite_db_path = "./data/spark_output.db"

# --- Configuración Spark ---
spark = SparkSession.builder \
    .appName("KappaPcInfo") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Muestra solo errores y advertencias

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
# parse time
parsed = json_str_df.select(F.from_json(F.col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", F.from_unixtime("time").cast("timestamp")) \
    .withColumn("cpu_usage_category",F.when(F.col("cpu_usage") < 30, "bajo").when(F.col("cpu_usage") < 70, "medio").otherwise("alto"))

# Creamos una ventana de datos de 10s cada 10s
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

# --- Creamos la tabla SQLite si no existe (driver sqlite3 de Python) ---
def ensure_sqlite_table(db_path=sqlite_db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS crudo (
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

    # Transformamos a pandas
    pdf = batch_df.toPandas()

    # Conectamos con la base de datos
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Convertimos filas en tuplas y usamos INSERT
    rows = [tuple(x) for x in pdf[["pc_id", "cpu_usage", "ram_usage", "packets_per_second", "cpu_usage_category", "time"]].to_numpy()]

    # Analizamos cada fila con la ultima ventana registrada, y detectar datos anomalos
    for idx, row in pdf.iterrows():
        pc_id = row["pc_id"]

        # Seleccionamos la ultima ventana para el pc_id
        cursor.execute("""
            SELECT avg_cpu, stddev_cpu, packets_avg, stddev_packets FROM promedioPorVentana
            WHERE pc_id = ?
            ORDER BY window_end DESC LIMIT 1
        """, (pc_id,))
        last_window = cursor.fetchone()

        # Comparamos el dato con la media mas 3 desviaciones estandar
        # 1% fuera de la norma
        avg_cpu_last, stddev_cpu_last, packets_avg_last, stddev_packets_last = (None, None, None, None)
        if last_window:
            avg_cpu_last, stddev_cpu_last, packets_avg_last, stddev_packets_last = last_window
        if avg_cpu_last is not None and stddev_cpu_last is not None:
            if row["cpu_usage"] > avg_cpu_last + 3*stddev_cpu_last:
                print(f"Anomalia detectada en el uso de cpu del pc {pc_id}:\n\tuso del cpu %{row['cpu_usage']}")
        if packets_avg_last is not None and stddev_packets_last is not None:
            if row["packets_per_second"] > packets_avg_last + 3*stddev_packets_last:
                print(f"Anomalia detectada en el uso de red del pc {pc_id}:\n\tpaquetes enviados por seguno {row['packets_per_second']}")

    print("Datos Ingesta: ", rows)
    with conn:
        conn.executemany(
            "INSERT INTO crudo (pc_id, cpu_usage, ram_usage, packets_per_second, cpu_usage_category, time) VALUES (?, ?, ?, ?, ?, ?)",
            rows
        )
    conn.close()
    print(f"Batch {batch_id}: insertadas {len(rows)} filas en SQLite")
    print("\n")

def foreach_batch_ventana(batch_df, batch_id):
    # Si no hay filas, salimos rápido
    if batch_df.rdd.isEmpty():
        return

    # Separamos el dato ventana
    window_df= batch_df.withColumn("window_start", F.col("window.start").cast("string")) \
        .withColumn("window_end", F.col("window.end").cast("string")) \
        .drop("window")

    # Transformamos a pandas
    pdf = window_df.toPandas()

    # Conectamos con la base de datos
    conn = sqlite3.connect(sqlite_db_path)
    rows = [tuple(x) for x in pdf[["pc_id", "window_start", "window_end", "avg_cpu", "stddev_cpu", "avg_ram", "packets_avg", "stddev_packets", "event_count"]].to_numpy()]

    print("Datos Ventana: ", rows)
    with conn:
        conn.executemany(
            "INSERT INTO promedioPorVentana (pc_id, window_start, window_end, avg_cpu, stddev_cpu, avg_ram, packets_avg, stddev_packets, event_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )

# Limpiamos la terminal
print('\x1b[2J\x1b[H')

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

query_ventana.awaitTermination()
query.awaitTermination()
