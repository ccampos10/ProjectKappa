import sqlite3
import pandas as pd

conn = sqlite3.connect("./data/spark_output.db")
df = pd.read_sql_query("SELECT * FROM crudo ORDER BY id LIMIT 20", conn)
print(df)
ventana_df = pd.read_sql_query("SELECT * FROM promedioPorVentana LIMIT 20", conn)
print(ventana_df)
conn.close()
