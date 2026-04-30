import clickhouse_connect
import pandas as pd

# koneksi
client = clickhouse_connect.get_client(
    host='overproficiently-unbenevolent-gregoria.ngrok-free.dev',
    port=443,
    username='mahasiswa',
    password='bigdata123',
    database='taxi_db',
    secure=True
)

print("=== 2.1 Statistik Dasar ===")
# UBAH: Menggunakan query_df agar otomatis menjadi Pandas DataFrame
df_1 = client.query_df(""" 
SELECT count() AS total_rows,
       avg(trip_distance) AS avg_distance
FROM taxi_db.yellow_trips
WHERE fare_amount > 0 AND trip_distance > 0
""")
# Langsung di-Transpose (.T) tanpa tambahan fungsi aneh-aneh
print(df_1.T)


print("\n=== 2.2 Payment ===")
df_2 = client.query_df(""" 
SELECT payment_type, count()
FROM taxi_db.yellow_trips
GROUP BY payment_type
""")
print(df_2)


print("\n=== 2.3 Per Jam ===")
df_3 = client.query_df(""" 
SELECT toHour(tpep_pickup_datetime), count()
FROM taxi_db.yellow_trips
GROUP BY toHour(tpep_pickup_datetime)
""")
print(df_3)


print("\n=== 2.4 Data Issues ===")
df_4 = client.query_df(""" 
SELECT countIf(fare_amount > 500)
FROM taxi_db.yellow_trips
""")
print(df_4)