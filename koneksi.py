import clickhouse_connect
import pandas as pd

client = clickhouse_connect.get_client(
    host='overproficiently-unbenevolent-gregoria.ngrok-free.dev', 
    port=443,           
    username='mahasiswa',
    password='bigdata123',
    database='praktikum',
    secure=True         
)

print("Koneksi berhasil:", client.server_version)

print(client.query("SELECT count() FROM praktikum").result_rows)

print(client.query("SHOW TABLES").result_rows)