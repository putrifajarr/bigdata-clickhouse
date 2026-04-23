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

# 2. Tembak perintah SQL dari Putri (USE praktikum & SHOW TABLES)
try:
    print("Mencoba terhubung ke server...\n")
    
    # 2. Pengecekan 1: Status Koneksi
    print(" Koneksi berhasil! Versi Server ClickHouse:", client.server_version)

    # 3. Pengecekan 2: Lihat daftar tabel
    print("\n Mengecek daftar tabel di database 'praktikum'...\n")
    
    # Langsung jalankan query SHOW TABLES dari database praktikum
    hasil = client.query("SHOW TABLES FROM praktikum")
    
    print("=== DAFTAR TABEL ===")
    for row in hasil.result_rows:
        print(f"- {row[0]}")

    # 4. Pengecekan 3: Hitung jumlah data (Sudah dikoreksi ke tabel scrape_data)
    print("\n Menghitung jumlah data yang sudah di-import pada tabel news_scrape_data_...")
    jumlah_data = client.query("SELECT count() FROM news_scrape_data_v2").result_rows
    # result_rows bentuknya list dalam list, misal: [(1000,)], jadi kita ambil indeks [0][0]
    print(f"Total baris data saat ini: {jumlah_data[0][0]} baris")
        
except Exception as e:
    print(f"Gagal mengecek: {e}")
finally:
    client.close()
