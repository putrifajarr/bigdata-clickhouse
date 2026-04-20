import clickhouse_connect
import pandas as pd
import time
import numpy as np

# Konfigurasi Koneksi
client = clickhouse_connect.get_client(
    host='overproficiently-unbenevolent-gregoria.ngrok-free.dev', 
    port=443,           
    username='mahasiswa',
    password='bigdata123',
    database='praktikum',
    secure=True         
)

csv_file = 'scrape_merged.csv'
table_name = 'scrape_data'
chunk_size = 5000  

try:
    # 1. Cek berapa banyak data yang sudah masuk
    print("Mengecek jumlah data di database...")
    res = client.query(f"SELECT count() FROM {table_name}")
    start_from = res.result_rows[0][0]
    
    print(f"Data ditemukan: {start_from} baris.")
    print(f"Melanjutkan import dari baris ke-{start_from + 1}...")

    # 2. Baca CSV dengan melewati (skiprows) baris yang sudah ada
    # Kita lewati 'start_from' baris pertama data (header tetap dibaca karena skiprows=range(1, ...))
    reader = pd.read_csv(
        csv_file, 
        skiprows=range(1, start_from + 1), 
        chunksize=chunk_size
    )

    start_time = time.time()
    total_rows = start_from

    for chunk in reader:
        # --- Preprocessing per Tipe Data ---
        str_cols = ['ticker', 'url', 'domain', 'source_common_name', 'title', 'text', 'language', 'content_hash', 'error', 'cik_str', 'company_name']
        for col in str_cols:
            if col in chunk.columns:
                chunk[col] = chunk[col].fillna('').astype(str)

        int_cols = ['priority', 'http_status', 'word_count']
        for col in int_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0).astype(np.int32)

        if 'day' in chunk.columns:
            chunk['day'] = pd.to_datetime(chunk['day'], errors='coerce').dt.date
        if 'ts' in chunk.columns:
            chunk['ts'] = pd.to_datetime(chunk['ts'], errors='coerce')
        if 'scraped_at' in chunk.columns:
            chunk['scraped_at'] = pd.to_datetime(chunk['scraped_at'], errors='coerce')

        if 'scrapeable' in chunk.columns:
            chunk['scrapeable'] = chunk['scrapeable'].fillna(False).astype(bool)

        # Insert ke ClickHouse
        client.insert_df(table=table_name, df=chunk)
        
        total_rows += len(chunk)
        elapsed = time.time() - start_time
        print(f"Berhasil mengunggah total {total_rows} baris... (Kecepatan: {len(chunk)/ (time.time() - start_time + 0.1):.0f} rps)")

    print(f"\nSelesai! Total {total_rows} baris berhasil diimport.")

except Exception as e:
    print(f"Terjadi kesalahan saat import: {e}")
    if 'chunk' in locals():
        print("\nTipe data pada chunk terakhir:")
        print(chunk.dtypes)

finally:
    client.close()
