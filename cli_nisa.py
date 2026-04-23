import clickhouse_connect

print("Menyambungkan ke Server...")

try:
    # Koneksi ke ngrok Pyta
    client = clickhouse_connect.get_client(
        host='overproficiently-unbenevolent-gregoria.ngrok-free.dev', 
        port=443,           
        username='mahasiswa',
        password='bigdata123',
        database='praktikum',
        secure=True         
    )
    
    print("✅ Berhasil masuk! Ketik 'exit' atau 'quit' untuk keluar.\n")
    
    # --- TAMBAHAN BARU: Cek otomatis total baris ---
    try:
        print(" Sedang mengambil info tabel news_scrape_data_v2...")
        hasil_count = client.query("SELECT count() FROM news_scrape_data_v2")
        total_baris = hasil_count.result_rows[0][0]
        # Format angka agar ada pemisah ribuan (titik) biar gampang dibaca
        print(f"Total baris saat ini: {total_baris:,}".replace(',', '.'))
        print("-" * 40)
    except Exception as e:
        print(f"⚠️ Gagal mengecek baris (mungkin tabel belum ada): {e}")
        print("-" * 40)
    # -----------------------------------------------
    
    # Ini yang bikin programmu bertingkah kayak CLI
    while True:
        # Bikin prompt khas ClickHouse :)
        query = input("praktikum :) ")
        
        # Kalau mau keluar
        if query.strip().lower() in ['exit', 'quit']:
            print("Sampai jumpa!")
            break
            
        # Kalau cuma tekan Enter, abaikan
        if not query.strip():
            continue
            
        try:
            # Tembak query ke server Pyta
            hasil = client.query(query)
            
            # Tampilkan hasil persis kayak di terminal CLI
            if hasil.result_rows:
                for row in hasil.result_rows:
                    # Gabungkan tiap kolom dengan pemisah tab agar rapi
                    print("\t".join(map(str, row)))
            else:
                print("Ok.") # Kalau query berhasil tapi gak ada data (misal ALTER/CREATE)
                
        except Exception as e:
            # Kalau Nisa salah ketik SQL, tampilkan errornya
            print(f"❌ Error: {e}")

except Exception as e:
    print(f"Gagal koneksi: {e}")
finally:
    if 'client' in locals():
        client.close()