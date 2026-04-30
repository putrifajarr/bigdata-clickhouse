import clickhouse_connect

print("Menyambungkan ke Server...")

try:
    # Koneksi ke ngrok server temanmu
    client = clickhouse_connect.get_client(
        host='overproficiently-unbenevolent-gregoria.ngrok-free.dev', 
        port=443,           
        username='mahasiswa',
        password='bigdata123',
        database='taxi_db', # 🔴 UBAH 1: Nama database diganti jadi taxi_db
        secure=True         
    )
    
    print("✅ Berhasil masuk! Ketik 'exit' atau 'quit' untuk keluar.\n")
    
    # --- Cek otomatis total baris ---
    try:
        print(" Sedang mengambil info tabel yellow_trips...")
        # 🔴 UBAH 2: Nama tabel diganti jadi yellow_trips
        hasil_count = client.query("SELECT count() FROM yellow_trips")
        total_baris = hasil_count.result_rows[0][0]
        # Format angka agar ada pemisah ribuan (titik)
        print(f"Total baris saat ini: {total_baris:,}".replace(',', '.'))
        print("-" * 40)
    except Exception as e:
        print(f"⚠️ Gagal mengecek baris (mungkin tabel belum ada): {e}")
        print("-" * 40)
    # -----------------------------------------------
    
    while True:
        # 🔴 UBAH 3: Ubah teks prompt biar sesuai dengan database
        query = input("taxi_db :) ")
        
        if query.strip().lower() in ['exit', 'quit']:
            print("Sampai jumpa!")
            break
            
        if not query.strip():
            continue
            
        try:
            hasil = client.query(query)
            
            if hasil.result_rows:
                for row in hasil.result_rows:
                    print("\t".join(map(str, row)))
            else:
                print("Ok.") 
                
        except Exception as e:
            # Kalau kamu salah ketik SQL, tampilkan errornya
            print(f"❌ Error: {e}")

except Exception as e:
    print(f"Gagal koneksi: {e}")
finally:
    if 'client' in locals():
        client.close()