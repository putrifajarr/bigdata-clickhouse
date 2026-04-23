# Konfigurasi Koneksi ke clickhouse melalui ngrok
client = clickhouse_connect.get_client(
    host='overproficiently-unbenevolent-gregoria.ngrok-free.dev', 
    port=443,           
    username='mahasiswa',
    password='bigdata123',
    database='praktikum',
    secure=True         
)