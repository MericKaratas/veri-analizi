import pandas as pd
import mysql.connector

# 1. CSV'den veri oku
df = pd.read_csv("C:/Users/tuna_/OneDrive/Masaüstü/verianaliz/data/son3.csv")

# 2. Boş değerleri None olarak ayarla
df = df.where(pd.notnull(df), None)
df = df.astype(object)

# 3. Tekrarlayan id'leri temizle (aynı PRIMARY KEY'ler varsa hata alma)
df = df.drop_duplicates(subset=['id'], keep='first')

# 4. Tarih sütunlarını datetime string'e dönüştür (MySQL uyumlu format)
datetime_cols = ["Açılış Tarihi", "Son Güncelleme", "Çözümlenme tarihi", "Kapanış tarihi"]
for col in datetime_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)

numeric_cols = ["Kimlik", "Çözülme süresi aşıldı"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)


# 5. Tamsayı (BIGINT, INT) gibi sütunlardaki float hatalarını engelle
int_cols = [
    'time_to_resolve', 'time_to_own', 'sla_waiting_duration',
    'ola_waiting_duration', 'internal_time_to_resolve',
    'internal_time_to_own', 'waiting_duration',
    'actiontime', 'close_delay_stat', 'solve_delay_stat',
    'takeintoaccount_delay_stat'
]

for col in int_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].apply(lambda x: int(x) if pd.notnull(x) else None)

        # 3. String (metin) olması gereken sütunlar
string_cols = [
    "Başlık", "Birim", "Durum", "Açıklama",
    "Çözülme süresi + İlerleme", "İstekte bulunan - İstekte bulunan",
    "Atananlar - Teknisyen"
]

for col in string_cols:
    if col in df.columns:
        df[col] = df[col].astype(str)
        df[col] = df[col].apply(lambda x: x.strip() if pd.notnull(x) else None)
        df[col] = df[col].replace("nan", None)


# 6. MySQL bağlantısı
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="M25d16t21.",  # kendi şifreni buraya yaz
    database="son3"
)
cursor = conn.cursor()

# 7. SQL sorgusu (INSERT IGNORE: id varsa atla)
sql = """
INSERT IGNORE INTO tickets (
   `Kimlik`, `Başlık`, `Birim`, `Durum`, `Açıklama`, `Açılış Tarihi`, `Çözülme süresi + İlerleme`,
   `Çözülme süresi aşıldı`, `İstekte bulunan - İstekte bulunan`, `Atananlar - Teknisyen`,
   `Çözümlenme tarihi`, `Kapanış tarihi`, `Son Güncelleme`
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# 7. Satır satır ekle
for index, row in df.iterrows():
    try:
        values = [
            row.get('Kimlik'),
            row.get('Başlık'),
            row.get('Birim'),
            row.get('Durum'),
            row.get('Açıklama'),
            row.get('Açılış Tarihi'),
            row.get('Çözülme süresi + İlerleme'),
            row.get('Çözülme süresi aşıldı'),
            row.get('İstekte bulunan - İstekte bulunan'),
            row.get('Atananlar - Teknisyen'),
            row.get('Çözümlenme tarihi'),
            row.get('Kapanış tarihi'),
            row.get('Son Güncelleme')
        ]
        cursor.execute(sql, tuple(values))
    except Exception as e:
        print(f"Hata (satır {index}): {e}")

# 8. Kaydet ve kapat
conn.commit()
cursor.close()
conn.close()

print("🎉 Tüm son3 verileri başarıyla aktarıldı.")
print("Toplam aktarılan kayıt sayısı:", len(df))
