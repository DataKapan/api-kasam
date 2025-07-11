import os
import psycopg2
import json
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Veritabanı bağlantısı kuran fonksiyon
def get_db_connection():
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
    return conn

# Scraper'dan gelen veriyi kabul edecek olan API adresi
@app.route('/api/v1/update-proposals', methods=['POST'])
def update_proposals():
    # Güvenlik: Scraper'dan gelen gizli anahtarı kontrol et
    sent_api_key = request.headers.get('X-API-KEY')
    server_api_key = os.environ.get('SERVER_API_KEY')
    if not sent_api_key or sent_api_key != server_api_key:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json()
    if not data or 'proposals' not in data:
        return jsonify({"error": "Missing proposals data"}), 400

    proposals = data['proposals']
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Hızlı kontrol için mevcut tüm tekliflerin esas numaralarını al
        cur.execute("SELECT esas_no FROM proposals")
        existing_esas_nos = {row[0] for row in cur.fetchall()}
        
        new_count = 0
        updated_count = 0

        for proposal in proposals:
            esas_no = proposal.get('esas_no')
            if not esas_no:
                continue

            if esas_no not in existing_esas_nos:
                # YENİ KAYIT: Veritabanına ekle
                new_count += 1
                cur.execute(
                    """
                    INSERT INTO proposals (donem_yasama, esas_no, tarih, milletvekili_veya_kurum, ozet, durum, linkler)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        proposal.get('donem_yasama'), esas_no, proposal.get('tarih'),
                        proposal.get('milletvekili_veya_kurum'), proposal.get('ozet'),
                        proposal.get('durum'), json.dumps(proposal.get('linkler', []))
                    )
                )
            else:
                # MEVCUT KAYIT: Sadece durumu farklıysa güncelle
                updated_count += 1
                cur.execute(
                    "UPDATE proposals SET durum = %s WHERE esas_no = %s AND durum IS DISTINCT FROM %s",
                    (proposal.get('durum'), esas_no, proposal.get('durum'))
                )
        
        conn.commit()
        return jsonify({
            "message": "Data processed successfully",
            "new_proposals": new_count,
            "updated_proposals": updated_count
        }), 200

    except Exception as error:
        if conn:
            conn.rollback()
        print(f"Database error: {error}")
        return jsonify({"error": "Database transaction failed"}), 500
    finally:
        if conn:
            cur.close()
            conn.close()

# Veritabanı tablosunu oluşturmak için basit bir komut
@app.route('/setup-database')
def setup_database():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS proposals (
                id SERIAL PRIMARY KEY,
                donem_yasama TEXT,
                esas_no TEXT UNIQUE NOT NULL,
                tarih TEXT,
                milletvekili_veya_kurum TEXT,
                ozet TEXT,
                durum TEXT,
                linkler JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        return "Database table 'proposals' is ready.", 200
    except Exception as error:
        return f"Error creating table: {error}", 500
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
