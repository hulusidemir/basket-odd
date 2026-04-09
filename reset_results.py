import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "basketball.db")

def reset_all_results():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Update all alerts to have empty results
    cursor.execute("UPDATE alerts SET result = '', final_score = '' WHERE result != ''")
    rowcount = cursor.rowcount
    conn.commit()
    conn.close()
    
    print("="*60)
    print(f"✅ {rowcount} adet maçın mevcut sonucu başarıyla sıfırlandı.")
    print("Artık 'Sonuçları Kontrol Et' butonuna basarak tüm maçların sonuçlarını en güncel algoritmayla tamamen hatasız şekilde tekrar çekebilirsiniz.")
    print("="*60)

if __name__ == "__main__":
    confirm = input("Bu işlem, veritabanındaki TÜM bitmiş maçların kayıtlı skorlarını sıfırlayarak onları 'Bekleyen' duruma geri alacaktır. Devam edilsin mi? (e/h): ")
    if confirm.lower() == 'e':
        reset_all_results()
    else:
        print("İşlem iptal edildi.")
