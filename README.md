---
title: Dashboard Analisis Penjualan
emoji: 📊
colorFrom: blue
colorTo: green
sdk: streamlit
main: app.py
---

# Dashboard Analisis Penjualan & Kompetitor Shopee

Aplikasi Streamlit untuk memantau kinerja penjualan toko utama (`DB KLIK`), membandingkan performa brand terhadap kompetitor, memantau tren stok, mencocokkan produk lintas toko dengan TF-IDF, dan menganalisis margin terhadap HPP.

## Menjalankan Secara Lokal

1. Pastikan dependensi terpasang:
   ```bash
   pip install -r requirements.txt
   ```
2. Jalankan aplikasi (otomatis membaca `DATA_REKAP.xlsx` atau Google Sheets jika `.streamlit/secrets.toml` terkonfigurasi):
   ```bash
   streamlit run app.py
   ```
