# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR
#  Direplikasi dan dikembangkan berdasarkan v3.4 Final
#  Update oleh Gemini @ 2025-10-04
# ===================================================================================

# --- Impor Pustaka/Library ---
import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
from thefuzz import process, fuzz
import re
import plotly.express as px
import time
from typing import List, Dict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# --- Konfigurasi Halaman Utama ---
st.set_page_config(
    layout="wide",
    page_title="Dashboard Analisis Kompetitor"
)

# =====================================================================================
# BLOK KONFIGURASI UTAMA
# =====================================================================================

# --- ID Google Drive & Sheets ---
PARENT_FOLDER_ID = "1rnx2fExmZi_AcldKvWb6_xv0ipj4WRbZ"
DATA_MENTAH_FOLDER_NAME = "DATA MENTAH (DIUPLOAD DI SINI)"
DATA_OLAHAN_FOLDER_NAME = "DATA OLAHAN (JANGAN DIEDIT)"
SPREADSHEET_ID = "1GRfT9sV5W2xN55Jod1d9nKE2mANb29m1i5clV3fSTv0"

# --- Nama Worksheet di Google Sheets ---
DB_SHEET_NAME = "DATABASE"
DB_BRAND_SHEET_NAME = "DATABASE_BRAND"
KAMUS_BRAND_SHEET_NAME = "kamus_brand"
HASIL_MATCHING_SHEET_NAME = "HASIL_MATCHING"

# --- Cache & State Management ---
# Nama file untuk menyimpan cache data yang sudah diproses
CACHE_FILE_NAME = "processed_data_cache.pkl"

# --- Inisialisasi Session State ---
# 'session_state' adalah cara Streamlit untuk mengingat variabel antar interaksi
if 'mode' not in st.session_state:
    st.session_state.mode = "initial" # Mode awal aplikasi
if 'master_df' not in st.session_state:
    st.session_state.master_df = pd.DataFrame() # DataFrame utama
if 'db_df' not in st.session_state:
    st.session_state.db_df = pd.DataFrame() # DataFrame DATABASE
if 'tfidf_vectorizer' not in st.session_state:
    st.session_state.tfidf_vectorizer = None # Untuk model TF-IDF
if 'tfidf_matrix' not in st.session_state:
    st.session_state.tfidf_matrix = None # Matriks kemiripan produk

# ===================================================================================
# BLOK FUNGSI-FUNGSI BANTU
# Kumpulan fungsi untuk otentikasi, interaksi API, pembersihan, dan pemrosesan data.
# ===================================================================================

# --- Fungsi Otentikasi & Koneksi API ---
@st.cache_resource
def get_gspread_client():
    """Membuat dan mengembalikan klien gspread untuk berinteraksi dengan Google Sheets."""
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict)
    scoped_creds = creds.with_scopes([
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(scoped_creds)

@st.cache_resource
def get_drive_service():
    """Membuat dan mengembalikan service object untuk Google Drive API."""
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict)
    return build('drive', 'v3', credentials=creds)

# --- Fungsi Interaksi Google Drive & Sheets ---
def get_sheet_as_df(sheet_name: str) -> pd.DataFrame:
    """Mengambil data dari worksheet tertentu dan mengubahnya menjadi DataFrame."""
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Error: Worksheet '{sheet_name}' tidak ditemukan di Google Sheets.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal mengambil data dari sheet '{sheet_name}': {e}")
        return pd.DataFrame()

def find_folder_id(service, parent_id: str, folder_name: str) -> str:
    """Mencari ID sebuah folder di dalam folder induk berdasarkan namanya."""
    query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])
    return items[0]['id'] if items else None

def list_files_in_folder(service, folder_id: str) -> List[Dict]:
    """Mendapatkan daftar file (CSV) dari dalam sebuah folder."""
    if not folder_id: return []
    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get('files', [])

def download_and_read_csv(service, file_id: str) -> pd.DataFrame:
    """Mengunduh file CSV dari Drive dan membacanya sebagai DataFrame."""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    try:
        return pd.read_csv(fh)
    except Exception as e:
        st.warning(f"Gagal membaca file CSV (ID: {file_id}). Mungkin file kosong atau format salah. Error: {e}")
        return pd.DataFrame()

# --- Fungsi Pembersihan & Transformasi Data ---
def normalize_brand(name: str, brand_dict: Dict[str, str], brand_set: set) -> str:
    """Membersihkan dan menstandarkan nama brand."""
    if not isinstance(name, str) or not name:
        return "TIDAK ADA BRAND"
    name_lower = name.strip().lower()
    if name_lower in brand_dict:
        return brand_dict[name_lower]
    
    best_match = process.extractOne(name_lower, brand_set, scorer=fuzz.token_set_ratio)
    return best_match[0] if best_match and best_match[1] > 85 else name.strip().upper()

def format_rupiah(amount: float) -> str:
    """Mengubah angka menjadi format mata uang Rupiah."""
    if pd.isna(amount): return "Rp 0"
    return f"Rp {amount:,.0f}".replace(",", ".")

# --- PERUBAIKAN 1 (Revisi): Fungsi Pelabelan Cerdas untuk DB KLIK menggunakan TF-IDF ---
def smart_label_dbklik(df_dbklik: pd.DataFrame, df_database: pd.DataFrame) -> pd.DataFrame:
    """
    Melabeli SKU dan KATEGORI untuk produk DB KLIK menggunakan TF-IDF dan Cosine Similarity.
    Metode ini menganalisis kemiripan teks berdasarkan bobot kata untuk akurasi yang lebih tinggi
    dan menggunakan data produk yang paling mirip dari database.
    """
    st.write("Memulai pelabelan cerdas (TF-IDF) untuk produk DB KLIK...")
    if 'NAMA' not in df_database.columns or df_database.empty:
        st.warning("Worksheet DATABASE kosong atau tidak memiliki kolom 'NAMA'. Pelabelan dilewati.")
        df_dbklik['SKU'] = "DB_INVALID"
        df_dbklik['KATEGORI'] = "DB_INVALID"
        return df_dbklik

    # 1. Normalisasi nama produk di kedua DataFrame
    db_database_normalized = df_database['NAMA'].apply(normalize_text_for_similarity)
    db_dbklik_normalized = df_dbklik['NAMA'].apply(normalize_text_for_similarity)

    # 2. Buat model TF-IDF yang dioptimalkan
    vectorizer = TfidfVectorizer(
        max_features=5000,  # Batasi jumlah kata yang dianalisis ke 5000 kata paling penting
        min_df=2,           # Abaikan kata yang hanya muncul di 1 nama produk (mengurangi noise)
        ngram_range=(1, 2)  # Analisis kata tunggal dan frasa 2 kata (misal: "core i5")
    )
    
    tfidf_matrix_database = vectorizer.fit_transform(db_database_normalized)
    tfidf_matrix_dbklik = vectorizer.transform(db_dbklik_normalized)

    # 3. Hitung cosine similarity
    cosine_similarities = cosine_similarity(tfidf_matrix_dbklik, tfidf_matrix_database)

    # 4. Cari kecocokan terbaik
    best_match_indices = np.argmax(cosine_similarities, axis=1)
    best_match_scores = np.max(cosine_similarities, axis=1)

    matched_skus = df_database['SKU'].iloc[best_match_indices].values
    matched_kategoris = df_database['KATEGORI'].iloc[best_match_indices].values

    # 5. Terapkan logika berdasarkan skor kemiripan
    similarity_threshold = 0.3 
    final_skus = np.where(best_match_scores >= similarity_threshold, matched_skus, "LOW_CONFIDENCE_MATCH")
    final_kategoris = np.where(best_match_scores >= similarity_threshold, matched_kategoris, "LOW_CONFIDENCE_MATCH")

    df_dbklik['SKU'] = final_skus
    df_dbklik['KATEGORI'] = final_kategoris
    
    st.write("✔️ Pelabelan cerdas DB KLIK (TF-IDF) selesai.")
    return df_dbklik
    
# --- PERUBAIKAN 2 (Kinerja): Fungsi Normalisasi Teks untuk TF-IDF ---
def normalize_text_for_similarity(text: str) -> str:
    """Membersihkan dan menstandarkan nama produk untuk analisis kemiripan."""
    if not isinstance(text, str): return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text) # Hapus karakter non-alfanumerik
    # Standarkan satuan
    text = re.sub(r'(\d+)\s*inch|\"', r'\1inch', text)
    text = re.sub(r'(\d+)\s*gb', r'\1gb', text)
    text = re.sub(r'(\d+)\s*tb', r'\1tb', text)
    text = re.sub(r'(\d+)\s*hz', r'\1hz', text)
    # Hapus stopwords umum
    stopwords = ['garansi', 'resmi', 'original', 'dan', 'promo', 'murah', 'untuk', 'dengan']
    tokens = text.split()
    tokens = [word for word in tokens if word not in stopwords]
    return ' '.join(tokens)

# --- Fungsi Inti Pemrosesan Data ---
@st.cache_data(ttl=3600) # Cache data selama 1 jam
def process_all_data(status_placeholder):
    """
    Fungsi utama yang menarik semua data dari Google Drive & Sheets,
    membersihkan, menggabungkan, dan memprosesnya untuk analisis.
    """
    drive_service = get_drive_service()
    
    status_placeholder.info("Mengkoneksikan ke Google Drive & Sheets...")
    time.sleep(1)

    # 1. Ambil data dasar dari Google Sheets
    status_placeholder.info("Mengambil data dari worksheet DATABASE, BRAND, dan KAMUS...")
    db_df = get_sheet_as_df(DB_SHEET_NAME)
    db_brand_df = get_sheet_as_df(DB_BRAND_SHEET_NAME)
    kamus_brand_df = get_sheet_as_df(KAMUS_BRAND_SHEET_NAME)
    
    if db_df.empty or db_brand_df.empty or kamus_brand_df.empty:
        status_placeholder.error("Gagal memuat data dasar dari Google Sheets. Proses dihentikan.")
        return pd.DataFrame(), pd.DataFrame(), None, None

    st.session_state.db_df = db_df # Simpan df database ke session state
    
    # 2. Siapkan normalisasi brand
    brand_set = set(db_brand_df['NAMA BRAND'].str.strip().str.upper())
    kamus_brand_df.dropna(subset=['Alias', 'Brand_Utama'], inplace=True)
    brand_dict = {
        row['Alias'].strip().lower(): row['Brand_Utama'].strip().upper()
        for _, row in kamus_brand_df.iterrows()
    }

    # 3. Temukan folder data mentah dan proses file di dalamnya
    status_placeholder.info("Mencari folder data di Google Drive...")
    data_mentah_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_MENTAH_FOLDER_NAME)
    if not data_mentah_folder_id:
        status_placeholder.error(f"Folder '{DATA_MENTAH_FOLDER_NAME}' tidak ditemukan di Google Drive.")
        return pd.DataFrame(), pd.DataFrame(), None, None

    toko_folders = drive_service.files().list(
        q=f"'{data_mentah_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id, name)"
    ).execute().get('files', [])

    if not toko_folders:
        status_placeholder.error(f"Tidak ada folder toko di dalam '{DATA_MENTAH_FOLDER_NAME}'.")
        return pd.DataFrame(), pd.DataFrame(), None, None

    all_dfs = []
    status_placeholder.info(f"Ditemukan {len(toko_folders)} folder toko. Memproses file CSV...")

    for toko_folder in toko_folders:
        toko_name = toko_folder['name'].upper()
        st.write(f"Memproses folder: **{toko_name}**")
        files = list_files_in_folder(drive_service, toko_folder['id'])
        
        for file in files:
            file_name = file['name']
            df = download_and_read_csv(drive_service, file['id'])
            if df.empty:
                st.write(f" L-> File '{file_name}' kosong atau gagal dibaca, dilewati.")
                continue

            # Standarisasi kolom
            df.columns = [col.upper().strip() for col in df.columns]
            if 'NAMA' not in df.columns or 'HARGA' not in df.columns:
                st.warning(f"File '{file_name}' tidak memiliki kolom 'NAMA' atau 'HARGA'. Dilewati.")
                continue
            
            df['TOKO'] = toko_name
            df['STATUS'] = 'READY' if 'ready' in file_name.lower() else 'HABIS'
            
            # Normalisasi kolom 'TERJUAL/BLN'
            if 'TERJUAL/BLN' in df.columns:
                df['TERJUAL/BLN'] = pd.to_numeric(df['TERJUAL/BLN'], errors='coerce').fillna(0).astype(int)
            else:
                df['TERJUAL/BLN'] = 0
            
            df['HARGA'] = pd.to_numeric(df['HARGA'], errors='coerce').fillna(0)
            
            all_dfs.append(df)

    if not all_dfs:
        status_placeholder.error("Tidak ada data valid yang bisa diproses dari semua file CSV.")
        return pd.DataFrame(), pd.DataFrame(), None, None

    # Gabungkan semua data menjadi satu DataFrame besar
    df_gabungan = pd.concat(all_dfs, ignore_index=True)
    
    # PERBAIKAN 1: Pisahkan data DB KLIK untuk pelabelan cerdas
    is_dbklik = df_gabungan['TOKO'] == 'DB KLIK'
    df_dbklik_part = df_gabungan[is_dbklik].copy()
    df_other_part = df_gabungan[~is_dbklik].copy()

    if not df_dbklik_part.empty:
        df_dbklik_processed = smart_label_dbklik(df_dbklik_part, db_df)
        df_gabungan = pd.concat([df_other_part, df_dbklik_processed], ignore_index=True)

    # Normalisasi brand untuk semua data
    status_placeholder.info("Menstandarkan nama brand...")
    df_gabungan['BRAND'] = df_gabungan['BRAND'].apply(lambda x: normalize_brand(x, brand_dict, brand_set))
    
    # Hitung Omzet
    df_gabungan['OMZET'] = df_gabungan['HARGA'] * df_gabungan['TERJUAL/BLN']

    # --- PERUBAIKAN 2 (Kinerja): Pre-computation untuk Similarity Search ---
    status_placeholder.info("Mempersiapkan model analisis kemiripan produk (TF-IDF)...")
    df_gabungan['NAMA_NORMALIZED'] = df_gabungan['NAMA'].apply(normalize_text_for_similarity)
    
    # Buat dan simpan vectorizer & matrix yang dioptimalkan
    tfidf_vectorizer = TfidfVectorizer(
        max_features=10000, # Batasi ke 10000 kata/frasa paling penting dari semua toko
        min_df=2,           # Abaikan kata yang terlalu jarang muncul
        max_df=0.9,         # Abaikan kata yang terlalu umum (muncul di >90% produk)
        ngram_range=(1, 2)  # Analisis kata tunggal dan frasa 2 kata
    )
    tfidf_matrix = tfidf_vectorizer.fit_transform(df_gabungan['NAMA_NORMALIZED'])
    status_placeholder.info("✔️ Model TF-IDF berhasil dibuat.")
    
    return df_gabungan, db_df, tfidf_vectorizer, tfidf_matrix

# --- PERUBAIKAN 3: Fungsi untuk Menampilkan Metrik WoW dengan Warna ---
def display_wow_metric(label, current_value, previous_value):
    """Menampilkan metrik dengan delta pertumbuhan dan warna otomatis."""
    delta_value = 0
    delta_text = "N/A"
    
    # Hindari pembagian dengan nol
    if previous_value > 0:
        delta_value = ((current_value - previous_value) / previous_value) * 100
        delta_text = f"{delta_value:.2f}%"
    elif current_value > 0 and previous_value == 0:
        delta_text = "Baru" # Jika sebelumnya 0 dan sekarang ada
    
    st.metric(label=label, value=f"{current_value:,.0f}", delta=delta_text)

# ===================================================================================
# --- UI UTAMA APLIKASI STREAMLIT ---
# ===================================================================================

# --- Judul Aplikasi ---
st.title("📊 Dashboard Analisis Kompetitor & Penjualan")
st.markdown("Versi 4.1 - Optimasi Kinerja TF-IDF")

# --- Sidebar ---
with st.sidebar:
    st.header("⚙️ Kontrol & Filter")
    
    # Tombol untuk menarik dan memproses data
    if st.button("🚀 Tarik & Proses Data Terbaru", type="primary", use_container_width=True):
        st.session_state.mode = "processing"

    # Jika data sudah ada, tampilkan filter
    if st.session_state.mode == "dashboard" and not st.session_state.master_df.empty:
        st.divider()
        st.subheader("🔍 Similarity Produk")
        st.info("Pilih produk dari toko Anda (DB KLIK) untuk mencari produk serupa di toko kompetitor.")

        # --- PERUBAIKAN 2: Filter berdasarkan Brand ---
        db_klik_df = st.session_state.master_df[st.session_state.master_df['TOKO'] == 'DB KLIK']
        
        # PERBAIKAN 4: Cek jika kolom 'BRAND' ada dan tidak kosong sebelum membuat list
        if 'BRAND' in db_klik_df.columns and not db_klik_df['BRAND'].empty:
            available_brands = sorted(db_klik_df['BRAND'].dropna().unique())
            
            # Tambahkan opsi 'SEMUA BRAND'
            brand_options = ["SEMUA BRAND"] + available_brands
            selected_brand = st.selectbox(
                "Filter berdasarkan Brand:",
                options=brand_options
            )

            # Filter produk berdasarkan brand yang dipilih
            if selected_brand == "SEMUA BRAND":
                product_list = sorted(db_klik_df['NAMA'].unique())
            else:
                product_list = sorted(db_klik_df[db_klik_df['BRAND'] == selected_brand]['NAMA'].unique())
        else:
            product_list = sorted(db_klik_df['NAMA'].unique())


        selected_product = st.selectbox(
            "Pilih Produk DB KLIK:",
            options=product_list,
            index=None,
            placeholder="Ketik untuk mencari produk..."
        )

# --- Logika Pemrosesan Data (Dipicu oleh Tombol) ---
if st.session_state.mode == "processing":
    with st.spinner("Harap tunggu, sedang memproses data... Ini mungkin memakan waktu beberapa menit."):
        status_placeholder = st.empty()
        try:
            processed_df, db_df, vectorizer, matrix = process_all_data(status_placeholder)
            
            if not processed_df.empty:
                st.session_state.master_df = processed_df
                st.session_state.db_df = db_df
                st.session_state.tfidf_vectorizer = vectorizer
                st.session_state.tfidf_matrix = matrix
                st.session_state.mode = "dashboard"
                status_placeholder.success("🎉 Semua data berhasil diproses!")
                time.sleep(2)
                st.rerun()
            else:
                st.session_state.mode = "initial"
                status_placeholder.error("Proses data gagal. Silakan periksa log di atas dan coba lagi.")
        
        except Exception as e:
            st.session_state.mode = "initial"
            st.exception(e)


# --- Logika Tampilan Dashboard (Setelah Data Siap) ---
if st.session_state.mode == "dashboard":
    df_gabungan = st.session_state.master_df
    
    if df_gabungan.empty:
        st.warning("Tidak ada data untuk ditampilkan. Silakan tarik data terlebih dahulu.")
    else:
        # --- PERBAIKAN ERROR TypeError: Inilah baris yang diperbaiki ---
        # Menambahkan .dropna() untuk membuang nilai kosong sebelum mengurutkan
        toko_list = sorted(df_gabungan['TOKO'].dropna().unique())
        
        tab1, tab2, tab3 = st.tabs(["📊 Analisis Umum", "📈 Detail Per Toko", "📋 Data Mentah Gabungan"])

        with tab1:
            st.header("Ringkasan Umum Semua Toko")
            
            # --- PERUBAIKAN 3: Analisis Pertumbuhan WoW dengan Warna ---
            st.subheader("Analisis Pertumbuhan Week-over-Week (WoW)")
            
            # Asumsi: Data memiliki kolom TANGGAL
            if 'TANGGAL' in df_gabungan.columns:
                df_gabungan['TANGGAL'] = pd.to_datetime(df_gabungan['TANGGAL'], errors='coerce')
                df_w = df_gabungan.dropna(subset=['TANGGAL']).copy()
                
                if not df_w.empty:
                    latest_date = df_w['TANGGAL'].max()
                    current_week_start = latest_date - pd.to_timedelta(6, unit='d')
                    previous_week_start = latest_date - pd.to_timedelta(13, unit='d')

                    current_week_df = df_w[(df_w['TANGGAL'] >= current_week_start) & (df_w['TANGGAL'] <= latest_date)]
                    previous_week_df = df_w[(df_w['TANGGAL'] >= previous_week_start) & (df_w['TANGGAL'] < current_week_start)]

                    # Hitung metrik
                    cw_omzet = current_week_df['OMZET'].sum()
                    pw_omzet = previous_week_df['OMZET'].sum()
                    cw_terjual = current_week_df['TERJUAL/BLN'].sum()
                    pw_terjual = previous_week_df['TERJUAL/BLN'].sum()
                    cw_sku_aktif = current_week_df[current_week_df['STATUS'] == 'READY']['NAMA'].nunique()
                    pw_sku_aktif = previous_week_df[previous_week_df['STATUS'] == 'READY']['NAMA'].nunique()

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        display_wow_metric("Total Omzet (Minggu Ini)", cw_omzet, pw_omzet)
                    with col2:
                        display_wow_metric("Total Produk Terjual (Minggu Ini)", cw_terjual, pw_terjual)
                    with col3:
                        display_wow_metric("Jumlah SKU Aktif (Minggu Ini)", cw_sku_aktif, pw_sku_aktif)
                else:
                    st.info("Tidak cukup data tanggal untuk analisis WoW.")
            else:
                st.warning("Kolom 'TANGGAL' tidak ditemukan untuk analisis WoW.")

            st.divider()
            
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Top 10 Produk Terlaris (Berdasarkan Omzet)")
                top_produk = df_gabungan.groupby('NAMA')['OMZET'].sum().nlargest(10).reset_index()
                fig = px.bar(top_produk, x='OMZET', y='NAMA', orientation='h', title="Top 10 Produk by Omzet")
                fig.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Distribusi Omzet per Toko")
                omzet_toko = df_gabungan.groupby('TOKO')['OMZET'].sum().reset_index()
                fig = px.pie(omzet_toko, values='OMZET', names='TOKO', title="Persentase Omzet per Toko")
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            st.header("Analisis Mendalam per Toko")
            selected_toko = st.selectbox("Pilih Toko untuk dianalisis:", options=toko_list)
            
            if selected_toko:
                df_toko = df_gabungan[df_gabungan['TOKO'] == selected_toko]
                st.dataframe(df_toko.sort_values(by="OMZET", ascending=False).head(20), use_container_width=True)

        with tab3:
            st.header("Tabel Data Gabungan")
            st.info("Berikut adalah seluruh data yang telah dibersihkan dan digabungkan dari semua toko.")
            st.dataframe(df_gabungan, use_container_width=True)
            
    # --- Logika untuk menampilkan hasil Similarity Search ---
    if 'selected_product' in locals() and selected_product:
        st.sidebar.divider()
        with st.sidebar:
            with st.spinner("Mencari produk serupa..."):
                try:
                    # 1. Cari index produk yang dipilih
                    product_idx = df_gabungan[df_gabungan['NAMA'] == selected_product].index[0]
                    
                    # 2. Hitung cosine similarity (sangat cepat)
                    cosine_sims = cosine_similarity(st.session_state.tfidf_matrix[product_idx], st.session_state.tfidf_matrix).flatten()
                    
                    # 3. Dapatkan 10 produk paling mirip
                    related_docs_indices = cosine_sims.argsort()[:-11:-1]
                    
                    results = []
                    for i in related_docs_indices:
                        # Jangan tampilkan produk itu sendiri
                        if i == product_idx:
                            continue
                        
                        # Hanya tampilkan produk dari toko lain
                        if df_gabungan.iloc[i]['TOKO'] != 'DB KLIK':
                            results.append({
                                'TOKO': df_gabungan.iloc[i]['TOKO'],
                                'NAMA PRODUK': df_gabungan.iloc[i]['NAMA'],
                                'HARGA': df_gabungan.iloc[i]['HARGA'],
                                'STATUS': df_gabungan.iloc[i]['STATUS'],
                                'OMZET': df_gabungan.iloc[i]['OMZET'],
                                'TERJUAL PER BULAN': df_gabungan.iloc[i]['TERJUAL/BLN'],
                                'SKOR': cosine_sims[i] * 100
                            })
                    
                    if results:
                        # --- PERUBAIKAN 2: Tampilan Tabel Similarity ---
                        st.subheader("Hasil Perbandingan Produk")
                        result_df = pd.DataFrame(results)
                        
                        # Formatting
                        result_df['HARGA'] = result_df['HARGA'].apply(format_rupiah)
                        result_df['OMZET'] = result_df['OMZET'].apply(format_rupiah)
                        result_df['SKOR'] = result_df['SKOR'].apply(lambda x: f"{x:.2f}%")
                        
                        # Tampilkan kolom sesuai permintaan
                        st.dataframe(
                            result_df[['TOKO', 'NAMA PRODUK', 'HARGA', 'STATUS', 'OMZET', 'TERJUAL PER BULAN', 'SKOR']],
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.sidebar.warning("Tidak ditemukan produk yang cukup mirip di toko kompetitor.")

                except IndexError:
                    st.sidebar.error("Produk yang dipilih tidak ditemukan dalam data. Coba proses ulang data.")
                except Exception as ex:
                    st.sidebar.error(f"Terjadi error: {ex}")

# --- Tampilan Awal Aplikasi ---
if st.session_state.mode == "initial":
    st.info("👈 Silakan klik tombol **'Tarik & Proses Data Terbaru'** di sidebar untuk memulai analisis.")
    st.image("https://storage.googleapis.com/gweb-cloudblog-publish/images/Google_Drive_logo.max-2200x2200.png", width=150)

