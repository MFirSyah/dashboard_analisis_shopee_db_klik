# ======================================================================================
# Bagian 1: Inisialisasi & Koneksi
# ======================================================================================

# --- Impor Library ---
# Library inti untuk aplikasi
import streamlit as st
import pandas as pd
from pathlib import Path

# Library untuk koneksi ke Google API (Sheets & Drive)
import gspread
from google.oauth2.service_account import Credentials
# ======================================================================================

# --- Impor Library Tambahan untuk Bagian 2 ---
# Pastikan Anda sudah menginstalnya: pip install thefuzz google-api-python-client
from thefuzz import process
from googleapiclient.discovery import build
import io

# --- Impor Library Tambahan untuk Bagian 3 ---
import plotly.express as px

# --- Konfigurasi Halaman Streamlit ---
# Mengatur judul tab, ikon, dan layout halaman. 
# Ini sebaiknya menjadi perintah st pertama yang dijalankan.
st.set_page_config(
    page_title="Dasbor Analisis Kompetitor",
    page_icon="📊",
    layout="wide"
)

# --- Koneksi ke Google API ---
# Menggunakan decorator @st.cache_resource agar koneksi tidak dibuat berulang kali
# setiap kali ada interaksi di aplikasi. Ini lebih efisien.
@st.cache_resource
def connect_to_google():
    """
    Fungsi untuk membuat koneksi terotentikasi ke Google Sheets dan Drive API.
    
    Menggunakan informasi kredensial yang disimpan di Streamlit Secrets.
    """
    try:
        # Mendefinisikan cakupan (scope) akses yang dibutuhkan
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # Mengambil kredensial dari Streamlit Secrets
        # Pastikan Anda sudah mengatur secrets di Streamlit Cloud
        creds = Credentials.from_service_account_info(
            st.secrets["google_service_account"],
            scopes=scopes
        )
        
        # Mengotorisasi dan membuat client gspread
        client = gspread.authorize(creds)
        
        st.toast("Koneksi ke Google API berhasil!", icon="✅")
        return client
    except Exception as e:
        st.error(f"Terjadi kesalahan saat koneksi ke Google API: {e}")
        st.warning("Pastikan Anda sudah mengatur 'google_service_account' di Streamlit Secrets.")
        return None

# --- Judul Utama Aplikasi ---
st.title("📊 Dasbor Analisis Kompetitor")

# Memanggil fungsi koneksi di awal
gc = connect_to_google()

# ======================================================================================
# Bagian 2: Fungsi "Backend" (Logika Inti & Pengolahan Data)
# ======================================================================================

# --- Konfigurasi Cache & Path ---
# Membuat folder untuk menyimpan data cache jika belum ada
CACHE_DIR = Path("processed_data")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / "processed_data.parquet"

# ID Google Sheet yang berisi 'kamus' dan 'database_brand'
# Anda bisa dapatkan ini dari URL Google Sheet Anda
# Contoh URL: https://docs.google.com/spreadsheets/d/THIS_IS_THE_ID/edit#gid=0
GOOGLE_SHEET_ID = st.secrets.get("google_sheet_id", "GANTI_DENGAN_ID_ASLI_JIKA_TIDAK_PAKAI_SECRET")

# --- Fungsi Pengambilan Data ---

# Menggunakan cache data agar tidak perlu fetch dari Google Sheet setiap saat
@st.cache_data(ttl=3600) # Cache data selama 1 jam (3600 detik)
def get_brain_data_from_sheets(_gc):
    """
    Mengambil data 'kamus brand' dan 'database brand' dari Google Sheet.
    
    Args:
        _gc: Klien gspread yang sudah terotentikasi.
        
    Returns:
        Tuple berisi dua DataFrame: (kamus_df, db_brand_df)
    """
    if not _gc:
        st.error("Koneksi Google API tidak tersedia.")
        return pd.DataFrame(), pd.DataFrame()
        
    try:
        st.write("Mengambil data 'otak' dari Google Sheets...")
        spreadsheet = _gc.open_by_key(GOOGLE_SHEET_ID)
        
        # Baca worksheet 'kamus_brand'
        kamus_sheet = spreadsheet.worksheet("kamus_brand")
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        
        # Baca worksheet 'database_brand'
        db_brand_sheet = spreadsheet.worksheet("database_brand")
        db_brand_df = pd.DataFrame(db_brand_sheet.get_all_records())
        
        st.write("Data 'otak' berhasil diambil.")
        return kamus_df, db_brand_df
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet dengan ID '{GOOGLE_SHEET_ID}' tidak ditemukan.")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal mengambil data dari Google Sheets: {e}")
        return pd.DataFrame(), pd.DataFrame()

# CATATAN: Fungsi untuk mengambil data dari Google Drive lebih kompleks.
# Untuk saat ini, kita akan fokus pada fungsi yang sudah ada.
# Fungsi get_raw_data_from_drive() akan kita tambahkan nanti jika diperlukan.

# --- Fungsi Pelabelan Cerdas (Intelligent Labeling) ---

def label_brand(product_name, kamus_map, db_brand_list, a, b):
    """
    Melabeli brand pada nama produk menggunakan logika 3 tahap.
    
    Args:
        product_name (str): Nama produk yang akan dianalisis.
        kamus_map (dict): Kamus alias brand untuk pencarian cepat.
        db_brand_list (list): Daftar brand resmi.
        
    Returns:
        str: Nama brand yang teridentifikasi atau 'TIDAK DIKETAHUI'.
    """
    # Tahap 1: Pencarian di Kamus (Paling Cepat)
    # Mencari apakah ada alias dari kamus yang cocok di awal nama produk
    for alias, brand_utama in kamus_map.items():
        if str(product_name).strip().upper().startswith(alias.upper()):
            return brand_utama
            
    # Tahap 2: Pencarian Langsung di Database Brand
    # Mencari apakah ada nama brand dari database yang ada di dalam nama produk
    # Ini efektif untuk brand seperti 'Logitech', 'Asus', 'MSI', dll.
    for brand in db_brand_list:
        if f" {brand} " in f" {product_name} ": # Diberi spasi agar tidak salah (misal 'Asus' vs 'Asustor')
            return brand
            
    # Tahap 3: Fuzzy Matching (Jika dua tahap di atas gagal)
    # Mengambil kata pertama dari nama produk dan mencocokkannya dengan database brand
    first_word = product_name.split(' ')[0]
    best_match, score = process.extractOne(first_word, db_brand_list)
    if score >= 85: # Menggunakan threshold 85 untuk keyakinan
        return best_match
        
    return "TIDAK DIKETAHUI"

# --- Fungsi Orkestrasi Pemrosesan Data ---

def process_raw_data(df, kamus_df, db_brand_df):
    """
    Orkestrator utama untuk membersihkan, memperkaya, dan melabeli data mentah.
    
    Args:
        df (DataFrame): DataFrame gabungan data mentah dari semua toko.
        kamus_df (DataFrame): DataFrame kamus brand.
        db_brand_df (DataFrame): DataFrame database brand.
        
    Returns:
        DataFrame: DataFrame yang sudah bersih dan siap untuk divisualisasikan.
    """
    if df.empty:
        return pd.DataFrame()
        
    st.write("Memulai pemrosesan data...")
    processed_df = df.copy()
    
    # 1. Standarisasi Nama Kolom
    processed_df.columns = ['Nama Produk', 'Harga', 'Terjual per Bulan']
    
    # 2. Konversi Tipe Data & Pembersihan
    processed_df['Harga'] = pd.to_numeric(processed_df['Harga'], errors='coerce')
    processed_df['Terjual per Bulan'] = pd.to_numeric(processed_df['Terjual per Bulan'], errors='coerce')
    processed_df.dropna(subset=['Harga', 'Terjual per Bulan'], inplace=True)
    
    # 3. Hitung Metrik Baru: Omzet
    processed_df['Omzet'] = processed_df['Harga'] * processed_df['Terjual per Bulan']
    
    # 4. Pelabelan Brand
    # Menyiapkan data 'otak' untuk pencarian yang lebih cepat
    kamus_map = dict(zip(kamus_df['Alias'], kamus_df['Brand_Utama']))
    db_brand_list = db_brand_df['brand_utama'].str.upper().tolist() # Kolom brand di db harus 'brand_utama'
    
    # Mengaplikasikan fungsi pelabelan
    st.write("Melakukan pelabelan brand cerdas... (Ini mungkin butuh beberapa saat)")
    processed_df['Brand'] = processed_df['Nama Produk'].apply(
        lambda x: label_brand(x, kamus_map, db_brand_list)
    )
    
    st.success("Data berhasil diproses!")
    return processed_df

# --- Fungsi Manajemen Cache Lokal ---

def save_data_to_cache(df):
    """Menyimpan DataFrame ke file cache lokal dalam format Parquet."""
    df.to_parquet(CACHE_FILE)
    st.toast("Data terbaru disimpan ke cache.", icon="💾")

def load_data_from_cache():
    """Membaca DataFrame dari file cache lokal jika ada."""
    if CACHE_FILE.exists():
        st.toast("Memuat data dari cache...", icon="⚡")
        return pd.read_parquet(CACHE_FILE)
    return None

# ======================================================================================
# Bagian 3: Fungsi "Frontend" (Antarmuka Pengguna)
# ======================================================================================

# --- Fungsi Tampilan: Dasbor Utama ---

def display_main_dashboard(df):
    """
    Menampilkan dasbor analitik utama dengan filter, metrik, dan grafik.
    
    Args:
        df (DataFrame): DataFrame yang sudah bersih dan siap divisualisasikan.
    """
    if df is None or df.empty:
        st.info("Silakan tarik data terlebih dahulu untuk menampilkan dasbor.")
        return

    st.header("Dasbor Analitik Utama", divider='rainbow')

    # --- Sidebar untuk Filter ---
    with st.sidebar:
        st.header("Filter Data")
        
        # Filter multi-pilih untuk Brand
        all_brands = df['Brand'].unique()
        selected_brands = st.multiselect(
            "Pilih Brand:",
            options=all_brands,
            default=all_brands[:10] # Default: tampilkan 10 brand teratas
        )

    # Filter DataFrame berdasarkan pilihan di sidebar
    if selected_brands:
        filtered_df = df[df['Brand'].isin(selected_brands)]
    else:
        # Jika tidak ada brand yang dipilih, tampilkan semua
        filtered_df = df.copy()

    # --- Tampilan Metrik Utama ---
    total_omzet = filtered_df['Omzet'].sum()
    total_terjual = filtered_df['Terjual per Bulan'].sum()
    jumlah_produk = len(filtered_df)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Omzet", f"Rp {total_omzet:,.0f}")
    col2.metric("Total Unit Terjual", f"{total_terjual:,.0f}")
    col3.metric("Jumlah Produk Unik", f"{jumlah_produk:,.0f}")

    st.markdown("---")

    # --- Tampilan Tab untuk Analisis ---
    tab1, tab2, tab3 = st.tabs(["📈 Ringkasan Analisis", "🏢 Analisis per Brand", "📄 Detail Data"])

    with tab1:
        st.subheader("Top 10 Produk Berdasarkan Omzet")
        top_10_produk = filtered_df.nlargest(10, 'Omzet')
        st.dataframe(top_10_produk, use_container_width=True)

    with tab2:
        st.subheader("Perbandingan Kinerja Brand")
        
        # Agregasi data per brand
        brand_performance = filtered_df.groupby('Brand').agg(
            Total_Omzet=('Omzet', 'sum'),
            Rata_Rata_Terjual=('Terjual per Bulan', 'mean'),
            Jumlah_Produk=('Nama Produk', 'count')
        ).sort_values('Total_Omzet', ascending=False).reset_index()

        # Grafik Bar: Total Omzet per Brand
        fig_omzet = px.bar(
            brand_performance,
            x='Brand',
            y='Total_Omzet',
            title='Total Omzet per Brand',
            labels={'Total_Omzet': 'Total Omzet (Rp)', 'Brand': 'Nama Brand'}
        )
        st.plotly_chart(fig_omzet, use_container_width=True)

    with tab3:
        st.subheader("Data Lengkap (Setelah Filter)")
        st.dataframe(filtered_df, use_container_width=True)


# --- Fungsi Tampilan: Mode Koreksi ---

def display_correction_mode(df_unknown):
    """
    Menampilkan antarmuka untuk memperbaiki brand yang 'TIDAK DIKETAHUI'.
    
    Args:
        df_unknown (DataFrame): DataFrame berisi produk yang brand-nya tidak teridentifikasi.
    """
    st.header("🔧 Mode Koreksi Brand", divider='orange')
    st.warning("Ditemukan beberapa produk yang brand-nya tidak dapat diidentifikasi secara otomatis. Mohon bantu perbaiki.")

    # Menggunakan st.form agar input dikirim bersamaan saat tombol ditekan
    with st.form(key='correction_form'):
        st.subheader("Daftar Produk untuk Dikoreksi")
        
        # Menampilkan data editor agar pengguna bisa mengisi brand yang benar
        # Kita tambahkan kolom baru 'Koreksi_Brand' untuk diisi pengguna
        df_unknown['Koreksi_Brand'] = ''
        edited_df = st.data_editor(
            df_unknown[['Nama Produk', 'Koreksi_Brand']],
            num_rows="dynamic" # Memungkinkan pengguna menambah/menghapus baris jika perlu
        )
        
        # Tombol submit di dalam form
        submitted = st.form_submit_button("Simpan Koreksi & Proses Ulang")

    if submitted:
        # (Logika untuk memproses koreksi akan ditambahkan di Bagian 4)
        st.session_state.corrections = edited_df
        st.success("Koreksi diterima! Data akan segera diproses ulang.")
        # Kita akan memicu proses ulang di Bagian 4
        
# ======================================================================================
# Bagian 4: Alur Kerja Utama (Main App Flow)
# ======================================================================================

# --- Fungsi Tambahan untuk Update ke Google Sheets ---
# (Kita letakkan di sini agar dekat dengan alur utamanya)

def update_brain_in_sheets(_gc, corrections_df):
    """
    Menambahkan data koreksi dari pengguna ke worksheet 'kamus_brand' di Google Sheet.
    """
    if not _gc:
        st.error("Koneksi Google API tidak tersedia.")
        return
        
    try:
        st.write("Mengupdate 'otak' di Google Sheets dengan koreksi baru...")
        spreadsheet = _gc.open_by_key(GOOGLE_SHEET_ID)
        kamus_sheet = spreadsheet.worksheet("kamus_brand")
        
        # Mengubah DataFrame koreksi menjadi format list of lists untuk di-append
        new_rows = []
        for _, row in corrections_df.iterrows():
            if row['Koreksi_Brand']: # Hanya tambahkan jika pengguna mengisi koreksi
                new_rows.append([row['Nama Produk'], row['Koreksi_Brand'].upper()])
        
        if new_rows:
            kamus_sheet.append_rows(new_rows)
            st.success("'Otak' berhasil diupdate! Aplikasi akan menggunakan pengetahuan baru ini selanjutnya.")
            # Membersihkan cache data 'otak' agar pengambilan berikutnya mendapat data terbaru
            get_brain_data_from_sheets.clear()
        else:
            st.warning("Tidak ada koreksi yang diisi untuk disimpan.")

    except Exception as e:
        st.error(f"Gagal mengupdate Google Sheet: {e}")


# --- Inisialisasi Session State ---
# 'st.session_state' adalah cara Streamlit mengingat variabel antar interaksi
if 'mode' not in st.session_state:
    st.session_state.mode = "dashboard" # Mode awal: 'dashboard' atau 'correction'
if 'data' not in st.session_state:
    st.session_state.data = load_data_from_cache() # Coba muat data dari cache saat pertama kali jalan
if 'unknown_data' not in st.session_state:
    st.session_state.unknown_data = None
if 'corrections' not in st.session_state:
    st.session_state.corrections = None

# --- Logika untuk Memproses Koreksi yang Disubmit ---
if st.session_state.corrections is not None:
    corrections = st.session_state.corrections
    # Hapus data koreksi dari session state agar tidak dijalankan berulang
    st.session_state.corrections = None 
    
    # Panggil fungsi untuk update Google Sheet
    update_brain_in_sheets(gc, corrections)
    
    # Set mode kembali ke dashboard dan hapus data lama untuk memaksa proses ulang
    st.session_state.mode = 'dashboard'
    st.session_state.data = None 
    st.info("Silakan klik 'Tarik & Proses Data Baru' lagi untuk melihat hasilnya dengan 'otak' yang sudah diupdate.")
    st.rerun() # Memaksa Streamlit menjalankan ulang skrip dari awal


# --- Tombol Pemicu Utama ---
if st.button("🚀 Tarik & Proses Data Baru", type="primary"):
    with st.spinner("Mohon tunggu, sedang mengambil dan memproses ribuan data..."):
        # 1. Mengambil Data 'Otak'
        kamus_df, db_brand_df = get_brain_data_from_sheets(gc)
        
        # 2. Mengambil Data Mentah (RAW DATA)
        #    CATATAN: Untuk sekarang, kita baca dari file CSV lokal yang Anda unggah.
        #    Nantinya, bagian ini bisa diganti dengan fungsi `get_raw_data_from_drive()`.
        try:
            habis_df = pd.read_csv("2025-07-14_DB KLIK_PRODUK_HABIS.csv")
            ready_df = pd.read_csv("2025-07-14_DB KLIK_PRODUK_READY.csv")
            raw_df = pd.concat([habis_df, ready_df], ignore_index=True)
            st.write(f"Total {len(raw_df)} baris data mentah berhasil dimuat.")
        except FileNotFoundError:
            st.error("File CSV 'DB KLIK' tidak ditemukan. Pastikan file ada di folder yang sama dengan `app.py`")
            st.stop()
            
        # 3. Memproses Data
        processed_df = process_raw_data(raw_df, kamus_df, db_brand_df)
        
        # 4. Cek Hasil & Tentukan Mode
        if processed_df.empty:
            st.error("Gagal memproses data.")
        else:
            unknown_df = processed_df[processed_df['Brand'] == "TIDAK DIKETAHUI"]
            
            if not unknown_df.empty:
                st.warning(f"Ditemukan {len(unknown_df)} produk tanpa brand.")
                st.session_state.unknown_data = unknown_df
                st.session_state.mode = "correction"
            else:
                st.success("Semua produk berhasil diidentifikasi!")
                save_data_to_cache(processed_df)
                st.session_state.data = processed_df
                st.session_state.mode = "dashboard"
    
    # 5. Rerun untuk Menampilkan Tampilan yang Sesuai
    st.rerun()


# --- Logika Tampilan Kondisional (Conditional Display) ---
# Bagian ini menentukan UI mana yang akan ditampilkan ke pengguna

st.markdown("---")

if st.session_state.mode == "correction":
    display_correction_mode(st.session_state.unknown_data)
else: # Mode 'dashboard'
    display_main_dashboard(st.session_state.data)