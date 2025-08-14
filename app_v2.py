# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 3.0
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Update: Implementasi Cache Cerdas (Parquet) & Gerbang Kualitas Data
# ===================================================================================

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
from thefuzz import process, fuzz
import re
import plotly.express as px
import time
import os

# --- KONFIGURASI HALAMAN ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis v3.0")

# =====================================================================================
# BLOK KONFIGURASI UTAMA
# =====================================================================================
# --- ID & Nama Aset Google Drive ---
# ID Folder paling atas yang berisi semua folder proyek
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq" 
# Nama folder tempat Anda mengupload file-file CSV mentah hasil scraping
DATA_MENTAH_FOLDER_NAME = "data_upload"
# Nama folder tempat aplikasi akan menyimpan file cache cerdas
DATA_OLAHAN_FOLDER_NAME = "processed_data"
# Nama file cache cerdas. Direkomendasikan menggunakan .parquet untuk kecepatan.
CACHE_FILE_NAME = "master_data.parquet"

# --- ID Google Sheet "Otak" ---
# ID untuk spreadsheet yang berisi database_brand, kamus_brand, dan DATABASE
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg"
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
KATEGORI_SHEET_NAME = "DATABASE"

# --- Nama Kolom Konsisten ---
# Ini membantu agar jika ada perubahan nama kolom, cukup diubah di satu tempat
NAMA_PRODUK_COL = "Nama Produk"
HARGA_COL = "Harga"
TERJUAL_COL = "Terjual per bulan"
LINK_COL = "Link"
STATUS_COL = "Status"
TOKO_COL = "Toko"
BRAND_COL = "BRAND"
TANGGAL_COL = "Tanggal"
OMZET_COL = "Omzet"
KATEGORI_COL = "Kategori"

# =====================================================================================
# FUNGSI-FUNGSI INTI (BACKEND)
# =====================================================================================

# --- Fungsi Otentikasi & Koneksi ---
@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    """
    Melakukan autentikasi sekali ke Google menggunakan Streamlit Secrets 
    dan mengembalikan service object untuk Drive dan Sheets.
    """
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets", 
                "https://www.googleapis.com/auth/drive" # Scope drive diperlukan untuk baca/tulis
            ],
        )
        drive_service = build('drive', 'v3', credentials=creds)
        gsheets_service = gspread.authorize(creds)
        return drive_service, gsheets_service
    except Exception as e:
        st.error(f"Gagal melakukan autentikasi ke Google. Pastikan `secrets.toml` sudah benar. Error: {e}")
        st.stop()

# --- Fungsi untuk Manajemen Folder di Google Drive ---
@st.cache_data(show_spinner="Mencari ID folder di Google Drive...")
def find_folder_id(_drive_service, parent_id, folder_name):
    """Mencari ID sebuah folder berdasarkan namanya di dalam folder induk."""
    query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}' and trashed = false"
    try:
        response = _drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = response.get('files', [])
        if files:
            return files[0].get('id')
        else:
            st.error(f"Folder '{folder_name}' tidak ditemukan di dalam folder induk. Harap periksa kembali nama folder.")
            st.stop()
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mencari folder '{folder_name}': {e}")
        st.stop()


# --- Fungsi untuk Memuat Data "Otak" (Database Brand, Kamus, Kategori) ---
@st.cache_data(show_spinner="Memuat 'otak' dari database...", ttl=3600) # Cache selama 1 jam
def load_intelligence_data(_gsheets_service, spreadsheet_id):
    """
    Memuat semua data pendukung dari Google Sheet "Otak": 
    database brand, kamus alias, dan database kategori.
    """
    try:
        spreadsheet = _gsheets_service.open_by_key(spreadsheet_id)
        
        # Memuat database brand utama
        db_sheet = spreadsheet.worksheet(DB_SHEET_NAME)
        brand_db_list = [item for item in db_sheet.col_values(1) if item]
        
        # Memuat kamus alias brand
        kamus_sheet = spreadsheet.worksheet(KAMUS_SHEET_NAME)
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()

        # Memuat database produk untuk pemetaan kategori
        kategori_sheet = spreadsheet.worksheet(KATEGORI_SHEET_NAME)
        db_kategori_df = pd.DataFrame(kategori_sheet.get_all_records())
        db_kategori_df.columns = [str(col).strip().upper() for col in db_kategori_df.columns]

        return brand_db_list, kamus_dict, db_kategori_df
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"GAGAL: Sheet '{e.args[0]}' tidak ditemukan di Google Sheet 'Otak'. Harap periksa nama sheet.")
        st.stop()
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet 'Otak'. Error: {e}")
        st.stop()

# --- Fungsi untuk Membaca Data Mentah dari Drive ---
@st.cache_data(show_spinner="Membaca semua data mentah dari folder kompetitor...", ttl=3600)
def get_raw_data_from_drive(_drive_service, data_mentah_folder_id):
    """
    Membaca SEMUA file CSV dari SEMUA subfolder toko di dalam folder data_upload,
    lalu menggabungkannya menjadi satu DataFrame mentah.
    """
    all_data = []
    # 1. Cari semua subfolder (toko)
    query_subfolders = f"'{data_mentah_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = _drive_service.files().list(q=query_subfolders, fields="files(id, name)").execute()
    subfolders = results.get('files', [])

    if not subfolders:
        st.warning("Tidak ada subfolder (toko) yang ditemukan di dalam folder data mentah.")
        return pd.DataFrame()

    # Progress bar untuk memberikan feedback visual kepada pengguna
    progress_bar = st.progress(0, text="Membaca data...")
    for i, folder in enumerate(subfolders):
        progress_text = f"Membaca folder toko: {folder['name']}..."
        progress_bar.progress((i + 1) / len(subfolders), text=progress_text)
        
        # 2. Untuk setiap subfolder, cari semua file CSV atau Google Sheet
        file_query = f"'{folder['id']}' in parents and (mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet') and trashed = false"
        file_results = _drive_service.files().list(q=file_query, fields="files(id, name, mimeType)").execute()
        files_in_folder = file_results.get('files', [])

        for file_item in files_in_folder:
            file_id = file_item.get('id')
            file_name = file_item.get('name')
            mime_type = file_item.get('mimeType')
            
            try:
                # 3. Unduh file (konversi G-Sheet ke CSV jika perlu)
                if mime_type == 'application/vnd.google-apps.spreadsheet':
                    request = _drive_service.files().export_media(fileId=file_id, mimeType='text/csv')
                else:
                    request = _drive_service.files().get_media(fileId=file_id)

                downloader = io.BytesIO(request.execute())
                
                if downloader.getbuffer().nbytes == 0:
                    st.warning(f"FILE KOSONG: File '{file_name}' di folder '{folder['name']}' kosong dan akan dilewati.")
                    continue

                df = pd.read_csv(downloader)
                
                # Tambahkan informasi penting dari nama folder dan file
                df[TOKO_COL] = folder['name']
                match_tanggal = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
                df[TANGGAL_COL] = pd.to_datetime(match_tanggal.group(1)) if match_tanggal else pd.NaT
                
                if 'ready' in file_name.lower():
                    df[STATUS_COL] = 'Tersedia'
                elif 'habis' in file_name.lower():
                    df[STATUS_COL] = 'Habis'
                else:
                    df[STATUS_COL] = 'N/A'
                    
                all_data.append(df)
            except Exception as file_error:
                st.error(f"GAGAL BACA FILE: Error saat memproses '{file_name}' di folder '{folder['name']}'.")
                st.error(f"Detail Error: {file_error}")
                st.info("Proses dihentikan. Perbaiki file sebelum mencoba lagi.")
                st.stop()
    
    progress_bar.empty()
    if not all_data:
        st.warning("Tidak ada data valid yang ditemukan di semua folder toko.")
        return pd.DataFrame()
    
    # Gabungkan semua data menjadi satu DataFrame besar
    final_df = pd.concat(all_data, ignore_index=True)
    return final_df

# --- Fungsi-fungsi Pemrosesan Data (Labeling, Cleaning, etc.) ---
def process_raw_data(raw_df, brand_db, kamus_brand, db_kategori):
    """
    Mengambil DataFrame mentah dan melakukan semua langkah pemrosesan:
    - Konversi tipe data
    - Menghitung omzet
    - Melakukan labeling brand
    - Melakukan pemetaan kategori
    """
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()

    # 1. Standarisasi nama kolom
    rename_map = {
        'Nama Produk': NAMA_PRODUK_COL,
        'Harga': HARGA_COL,
        'Terjual per Bulan': TERJUAL_COL,
        'Link': LINK_COL
    }
    df.rename(columns=lambda c: rename_map.get(c.strip(), c.strip()), inplace=True)

    # 2. Pastikan kolom esensial ada
    if NAMA_PRODUK_COL not in df.columns:
        st.error(f"KOLOM HILANG: Data gabungan tidak memiliki kolom '{NAMA_PRODUK_COL}'. Proses tidak bisa dilanjutkan.")
        st.stop()

    # 3. Konversi tipe data dan penanganan error
    df[HARGA_COL] = pd.to_numeric(df.get(HARGA_COL), errors='coerce').fillna(0)
    # Logika pembersihan kolom terjual dihilangkan sesuai permintaan
    df[TERJUAL_COL] = pd.to_numeric(df.get(TERJUAL_COL), errors='coerce').fillna(0)
    
    df[HARGA_COL] = df[HARGA_COL].astype(float)
    df[TERJUAL_COL] = df[TERJUAL_COL].astype(int)
    
    # 4. Hitung Omzet
    df[OMZET_COL] = df[HARGA_COL] * df[TERJUAL_COL]
    
    # 5. Proses Labeling Brand (Fuzzy Matching)
    st.write("Memulai proses labeling brand (mungkin butuh waktu)...")
    df = label_brands(df, brand_db, kamus_brand)
    
    # 6. Proses Pemetaan Kategori (Fuzzy Matching)
    st.write("Memulai proses pemetaan kategori...")
    df = map_categories(df, db_kategori)
    
    st.write("Semua proses data selesai.")
    return df

def label_brands(df, brand_db, kamus_brand, fuzzy_threshold=88):
    """Memberi label brand pada setiap produk berdasarkan database dan kamus."""
    brand_db_sorted = sorted(brand_db, key=len, reverse=True)
    brands = []
    
    for product_name in df[NAMA_PRODUK_COL].astype(str).str.upper():
        found_brand = None
        # Prioritas 1: Cek kamus alias
        for alias, brand_utama in kamus_brand.items():
            if re.search(r'\b' + re.escape(str(alias).upper()) + r'\b', product_name):
                found_brand = brand_utama
                break
        if found_brand:
            brands.append(found_brand)
            continue
        
        # Prioritas 2: Cek database brand (pencocokan eksak)
        for brand in brand_db_sorted:
            if re.search(r'\b' + re.escape(brand.upper()) + r'\b', product_name) or (brand.upper() in product_name.replace(" ", "")):
                found_brand = brand
                break
        if found_brand:
            brands.append(found_brand)
            continue
            
        # Prioritas 3: Fuzzy matching sebagai usaha terakhir
        best_match = process.extractOne(product_name, brand_db, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold:
            found_brand = best_match[0]
            
        brands.append(found_brand if found_brand else "TIDAK DIKETAHUI")
        
    df[BRAND_COL] = brands
    return df

@st.cache_data(show_spinner="Memetakan kategori produk...")
def map_categories(_df, _db_kategori, fuzzy_threshold=95):
    """Memetakan kategori untuk setiap produk berdasarkan database kategori."""
    _df[KATEGORI_COL] = 'Lainnya'
    if _db_kategori.empty or 'NAMA' not in _db_kategori.columns or 'KATEGORI' not in _db_kategori.columns:
        return _df
    
    db_unique = _db_kategori.drop_duplicates(subset=['NAMA'])
    db_map = db_unique.set_index('NAMA')['KATEGORI']
    
    for index, row in _df.iterrows():
        if pd.notna(row[NAMA_PRODUK_COL]):
            match, score = process.extractOne(row[NAMA_PRODUK_COL], db_map.index, scorer=fuzz.token_set_ratio)
            if score >= fuzzy_threshold:
                _df.loc[index, KATEGORI_COL] = db_map[match]
    return _df

# --- Fungsi Manajemen Cache Cerdas (Parquet) ---
def check_cache_exists(drive_service, folder_id, filename):
    """Mengecek apakah file cache sudah ada di folder processed_data."""
    query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    response = drive_service.files().list(q=query, fields="files(id)").execute()
    return response.get('files', [])

def load_data_from_cache(drive_service, file_id):
    """Mengunduh file Parquet dari Drive dan memuatnya sebagai DataFrame."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_parquet(fh)

def save_data_to_cache(drive_service, folder_id, filename, df_to_save):
    """Menyimpan DataFrame sebagai file Parquet ke Google Drive."""
    buffer = io.BytesIO()
    df_to_save.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    # Cek apakah file sudah ada untuk di-update, atau buat baru
    existing_files = check_cache_exists(drive_service, folder_id, filename)
    
    media_body = MediaFileUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    if existing_files:
        # Update file yang sudah ada
        file_id = existing_files[0]['id']
        drive_service.files().update(fileId=file_id, media_body=media_body).execute()
        st.toast(f"Cache cerdas '{filename}' berhasil diperbarui.", icon="🔄")
    else:
        # Buat file baru
        file_metadata = {'name': filename, 'parents': [folder_id]}
        drive_service.files().create(body=file_metadata, media_body=media_body, fields='id').execute()
        st.toast(f"Cache cerdas '{filename}' berhasil dibuat.", icon="✅")

# --- Fungsi Bantuan untuk UI ---
def format_harga(x):
    if pd.isnull(x): return "N/A"
    try: return f"Rp {float(x):,.0f}"
    except (ValueError, TypeError): return str(x)

def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

def colorize_growth(val):
    color = 'grey'
    if isinstance(val, str):
        if '▲' in val: color = '#28a745'
        elif '▼' in val: color = '#dc3545'
    return f'color: {color}'

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# =====================================================================================
# FUNGSI-FUNGSI TAMPILAN (FRONTEND)
# =====================================================================================

def display_correction_mode(gsheets_service):
    """
    Menampilkan UI "Ruang Kontrol" saat aplikasi dalam mode perbaikan data.
    Halaman lain akan disembunyikan.
    """
    st.header("🧠 Ruang Kontrol: Perbaikan Data Brand")
    st.warning("Ditemukan beberapa produk yang brand-nya tidak dikenali. Harap perbaiki data di bawah ini sebelum melanjutkan ke dashboard analisis.")
    
    df_to_fix = st.session_state.df_to_fix
    unknown_products = df_to_fix[df_to_fix[BRAND_COL] == 'TIDAK DIKETAHUI']
    
    if unknown_products.empty:
        st.success("🎉 Semua brand sudah dikenali! Menyimpan data bersih...")
        
        # Simpan data yang sudah bersih ke cache cerdas
        save_data_to_cache(
            st.session_state.drive_service, 
            st.session_state.data_olahan_folder_id, 
            CACHE_FILE_NAME, 
            df_to_fix
        )
        
        # Update state aplikasi dan refresh
        st.session_state.mode = 'dashboard'
        st.session_state.master_df = df_to_fix.copy()
        del st.session_state.df_to_fix
        time.sleep(2)
        st.rerun()
        return

    st.info(f"Tersisa **{len(unknown_products)} produk** yang perlu direview.")
    
    product_to_review = unknown_products.iloc[0]
    st.divider()
    st.write("Produk yang perlu direview:")
    st.info(f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review[TOKO_COL]})")

    with st.form(key="review_form_single"):
        st.subheader("Apa brand yang benar untuk produk ini?")
        
        col1, col2 = st.columns(2)
        
        # Muat daftar brand terbaru dari session state
        brand_list = [""] + sorted(st.session_state.brand_db)
        selected_brand = col1.selectbox("1. Pilih dari brand yang sudah ada:", options=brand_list, help="Pilih brand yang paling sesuai dari daftar.")
        
        new_brand_input = col2.text_input("2. Atau, masukkan nama brand BARU:", help="Isi ini jika brand tidak ada di daftar sebelah.")
        
        st.divider()
        
        st.subheader("Ajari sistem tentang Alias (Nama Lain)")
        alias_input = st.text_input("Jika produk ini punya nama lain/singkatan, masukkan di sini:", 
                                    help="Contoh: Nama produk 'MI NOTEBOOK', Brand Utama 'XIAOMI'. Maka isi alias ini dengan 'MI'.")
        
        submitted = st.form_submit_button("Ajarkan ke Sistem & Lanjut")

        if submitted:
            final_brand = ""
            if new_brand_input:
                final_brand = new_brand_input.strip().upper()
            elif selected_brand:
                final_brand = selected_brand
            
            if not final_brand:
                st.error("Anda harus memilih brand yang sudah ada atau memasukkan brand baru.")
            else:
                # Proses update ke Google Sheet "Otak"
                if new_brand_input and final_brand not in st.session_state.brand_db:
                    try:
                        sheet = gsheets_service.open_by_key(SPREADSHEET_ID).worksheet(DB_SHEET_NAME)
                        sheet.append_row([final_brand], value_input_option='USER_ENTERED')
                        st.session_state.brand_db.append(final_brand) # Update list di memori
                        st.toast(f"Brand baru '{final_brand}' berhasil ditambahkan ke database.", icon="➕")
                    except Exception as e:
                        st.error(f"Gagal menyimpan brand baru ke Google Sheet: {e}")
                
                if alias_input:
                    try:
                        sheet = gsheets_service.open_by_key(SPREADSHEET_ID).worksheet(KAMUS_SHEET_NAME)
                        sheet.append_row([alias_input.strip().upper(), final_brand], value_input_option='USER_ENTERED')
                        st.toast(f"Alias '{alias_input.upper()}' untuk '{final_brand}' berhasil disimpan.", icon="📚")
                    except Exception as e:
                        st.error(f"Gagal menyimpan alias ke Google Sheet: {e}")
                
                # Update DataFrame di memori (session_state)
                product_name_to_update = product_to_review[NAMA_PRODUK_COL]
                indices_to_update = st.session_state.df_to_fix[st.session_state.df_to_fix[NAMA_PRODUK_COL] == product_name_to_update].index
                st.session_state.df_to_fix.loc[indices_to_update, BRAND_COL] = final_brand
                
                st.toast("Sistem telah belajar! Menampilkan produk berikutnya...", icon="✅")
                time.sleep(1)
                st.rerun()

def display_main_dashboard(df):
    """
    Menampilkan seluruh UI dashboard utama, termasuk sidebar dan semua halaman analisis.
    Fungsi ini hanya dipanggil jika data sudah bersih dan siap.
    """
    st.sidebar.header("Navigasi Halaman")
    page = st.sidebar.radio("Pilih Halaman:", ["Ringkasan Eksekutif", "Analisis Mendalam", "Analisis Produk Tunggal"])
    st.sidebar.divider()

    st.sidebar.header("Filter Global")
    
    # Filter Toko Utama
    all_stores = sorted(df[TOKO_COL].unique())
    try:
        default_store_index = all_stores.index("DB_KLIK")
    except ValueError:
        default_store_index = 0
    main_store = st.sidebar.selectbox("Pilih Toko Utama Anda:", all_stores, index=default_store_index)

    # Filter Rentang Tanggal
    df_with_dates = df.dropna(subset=[TANGGAL_COL]).copy()
    min_date, max_date = df_with_dates[TANGGAL_COL].min().date(), df_with_dates[TANGGAL_COL].max().date()
    selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)

    # Filter Akurasi Fuzzy
    accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1, key="global_accuracy", help="Digunakan untuk membandingkan produk antar toko.")

    st.sidebar.divider()
    st.sidebar.header("Download Data")

    csv_to_download = convert_df_to_csv(df)
    st.sidebar.download_button(
       label="📥 Download Data Olahan (CSV)", data=csv_to_download,
       file_name='data_olahan_lengkap.csv', mime='text/csv',
    )

    # Logika filtering data berdasarkan input sidebar
    if len(selected_date_range) != 2:
        st.warning("Harap pilih rentang tanggal yang valid.")
        st.stop()
        
    start_date, end_date = selected_date_range
    df_filtered = df_with_dates[(df_with_dates[TANGGAL_COL].dt.date >= start_date) & (df_with_dates[TANGGAL_COL].dt.date <= end_date)].copy()
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()
        
    df_filtered['Minggu'] = df_filtered[TANGGAL_COL].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
    main_store_df = df_filtered[df_filtered[TOKO_COL] == main_store].copy()
    competitor_df = df_filtered[df_filtered[TOKO_COL] != main_store].copy()

    # Navigasi Halaman
    if page == "Ringkasan Eksekutif":
        st.header("📈 Ringkasan Eksekutif")
        
        latest_date_in_data = df_filtered[TANGGAL_COL].max()
        st.markdown(f"Menampilkan data terbaru per tanggal **{latest_date_in_data.strftime('%d %b %Y')}**")
        
        df_latest = df_filtered[df_filtered[TANGGAL_COL] == latest_date_in_data]
        df_latest_main_store = df_latest[df_latest[TOKO_COL] == main_store]

        omzet_today_main = df_latest_main_store[OMZET_COL].sum()
        units_today_main = df_latest_main_store[TERJUAL_COL].sum()
        
        total_ready_latest_main = len(df_latest_main_store[df_latest_main_store[STATUS_COL] == 'Tersedia'])
        total_habis_latest_main = len(df_latest_main_store[df_latest_main_store[STATUS_COL] == 'Habis'])
        total_produk_latest_main = total_ready_latest_main + total_habis_latest_main
        
        units_sold_latest_ready_main = df_latest_main_store[df_latest_main_store[STATUS_COL] == 'Tersedia'][TERJUAL_COL].sum()
        
        col1, col2, col3 = st.columns(3)
        col1.metric(f"Omzet {main_store} (Hari Ini)", format_harga(omzet_today_main), f"{int(units_today_main)} unit terjual")
        col2.metric(f"Jumlah Produk {main_store} (Hari Ini)", f"{total_produk_latest_main:,} Produk", f"Tersedia: {total_ready_latest_main:,} | Habis: {total_habis_latest_main:,}")
        col3.metric(f"Unit Terjual {main_store} (Ready, Hari Ini)", f"{int(units_sold_latest_ready_main):,} Unit")
        
        st.divider()

        st.subheader("Perbandingan Omzet per Toko (Data Terbaru)")
        omzet_latest_per_store = df_latest.groupby(TOKO_COL)[OMZET_COL].sum().sort_values(ascending=False).reset_index()
        fig_bar = px.bar(omzet_latest_per_store, x=TOKO_COL, y=OMZET_COL, title=f"Total Omzet per Toko pada {latest_date_in_data.strftime('%d %b %Y')}", text_auto=True)
        fig_bar.update_traces(texttemplate='%{value:,.0f}')
        st.plotly_chart(fig_bar, use_container_width=True)
        
        st.divider()
        
        st.subheader("Tabel Pertumbuhan Omzet Mingguan per Toko (%)")
        weekly_omzet_pivot = df_filtered.groupby(['Minggu', TOKO_COL])[OMZET_COL].sum().unstack()
        weekly_growth_pivot = weekly_omzet_pivot.pct_change()
        weekly_growth_pivot.index = pd.to_datetime(weekly_growth_pivot.index).strftime('%Y-%m-%d')
        
        st.dataframe(
            weekly_growth_pivot.style.format(format_wow_growth).applymap(colorize_growth),
            use_container_width=True
        )

    elif page == "Analisis Mendalam":
        st.header("🔍 Analisis Mendalam")
        tab_titles = [f"⭐ Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Brand Kompetitor", "📦 Status Stok", "📈 Kinerja Penjualan", "📊 Produk Baru"]
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_titles)

        with tab1:
            st.subheader("1. Produk Terlaris")
            top_products = main_store_df.sort_values(TERJUAL_COL, ascending=False).head(15)[[NAMA_PRODUK_COL, TERJUAL_COL, OMZET_COL]]
            st.dataframe(top_products.style.format({OMZET_COL: format_harga, TERJUAL_COL: '{:,.0f}'}), use_container_width=True, hide_index=True)

            st.subheader("2. Distribusi Omzet Brand")
            brand_omzet_main = main_store_df.groupby(BRAND_COL)[OMZET_COL].sum().reset_index()
            fig_brand_pie = px.pie(brand_omzet_main, names=BRAND_COL, values=OMZET_COL, title='Distribusi Omzet Brand')
            st.plotly_chart(fig_brand_pie, use_container_width=True)

        with tab2:
            st.subheader(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
            if not main_store_df.empty:
                latest_date = main_store_df[TANGGAL_COL].max()
                main_store_latest = main_store_df[main_store_df[TANGGAL_COL] == latest_date].copy()
                
                product_list = sorted(main_store_latest[NAMA_PRODUK_COL].unique())
                selected_product = st.selectbox("Pilih produk dari toko Anda untuk dibandingkan:", product_list)
                
                if selected_product:
                    product_info = main_store_latest[main_store_latest[NAMA_PRODUK_COL] == selected_product].iloc[0]
                    st.markdown(f"**Produk Pilihan:** *{product_info[NAMA_PRODUK_COL]}*")
                    col1, col2 = st.columns(2)
                    col1.metric(f"Harga di {main_store}", format_harga(product_info[HARGA_COL]))
                    col2.metric(f"Status", product_info[STATUS_COL])
                    
                    st.markdown("---")
                    st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                    competitor_latest = competitor_df[competitor_df[TANGGAL_COL] == latest_date]
                    if not competitor_latest.empty:
                        matches = process.extract(product_info[NAMA_PRODUK_COL], competitor_latest[NAMA_PRODUK_COL].tolist(), limit=5, scorer=fuzz.token_set_ratio)
                        valid_matches = [m for m in matches if m[1] >= accuracy_cutoff]
                        if not valid_matches:
                            st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                        else:
                            for product, score in valid_matches:
                                match_info = competitor_latest[competitor_latest[NAMA_PRODUK_COL] == product].iloc[0]
                                price_diff = float(match_info[HARGA_COL]) - float(product_info[HARGA_COL])
                                st.markdown(f"**Toko: {match_info[TOKO_COL]}** (Kemiripan: {int(score)}%)")
                                st.markdown(f"*{match_info[NAMA_PRODUK_COL]}*")
                                c1, c2 = st.columns(2)
                                c1.metric("Harga Kompetitor", format_harga(match_info[HARGA_COL]), delta=f"Rp {price_diff:,.0f}")
                                c2.metric("Status", match_info[STATUS_COL])
        with tab3:
            st.subheader("Analisis Brand di Toko Kompetitor")
            if competitor_df.empty:
                st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
            else:
                brand_analysis = competitor_df.groupby([TOKO_COL, BRAND_COL]).agg(Total_Omzet=(OMZET_COL, 'sum'), Total_Unit_Terjual=(TERJUAL_COL, 'sum')).reset_index()
                fig = px.treemap(brand_analysis, path=[TOKO_COL, BRAND_COL], values='Total_Omzet', title='Peta Omzet Brand per Toko Kompetitor')
                st.plotly_chart(fig, use_container_width=True)

        with tab4:
            st.subheader("Tren Status Stok Mingguan per Toko")
            stock_trends = df_filtered.groupby(['Minggu', TOKO_COL, STATUS_COL]).size().unstack(fill_value=0).reset_index()
            if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
            if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
            stock_trends_melted = stock_trends.melt(id_vars=['Minggu', TOKO_COL], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
            fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color=TOKO_COL, line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
            st.plotly_chart(fig_stock_trends, use_container_width=True)

        with tab5:
            st.subheader("Grafik Omzet Mingguan")
            weekly_omzet = df_filtered.groupby(['Minggu', TOKO_COL])[OMZET_COL].sum().reset_index()
            fig_weekly_omzet = px.line(weekly_omzet, x='Minggu', y=OMZET_COL, color=TOKO_COL, markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
            st.plotly_chart(fig_weekly_omzet, use_container_width=True)

        with tab6:
            st.subheader("Perbandingan Produk Baru Antar Minggu")
            weeks = sorted(df_filtered['Minggu'].unique())
            if len(weeks) < 2:
                st.info("Butuh setidaknya 2 minggu data untuk perbandingan.")
            else:
                col1, col2 = st.columns(2)
                week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0, key="week_before")
                week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1, key="week_after")
                if week_before >= week_after:
                    st.error("Minggu Penentu harus setelah Minggu Pembanding.")
                else:
                    for store in sorted(df_filtered[TOKO_COL].unique()):
                        with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                            products_before = set(df_filtered[(df_filtered[TOKO_COL] == store) & (df_filtered['Minggu'] == week_before)][NAMA_PRODUK_COL])
                            products_after = set(df_filtered[(df_filtered[TOKO_COL] == store) & (df_filtered['Minggu'] == week_after)][NAMA_PRODUK_COL])
                            new_products = products_after - products_before
                            if not new_products:
                                st.write("Tidak ada produk baru yang terdeteksi.")
                            else:
                                st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                                new_products_df = df_filtered[(df_filtered[NAMA_PRODUK_COL].isin(new_products)) & (df_filtered[TOKO_COL] == store) & (df_filtered['Minggu'] == week_after)]
                                st.dataframe(new_products_df[[NAMA_PRODUK_COL, HARGA_COL, STATUS_COL, TERJUAL_COL]].style.format({HARGA_COL: format_harga}), use_container_width=True, hide_index=True)

    elif page == "Analisis Produk Tunggal":
        st.header("🎯 Analisis Produk Tunggal")
        st.info("Gunakan filter di bawah untuk menemukan produk spesifik dan melihat trennya di seluruh pasar.")
        
        all_brands = ['Semua Brand'] + sorted(df_filtered[BRAND_COL].unique())
        selected_brand_filter = st.selectbox("Filter berdasarkan Brand (Opsional):", all_brands)
        
        if selected_brand_filter == 'Semua Brand':
            product_pool = df_filtered
        else:
            product_pool = df_filtered[df_filtered[BRAND_COL] == selected_brand_filter]

        all_products = sorted(product_pool[NAMA_PRODUK_COL].unique())
        if all_products:
            selected_product = st.selectbox("Cari dan pilih produk:", all_products)
            
            if selected_product:
                product_df = df_filtered[df_filtered[NAMA_PRODUK_COL] == selected_product].copy()
                
                st.subheader(f"Tren Historis untuk: {selected_product}")
                product_df_sorted = product_df.sort_values(by=TANGGAL_COL)
                fig_trend = px.line(product_df_sorted, x=TANGGAL_COL, y=HARGA_COL, color=TOKO_COL, markers=True, title="Tren Harga dari Waktu ke Waktu")
                st.plotly_chart(fig_trend, use_container_width=True)

                st.subheader("Perbandingan Kompetitif Saat Ini")
                latest_date_overall = df_filtered[TANGGAL_COL].max()
                latest_products_df = df_filtered[df_filtered[TANGGAL_COL] == latest_date_overall]
                matches = process.extract(selected_product, latest_products_df[NAMA_PRODUK_COL].unique(), limit=None, scorer=fuzz.token_set_ratio)
                similar_product_names = [match[0] for match in matches if match[1] >= accuracy_cutoff]
                competitor_landscape = latest_products_df[latest_products_df[NAMA_PRODUK_COL].isin(similar_product_names)]
                st.dataframe(competitor_landscape[[TOKO_COL, NAMA_PRODUK_COL, HARGA_COL, STATUS_COL, TERJUAL_COL]].style.format({HARGA_COL: format_harga, TERJUAL_COL: '{:,.0f}'}), use_container_width=True, hide_index=True)
        else:
            st.warning("Tidak ada produk untuk ditampilkan dengan filter brand yang dipilih.")


# =====================================================================================
# ALUR KERJA UTAMA APLIKASI
# =====================================================================================

st.title("📊 Dashboard Analisis Penjualan & Kompetitor v3.0")
st.markdown("Versi dengan *Cache Cerdas* dan *Gerbang Kualitas Data*.")

# Inisialisasi session_state jika belum ada
if 'mode' not in st.session_state:
    st.session_state.mode = 'initial' # Mode awal
if 'master_df' not in st.session_state:
    st.session_state.master_df = pd.DataFrame()
if 'df_to_fix' not in st.session_state:
    st.session_state.df_to_fix = pd.DataFrame()

# --- Tombol Pemicu Utama ---
st.sidebar.header("Kontrol Utama")
st.sidebar.info("Proses ini akan cepat jika cache cerdas sudah ada. Jika tidak, proses akan memakan waktu untuk membangun cache baru.")
if st.sidebar.button("🚀 Tarik & Proses Data Terbaru", type="primary"):
    with st.spinner("Memeriksa status data..."):
        # 1. Otentikasi dan dapatkan service object
        drive_service, gsheets_service = get_google_apis()
        st.session_state.drive_service = drive_service # Simpan untuk digunakan nanti
        st.session_state.gsheets_service = gsheets_service

        # 2. Cari ID folder data mentah dan olahan
        data_mentah_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_MENTAH_FOLDER_NAME)
        data_olahan_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_OLAHAN_FOLDER_NAME)
        st.session_state.data_olahan_folder_id = data_olahan_folder_id # Simpan untuk digunakan nanti

        # 3. Cek apakah cache cerdas sudah ada
        cache_file = check_cache_exists(drive_service, data_olahan_folder_id, CACHE_FILE_NAME)

        if cache_file:
            # --- JALUR CEPAT ---
            st.toast("Cache cerdas ditemukan! Memuat data...", icon="⚡")
            df = load_data_from_cache(drive_service, cache_file[0]['id'])
            st.session_state.master_df = df
            st.session_state.mode = 'dashboard'
        else:
            # --- JALUR BERAT ---
            st.toast("Cache cerdas tidak ditemukan. Memulai proses data dari awal...", icon="🐌")
            
            # Memuat data "otak"
            brand_db, kamus_brand, db_kategori = load_intelligence_data(gsheets_service, SPREADSHEET_ID)
            st.session_state.brand_db = brand_db # Simpan untuk Ruang Kontrol

            # Membaca semua data mentah
            raw_df = get_raw_data_from_drive(drive_service, data_mentah_folder_id)
            
            if raw_df.empty:
                st.warning("Tidak ada data mentah yang bisa diproses.")
                st.session_state.mode = 'initial'
            else:
                # Memproses data mentah (termasuk labeling)
                processed_df = process_raw_data(raw_df, brand_db, kamus_brand, db_kategori)
                
                # Gerbang Kualitas Data
                unknown_brands_count = (processed_df[BRAND_COL] == 'TIDAK DIKETAHUI').sum()
                
                if unknown_brands_count > 0:
                    # Jika ada brand tak dikenal, masuk mode perbaikan
                    st.session_state.df_to_fix = processed_df
                    st.session_state.mode = 'correction'
                else:
                    # Jika semua bersih, simpan ke cache dan masuk mode dashboard
                    save_data_to_cache(drive_service, data_olahan_folder_id, CACHE_FILE_NAME, processed_df)
                    st.session_state.master_df = processed_df
                    st.session_state.mode = 'dashboard'
    
    st.rerun() # Refresh halaman untuk menampilkan UI sesuai mode yang baru


# --- Logika Tampilan Berdasarkan Mode Aplikasi ---
if st.session_state.mode == 'initial':
    st.info("👈 Silakan klik tombol **'Tarik & Proses Data Terbaru'** di sidebar untuk memulai.")
    st.markdown("---")
    st.subheader("Struktur Folder yang Diharapkan di Google Drive:")
    st.code(f"""
STREAMLIT_ANALISIS_PENJUALAN/ (ID: {PARENT_FOLDER_ID})
|
|-- 📂 {DATA_MENTAH_FOLDER_NAME}/
|   |-- 📂 NAMA_TOKO_1/
|   |   |-- 📜 2025-08-12-ready.csv
|   |   `-- ...
|   `-- 📂 NAMA_TOKO_2/
|
|-- 📂 {DATA_OLAHAN_FOLDER_NAME}/
|   `-- (Folder ini akan diisi otomatis oleh aplikasi)
|
`-- 📜 (File Google Sheet 'Otak' Anda)
    """, language="text")

elif st.session_state.mode == 'correction':
    # Panggil UI untuk mode perbaikan
    display_correction_mode(st.session_state.gsheets_service)

elif st.session_state.mode == 'dashboard':
    # Panggil UI untuk dashboard utama
    if not st.session_state.master_df.empty:
        display_main_dashboard(st.session_state.master_df)
    else:
        st.error("Terjadi kesalahan, data master tidak berhasil dimuat.")
        st.session_state.mode = 'initial'
        st.rerun()
