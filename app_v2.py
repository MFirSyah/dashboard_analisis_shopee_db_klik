# ===================================================================================
#  DASHBOARD ANALISIS BRAND & KOMPETITOR - VERSI 2.0
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Fitur Utama:
#  - Otomatisasi Labeling Brand dengan Sistem Multi-Lapis
#  - "Ruang Kontrol" Interaktif untuk Melatih Sistem (Human-in-the-Loop)
#  - Koneksi Langsung ke Google Drive (Multi-Folder) & Google Sheets
# ===================================================================================

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import io
from thefuzz import process, fuzz
import re

# --- KONFIGURASI HALAMAN & ID ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis Brand v2.0")

# ID ini didapatkan dari URL Google Drive & Google Sheets Anda
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq"
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg"
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
NAMA_PRODUK_COL = "Nama Produk" # Sesuaikan jika nama kolom produk Anda berbeda

# --- FUNGSI-FUNGSI UTAMA (Dengan Caching untuk Performa) ---

@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    """
    Melakukan autentikasi sekali dan mengembalikan service object untuk Drive dan Sheets.
    Menggunakan st.cache_resource agar koneksi tidak dibuat berulang kali.
    """
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.readonly",
            ],
        )
        drive_service = build('drive', 'v3', credentials=creds)
        gsheets_service = gspread.authorize(creds)
        return drive_service, gsheets_service
    except Exception as e:
        st.error(f"Gagal melakukan autentikasi ke Google. Pastikan `secrets.toml` sudah benar. Error: {e}")
        st.stop()

@st.cache_data(show_spinner="Memuat 'otak' dari database brand...", ttl=300)
def load_brand_intelligence(_gsheets_service, spreadsheet_id):
    """
    Memuat database brand resmi dan kamus alias dari Google Sheet.
    Data di-cache selama 5 menit (300 detik).
    """
    try:
        spreadsheet = _gsheets_service.open_by_key(spreadsheet_id)
        
        # Memuat database brand resmi
        db_sheet = spreadsheet.worksheet(DB_SHEET_NAME)
        brand_db_list = [item for item in db_sheet.col_values(1) if item] # Ambil kolom pertama, abaikan sel kosong
        
        # Memuat kamus alias
        kamus_sheet = spreadsheet.worksheet(KAMUS_SHEET_NAME)
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        
        if 'Alias' not in kamus_df.columns or 'Brand_Utama' not in kamus_df.columns:
            st.error("File 'kamus_brand' harus memiliki kolom 'Alias' dan 'Brand_Utama'.")
            return [], {}

        kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()
        
        return brand_db_list, kamus_dict
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"Worksheet tidak ditemukan: {e}. Pastikan nama sheet sudah benar ('{DB_SHEET_NAME}' dan '{KAMUS_SHEET_NAME}').")
        st.stop()
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet. Error: {e}")
        st.stop()

@st.cache_data(show_spinner="Membaca semua data dari folder kompetitor...", ttl=300)
def get_all_competitor_data(_drive_service, parent_folder_id):
    """
    Membaca semua file CSV dari semua subfolder di dalam folder induk Google Drive.
    """
    all_data = []
    try:
        # 1. Cari semua subfolder di dalam folder induk
        query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        results = _drive_service.files().list(q=query, fields="files(id, name)").execute()
        subfolders = results.get('files', [])

        if not subfolders:
            st.warning(f"Tidak ada subfolder yang ditemukan di dalam folder induk.")
            return pd.DataFrame()

        # 2. Loop melalui setiap subfolder
        for folder in subfolders:
            file_query = f"'{folder['id']}' in parents and mimeType = 'text/csv'"
            file_results = _drive_service.files().list(q=file_query, fields="files(id, name)").execute()
            csv_files = file_results.get('files', [])

            for csv_file in csv_files:
                request = _drive_service.files().get_media(fileId=csv_file['id'])
                downloader = io.BytesIO(request.execute())
                df = pd.read_csv(downloader)
                
                # Menambahkan kolom sumber data
                df['Toko'] = folder['name']
                df['Sumber_File'] = csv_file['name']
                all_data.append(df)
        
        if not all_data:
            return pd.DataFrame()

        return pd.concat(all_data, ignore_index=True)

    except HttpError as error:
        st.error(f"Gagal mengakses Google Drive. Pastikan folder sudah dibagikan. Error: {error}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data CSV: {e}")
        return pd.DataFrame()

def label_brands(df, brand_db, kamus_brand, fuzzy_threshold=88):
    """
    Fungsi utama untuk melabeli brand dengan strategi multi-lapis.
    """
    if NAMA_PRODUK_COL not in df.columns:
        st.error(f"Data CSV tidak memiliki kolom '{NAMA_PRODUK_COL}'. Proses labeling tidak bisa dilanjutkan.")
        st.stop()

    # Urutkan brand DB dari yang terpanjang agar tidak salah identifikasi (misal: ACERPURE vs ACER)
    brand_db_sorted = sorted(brand_db, key=len, reverse=True)
    
    brands = []
    for _, row in df.iterrows():
        product_name = str(row[NAMA_PRODUK_COL]).upper()
        found_brand = None

        # Saringan Lapis 1: Kamus Alias (Paling Akurat)
        for alias, brand_utama in kamus_brand.items():
            # Menggunakan regex untuk mencari kata utuh
            if re.search(r'\b' + re.escape(str(alias).upper()) + r'\b', product_name):
                found_brand = brand_utama
                break
        if found_brand:
            brands.append(found_brand)
            continue

        # Saringan Lapis 2: Database Resmi (Substring & Kata Utuh)
        for brand in brand_db_sorted:
            if re.search(r'\b' + re.escape(brand.upper()) + r'\b', product_name):
                found_brand = brand
                break
            # Untuk kasus "Lenovo22"
            if brand.upper() in product_name.replace(" ", ""):
                found_brand = brand
                break
        if found_brand:
            brands.append(found_brand)
            continue
            
        # Saringan Lapis 3: Fuzzy Matching (Untuk Typo)
        # Kita pecah nama produk menjadi kata-kata untuk perbandingan yang lebih baik
        words_in_product = re.findall(r'\b\w+\b', product_name)
        best_match = process.extractOne(product_name, brand_db, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold:
            found_brand = best_match[0]
        
        if found_brand:
            brands.append(found_brand)
            continue

        # Saringan Lapis 4: Gagal, tandai untuk review
        brands.append("TIDAK DIKETAHUI")

    df['BRAND'] = brands
    return df

def update_google_sheet(gsheets_service, spreadsheet_id, sheet_name, values):
    """Fungsi untuk menambahkan baris baru ke Google Sheet."""
    try:
        sheet = gsheets_service.open_by_key(spreadsheet_id).worksheet(sheet_name)
        sheet.append_row(values, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Gagal mengupdate Google Sheet: {e}")
        return False

# --- TAMPILAN APLIKASI STREAMLIT ---

st.title("📊 Dashboard Analisis Brand & Kompetitor v2.0")
st.markdown("---")

# 1. Inisialisasi & Memuat Data
drive_service, gsheets_service = get_google_apis()

if 'master_df' not in st.session_state:
    brand_db, kamus_brand = load_brand_intelligence(gsheets_service, SPREADSHEET_ID)
    raw_df = get_all_competitor_data(drive_service, PARENT_FOLDER_ID)
    
    if not raw_df.empty:
        st.session_state.brand_db = brand_db
        st.session_state.kamus_brand = kamus_brand
        st.session_state.master_df = label_brands(raw_df, brand_db, kamus_brand)
    else:
        st.info("Tidak ada data yang ditemukan di folder Google Drive Anda.")
        st.stop()

# 2. Tampilan Analisis Utama (Mirip V1)
st.header("📈 Analisis Gabungan")

if 'master_df' in st.session_state:
    df_final = st.session_state.master_df
    st.write(f"Total data yang berhasil diproses: **{len(df_final)} baris**.")
    st.dataframe(df_final)

    st.subheader("Statistik Deskriptif")
    st.write(df_final.describe(include='all'))

# 3. Ruang Kontrol Interaktif untuk Review
st.markdown("---")
st.header("🧠 Ruang Kontrol: Latih Sistem Anda")

unknown_df = df_final[df_final['BRAND'] == 'TIDAK DIKETAHUI']

if unknown_df.empty:
    st.success("🎉 Hebat! Semua produk sudah berhasil dikenali oleh sistem.")
else:
    st.warning(f"Ditemukan **{len(unknown_df)} produk** yang brand-nya tidak dikenali.")
    
    with st.expander("Buka untuk mereview dan melatih sistem"):
        
        # Ambil satu produk untuk direview
        product_to_review = unknown_df.iloc[0]
        st.write("Produk yang perlu direview:")
        st.info(f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review['Toko']})")

        with st.form(key="review_form"):
            st.write("**Apa brand yang benar untuk produk ini?**")
            
            # Opsi 1: Brand sudah ada di database
            existing_brand_options = [""] + sorted(st.session_state.brand_db)
            selected_brand = st.selectbox("Pilih dari brand yang sudah ada:", options=existing_brand_options)
            
            # Opsi 2: Ini adalah brand yang benar-benar baru
            new_brand_input = st.text_input("Atau, masukkan nama brand BARU:")
            
            # Opsi 3: Ini adalah alias/sub-brand
            alias_input = st.text_input("Jika ini adalah ALIAS/TYPO, masukkan aliasnya di sini (misal: MI, ROG, Alactroz):")
            
            submitted = st.form_submit_button("Ajarkan ke Sistem!")

            if submitted:
                correction_made = False
                # Logika untuk memproses input dari form
                if alias_input and selected_brand:
                    # Kasus: Menambahkan alias baru untuk brand yang sudah ada
                    if update_google_sheet(gsheets_service, SPREADSHEET_ID, KAMUS_SHEET_NAME, [alias_input.strip().upper(), selected_brand]):
                        st.success(f"Pelajaran baru disimpan: '{alias_input.upper()}' sekarang akan dikenali sebagai '{selected_brand}'.")
                        correction_made = True
                    
                elif new_brand_input:
                    # Kasus: Menambahkan brand baru ke database
                    new_brand_upper = new_brand_input.strip().upper()
                    if new_brand_upper not in st.session_state.brand_db:
                        if update_google_sheet(gsheets_service, SPREADSHEET_ID, DB_SHEET_NAME, [new_brand_upper]):
                            st.success(f"Brand baru ditambahkan ke database: '{new_brand_upper}'.")
                            # Jika alias juga diisi, langsung petakan
                            if alias_input:
                                update_google_sheet(gsheets_service, SPREADSHEET_ID, KAMUS_SHEET_NAME, [alias_input.strip().upper(), new_brand_upper])
                            correction_made = True
                    else:
                        st.warning(f"Brand '{new_brand_upper}' sudah ada di database.")

                elif selected_brand and not alias_input:
                    st.error("Untuk memilih brand yang sudah ada, Anda harus mengisi kolom ALIAS/TYPO.")
                
                else:
                    st.error("Mohon isi form dengan benar.")

                if correction_made:
                    # Hapus cache dan state untuk memaksa reload & re-labeling
                    st.cache_data.clear()
                    st.cache_resource.clear()
                    del st.session_state.master_df
                    st.rerun()
