# ===================================================================================
#  DASHBOARD ANALISIS BRAND & KOMPETITOR - VERSI 2.0 (LENGKAP & Sesuai V1)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Fitur Utama:
#  - Otomatisasi Labeling Brand dengan Sistem Multi-Lapis
#  - "Ruang Kontrol" Interaktif untuk Melatih Sistem (Human-in-the-Loop)
#  - Tab Analisis Mendalam (Brand, Toko, Produk Baru per Minggu)
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
import plotly.express as px

# --- KONFIGURASI HALAMAN & ID ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis Brand v2.0")

# ID ini didapatkan dari URL Google Drive & Google Sheets Anda
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq"
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg"
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
NAMA_PRODUK_COL = "Nama Produk" # Sesuaikan jika nama kolom produk Anda berbeda
HARGA_COL = "Harga" # Sesuaikan jika nama kolom harga Anda berbeda

# --- FUNGSI-FUNGSI UTAMA (Dengan Caching untuk Performa) ---

@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    """Melakukan autentikasi sekali dan mengembalikan service object untuk Drive dan Sheets."""
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
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
    """Memuat database brand resmi dan kamus alias dari Google Sheet."""
    try:
        spreadsheet = _gsheets_service.open_by_key(spreadsheet_id)
        db_sheet = spreadsheet.worksheet(DB_SHEET_NAME)
        brand_db_list = [item for item in db_sheet.col_values(1) if item]
        kamus_sheet = spreadsheet.worksheet(KAMUS_SHEET_NAME)
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        if 'Alias' not in kamus_df.columns or 'Brand_Utama' not in kamus_df.columns:
            st.error("File 'kamus_brand' harus memiliki kolom 'Alias' dan 'Brand_Utama'.")
            return [], {}
        kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()
        return brand_db_list, kamus_dict
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet. Error: {e}")
        st.stop()

@st.cache_data(show_spinner="Membaca semua data dari folder kompetitor...", ttl=300)
def get_all_competitor_data(_drive_service, parent_folder_id):
    """Membaca semua file CSV dari semua subfolder di dalam folder induk Google Drive."""
    all_data = []
    try:
        query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        results = _drive_service.files().list(q=query, fields="files(id, name)").execute()
        subfolders = results.get('files', [])

        if not subfolders:
            st.warning("Tidak ada subfolder yang ditemukan di dalam folder induk.")
            return pd.DataFrame()

        for folder in subfolders:
            file_query = f"'{folder['id']}' in parents and mimeType = 'text/csv'"
            file_results = _drive_service.files().list(q=file_query, fields="files(id, name)").execute()
            csv_files = file_results.get('files', [])
            for csv_file in csv_files:
                request = _drive_service.files().get_media(fileId=csv_file['id'])
                downloader = io.BytesIO(request.execute())
                df = pd.read_csv(downloader)
                df['Toko'] = folder['name']
                match = re.search(r'(\d{2}-\d{2}-\d{4})', csv_file['name'])
                if match:
                    df['Tanggal'] = pd.to_datetime(match.group(1), format='%d-%m-%Y')
                else:
                    df['Tanggal'] = pd.NaT
                all_data.append(df)
        
        if not all_data: return pd.DataFrame()
        return pd.concat(all_data, ignore_index=True)
    except Exception as e:
        st.error(f"Terjadi kesalahan saat mengambil data CSV: {e}")
        return pd.DataFrame()

def label_brands(df, brand_db, kamus_brand, fuzzy_threshold=88):
    """Fungsi utama untuk melabeli brand dengan strategi multi-lapis."""
    if NAMA_PRODUK_COL not in df.columns:
        st.error(f"Data CSV tidak memiliki kolom '{NAMA_PRODUK_COL}'.")
        st.stop()
    brand_db_sorted = sorted(brand_db, key=len, reverse=True)
    brands = []
    for _, row in df.iterrows():
        product_name = str(row[NAMA_PRODUK_COL]).upper()
        found_brand = None
        for alias, brand_utama in kamus_brand.items():
            if re.search(r'\b' + re.escape(str(alias).upper()) + r'\b', product_name):
                found_brand = brand_utama
                break
        if found_brand: brands.append(found_brand); continue
        for brand in brand_db_sorted:
            if re.search(r'\b' + re.escape(brand.upper()) + r'\b', product_name) or (brand.upper() in product_name.replace(" ", "")):
                found_brand = brand
                break
        if found_brand: brands.append(found_brand); continue
        best_match = process.extractOne(product_name, brand_db, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold:
            found_brand = best_match[0]
        brands.append(found_brand if found_brand else "TIDAK DIKETAHUI")
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

# 1. Inisialisasi & Memuat Data
drive_service, gsheets_service = get_google_apis()
if 'master_df' not in st.session_state:
    brand_db, kamus_brand = load_brand_intelligence(gsheets_service, SPREADSHEET_ID)
    raw_df = get_all_competitor_data(drive_service, PARENT_FOLDER_ID)
    if not raw_df.empty:
        st.session_state.brand_db = brand_db
        st.session_state.kamus_brand = kamus_brand
        st.session_state.master_df = label_brands(raw_df.copy(), brand_db, kamus_brand)
    else:
        st.info("Tidak ada data yang ditemukan di folder Google Drive Anda.")
        st.stop()

df_final = st.session_state.master_df
df_labeled = df_final[df_final['BRAND'] != 'TIDAK DIKETAHUI'].copy()

# --- SIDEBAR FILTER ---
st.sidebar.header("Filter Analisis")
all_brands = sorted(df_labeled['BRAND'].unique())
selected_brands = st.sidebar.multiselect("Pilih Brand:", all_brands, default=all_brands)

all_stores = sorted(df_labeled['Toko'].unique())
selected_stores = st.sidebar.multiselect("Pilih Toko:", all_stores, default=all_stores)

if not selected_brands or not selected_stores:
    st.warning("Silakan pilih minimal satu Brand dan satu Toko untuk memulai analisis.")
    st.stop()

df_filtered = df_labeled[(df_labeled['BRAND'].isin(selected_brands)) & (df_labeled['Toko'].isin(selected_stores))]

# --- TAMPILAN UTAMA DENGAN TAB ---
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard Utama", "🏢 Analisis Toko", "🏷️ Analisis Brand", "🔍 Produk Baru", "🧠 Ruang Kontrol"])

with tab1:
    st.header("Dashboard Utama")
    st.write(f"Menampilkan data untuk **{len(selected_brands)}** brand di **{len(selected_stores)}** toko.")
    
    # PERBAIKAN: Menambahkan formatting harga
    df_display = df_filtered.copy()
    if HARGA_COL in df_display.columns:
        df_display[HARGA_COL] = df_display[HARGA_COL].apply(lambda x: f"Rp {x:,.0f}" if pd.notnull(x) else "N/A")
    st.dataframe(df_display)

with tab2:
    st.header("Analisis Per Toko")
    st.markdown("Distribusi jumlah produk per brand di setiap toko yang dipilih.")
    store_brand_counts = df_filtered.groupby(['Toko', 'BRAND']).size().reset_index(name='Jumlah Produk')
    fig = px.bar(store_brand_counts, x='Toko', y='Jumlah Produk', color='BRAND',
                 title='Jumlah Produk per Brand di Setiap Toko',
                 labels={'Jumlah Produk': 'Total Produk', 'Toko': 'Nama Toko'},
                 barmode='stack')
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("Analisis Per Brand")
    st.markdown("Distribusi jumlah produk per toko untuk setiap brand yang dipilih.")
    brand_store_counts = df_filtered.groupby(['BRAND', 'Toko']).size().reset_index(name='Jumlah Produk')
    fig = px.bar(brand_store_counts, x='BRAND', y='Jumlah Produk', color='Toko',
                 title='Jumlah Produk per Toko untuk Setiap Brand',
                 labels={'Jumlah Produk': 'Total Produk', 'BRAND': 'Nama Brand'},
                 barmode='group')
    st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("Perbandingan Produk Baru")
    st.markdown("Bandingkan daftar produk antara dua minggu untuk menemukan produk baru.")
    
    # PERBAIKAN: Menggunakan logika perbandingan per MINGGU, bukan per TANGGAL
    df_with_week = df_filtered.copy()
    df_with_week.dropna(subset=['Tanggal'], inplace=True)
    df_with_week['Minggu'] = df_with_week['Tanggal'].dt.strftime('%Y-%U') # Format Tahun-Mingguke
    
    available_weeks = sorted(df_with_week['Minggu'].unique())
    
    if len(available_weeks) < 2:
        st.info("Perlu minimal data dari dua minggu berbeda untuk melakukan perbandingan.")
    else:
        col1, col2 = st.columns(2)
        week_before = col1.selectbox("Pilih Minggu Pembanding:", available_weeks, index=0)
        week_after = col2.selectbox("Pilih Minggu Penentu:", available_weeks, index=len(available_weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            for store in selected_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_with_week[(df_with_week['Toko'] == store) & (df_with_week['Minggu'] == week_before)][NAMA_PRODUK_COL])
                    products_after = set(df_with_week[(df_with_week['Toko'] == store) & (df_with_week['Minggu'] == week_after)][NAMA_PRODUK_COL])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_with_week[df_with_week[NAMA_PRODUK_COL].isin(new_products) & (df_with_week['Toko'] == store) & (df_with_week['Minggu'] == week_after)]
                        
                        # PERBAIKAN: Menambahkan formatting harga juga di sini
                        if HARGA_COL in new_products_df.columns:
                            new_products_df[HARGA_COL] = new_products_df[HARGA_COL].apply(lambda x: f"Rp {x:,.0f}" if pd.notnull(x) else "N/A")
                        st.dataframe(new_products_df[[NAMA_PRODUK_COL, HARGA_COL, 'BRAND']])

with tab5:
    st.header("Ruang Kontrol: Latih Sistem Anda")
    unknown_df = st.session_state.master_df[st.session_state.master_df['BRAND'] == 'TIDAK DIKETAHUI']

    if unknown_df.empty:
        st.success("🎉 Hebat! Semua produk sudah berhasil dikenali oleh sistem.")
    else:
        st.warning(f"Ditemukan **{len(unknown_df)} produk** yang brand-nya tidak dikenali.")
        
        product_to_review = unknown_df.iloc[0]
        st.write("Produk yang perlu direview:")
        st.info(f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review['Toko']})")

        with st.form(key="review_form"):
            st.write("**Apa brand yang benar untuk produk ini?**")
            col1, col2 = st.columns(2)
            selected_brand = col1.selectbox("Pilih dari brand yang sudah ada:", options=[""] + sorted(st.session_state.brand_db))
            new_brand_input = col2.text_input("Atau, masukkan nama brand BARU:")
            alias_input = st.text_input("Jika ini adalah ALIAS/TYPO, masukkan di sini (misal: MI, ROG, Alactroz):")
            
            submitted = st.form_submit_button("Ajarkan ke Sistem!")

            if submitted:
                correction_made = False
                if alias_input and selected_brand:
                    if update_google_sheet(gsheets_service, SPREADSHEET_ID, KAMUS_SHEET_NAME, [alias_input.strip().upper(), selected_brand]):
                        st.success(f"Pelajaran baru disimpan: '{alias_input.upper()}' sekarang akan dikenali sebagai '{selected_brand}'.")
                        correction_made = True
                elif new_brand_input:
                    new_brand_upper = new_brand_input.strip().upper()
                    if new_brand_upper not in st.session_state.brand_db:
                        if update_google_sheet(gsheets_service, SPREADSHEET_ID, DB_SHEET_NAME, [new_brand_upper]):
                            st.success(f"Brand baru ditambahkan: '{new_brand_upper}'.")
                            if alias_input: update_google_sheet(gsheets_service, SPREADSHEET_ID, KAMUS_SHEET_NAME, [alias_input.strip().upper(), new_brand_upper])
                            correction_made = True
                    else: st.warning(f"Brand '{new_brand_upper}' sudah ada.")
                elif selected_brand and not alias_input: st.error("Untuk memilih brand yang sudah ada, Anda harus mengisi kolom ALIAS/TYPO.")
                else: st.error("Mohon isi form dengan benar.")

                if correction_made:
                    st.cache_data.clear()
                    st.cache_resource.clear()
                    del st.session_state.master_df
                    st.rerun()
