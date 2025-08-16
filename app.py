# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR
#  Direplikasi dan dikembangkan berdasarkan v3.4 Final
# ===================================================================================

# --- Impor Pustaka/Library ---
# Pustaka-pustaka ini akan kita butuhkan untuk keseluruhan aplikasi
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io
from thefuzz import process, fuzz
import re
import plotly.express as px
import time
from typing import Callable, Any, Dict, List, Optional, Tuple, Set

# --- Konfigurasi Halaman Utama ---
# Mengatur judul tab di browser, ikon, dan layout halaman menjadi lebar
st.set_page_config(
    layout="wide", 
    page_title="Dashboard Analisis Kompetitor"
)

# =====================================================================================
# BLOK KONFIGURASI UTAMA
# Di sini kita mendefinisikan semua variabel penting agar mudah diubah di satu tempat.
# =====================================================================================

# --- ID Google Drive & Sheets ---
# ID Folder Induk tempat semua folder proyek berada
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq" 
# Nama folder tempat Anda mengunggah data mentah (CSV dari toko-toko)
DATA_MENTAH_FOLDER_NAME = "data_upload"
# Nama folder tempat aplikasi akan menyimpan data olahan (cache parquet)
DATA_OLAHAN_FOLDER_NAME = "processed_data"
# Nama file untuk cache data olahan
CACHE_FILE_NAME = "master_data.parquet"
# ID dari Google Sheet yang berfungsi sebagai "otak" atau database aplikasi
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg"

# --- Nama Worksheet di dalam Google Sheet "Otak" ---
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
KATEGORI_SHEET_NAME = "DATABASE"

# --- Standardisasi Nama Kolom ---
# Mendefinisikan nama kolom standar yang akan digunakan di seluruh aplikasi
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
# Daftar kolom yang wajib ada di setiap file data mentah
REQUIRED_COLUMNS = {NAMA_PRODUK_COL, HARGA_COL, TERJUAL_COL}

# =====================================================================================
# Bagian 2: Utilities (Fungsi Bantuan)
# Kumpulan fungsi kecil untuk membuat aplikasi lebih tangguh dan pintar.
# =====================================================================================

def with_retry(fn: Callable, max_attempts: int = 4, base_delay: float = 1.0, exc_types: Tuple = (Exception,),
            fail_msg: Optional[str] = None):
    """
    Decorator untuk mencoba ulang sebuah fungsi jika gagal.
    Sangat berguna untuk operasi jaringan (API calls) yang bisa gagal sesaat.
    """
    def wrapper(*args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                # Mencoba menjalankan fungsi aslinya
                return fn(*args, **kwargs)
            except exc_types as e:
                # Jika gagal dan ini adalah percobaan terakhir, tampilkan error dan hentikan
                if attempt == max_attempts:
                    if fail_msg:
                        st.error(f"{fail_msg}: {e}")
                    raise
                # Jika bukan percobaan terakhir, tunggu sejenak sebelum mencoba lagi
                # Waktu tunggu akan semakin lama setiap kali gagal (1s, 2s, 3s, ...)
                time.sleep(base_delay * attempt)
    return wrapper

# Daftar format encoding dan pemisah (separator) yang umum untuk file CSV
CSV_POSSIBLE_ENCODINGS = ["utf-8", "utf-8-sig", "latin1", "iso-8859-1"]
CSV_POSSIBLE_SEPARATORS = [",", ";", "\t", "|"]

def read_csv_safely(byte_stream: io.BytesIO) -> pd.DataFrame:
    """
    Membaca data CSV dari byte stream dengan mencoba berbagai kombinasi
    encoding dan separator untuk memaksimalkan keberhasilan pembacaan.
    """
    # Mengembalikan pointer ke awal file stream
    byte_stream.seek(0) 
    # Membaca seluruh konten byte untuk digunakan ulang
    raw_bytes = byte_stream.read()
    
    for enc in CSV_POSSIBLE_ENCODINGS:
        for sep in CSV_POSSIBLE_SEPARATORS:
            try:
                # Membuat stream baru dari byte yang sudah dibaca
                df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, sep=sep)
                # Jika pembacaan berhasil (menghasilkan lebih dari 1 kolom), kembalikan hasilnya
                if len(df.columns) > 1:
                    return df
            except Exception:
                # Jika gagal, coba kombinasi berikutnya
                pass
                
    # Jika semua kombinasi gagal, coba cara standar Pandas sebagai usaha terakhir
    return pd.read_csv(io.BytesIO(raw_bytes))

# =====================================================================================
# Bagian 3: Fungsi-fungsi Inti (Backend)
# Kumpulan fungsi utama untuk otentikasi, navigasi Drive, dan pengambilan data.
# =====================================================================================

# --- Fungsi Otentikasi & Koneksi ---
@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    """
    Membuat koneksi aman ke Google Drive dan Google Sheets API.
    Menggunakan @st.cache_resource agar koneksi ini dibuat sekali saja.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Mengambil kredensial dari Streamlit Secrets
    creds = Credentials.from_service_account_info(
        st.secrets["google_service_account"],
        scopes=scopes
    )
    
    # Membuat service client untuk Google Drive API v3
    drive_service = build("drive", "v3", credentials=creds)
    
    # Membuat service client untuk Google Sheets API (via gspread)
    gsheets_service = gspread.authorize(creds)
    
    return drive_service, gsheets_service


# --- Fungsi untuk Manajemen Folder di Google Drive ---
@st.cache_data(show_spinner="Mencari ID folder di Google Drive...")
def find_folder_id(_drive_service, parent_id, folder_name):
    """
    Mencari ID dari sebuah folder berdasarkan namanya di dalam folder induk (parent).
    """
    query = (
        f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' "
        f"and name = '{folder_name}' and trashed = false"
    )

    # Menggunakan fungsi with_retry yang sudah kita buat
    @with_retry(fail_msg=f"Gagal mencari folder '{folder_name}' setelah beberapa kali percobaan.")
    def _list_folders():
        return _drive_service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,      # Wajib untuk Shared Drive
            includeItemsFromAllDrives=True # Wajib untuk Shared Drive
        ).execute()

    response = _list_folders()
    files = response.get("files", [])
    if files:
        return files[0].get("id")
    
    # Jika folder tidak ditemukan, hentikan aplikasi dan beri pesan error
    st.error(f"FATAL: Folder '{folder_name}' tidak ditemukan di dalam folder induk (ID: {parent_id}).")
    st.info("Pastikan nama folder di Google Drive sama persis dengan yang ada di konfigurasi.")
    st.stop()


# --- Fungsi untuk Memuat Data "Otak" (Database Brand, Kamus, Kategori) ---
@st.cache_data(show_spinner="Memuat 'otak' dari Google Sheets...", ttl=3600)
def load_intelligence_data(_gsheets_service, spreadsheet_id):
    """
    Mengambil semua data 'pintar' dari Google Sheet (database brand, kamus, kategori).
    Data di-cache selama 1 jam (3600 detik) untuk efisiensi.
    """
    try:
        spreadsheet = _gsheets_service.open_by_key(spreadsheet_id)

        # 1. Mengambil database brand utama (hanya kolom pertama)
        db_sheet = spreadsheet.worksheet(DB_SHEET_NAME)
        # Mengambil semua nilai di kolom pertama yang tidak kosong
        brand_db_list = [item for item in db_sheet.col_values(1) if item]

        # 2. Mengambil kamus alias brand dan mengubahnya menjadi dictionary
        kamus_sheet = spreadsheet.worksheet(KAMUS_SHEET_NAME)
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        # Mengubah DataFrame menjadi dictionary agar pencarian lebih cepat (Alias -> Brand_Utama)
        kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()

        # 3. Mengambil database produk untuk pemetaan kategori
        kategori_sheet = spreadsheet.worksheet(KATEGORI_SHEET_NAME)
        db_kategori_df = pd.DataFrame(kategori_sheet.get_all_records())
        # Menyeragamkan nama kolom menjadi huruf besar
        db_kategori_df.columns = [str(col).strip().upper() for col in db_kategori_df.columns]

        return brand_db_list, kamus_dict, db_kategori_df
    
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"FATAL: Worksheet '{e.args[0]}' tidak ditemukan di Google Sheet 'Otak'. Proses dihentikan.")
        st.stop()
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet 'Otak'. Error: {e}")
        st.stop()

# --- Fungsi untuk Membaca Semua Data Mentah dari Drive ---
@st.cache_data(show_spinner="Membaca semua data mentah dari folder kompetitor...", ttl=3600)
def get_raw_data_from_drive(_drive_service, data_mentah_folder_id):
    """
    Mengambil semua file data dari setiap subfolder toko di dalam folder 'data_upload',
    menggabungkannya menjadi satu DataFrame besar.
    """
    all_data = []
    
    # 1. Cari semua subfolder (toko) di dalam folder 'data_upload'
    query_subfolders = f"'{data_mentah_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    subfolders = _drive_service.files().list(q=query_subfolders, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])

    if not subfolders:
        st.warning("Tidak ada folder toko yang ditemukan di dalam folder 'data_upload'.")
        return pd.DataFrame()

    progress_bar = st.progress(0, text="Memulai pembacaan data...")
    date_regex = re.compile(r"(\d{4}[-_]\d{2}[-_]\d{2})") # Pola untuk mengekstrak tanggal dari nama file

    # 2. Iterasi melalui setiap folder toko
    for i, folder in enumerate(subfolders):
        progress_text = f"Membaca folder toko: {folder['name']}..."
        progress_bar.progress((i + 1) / len(subfolders), text=progress_text)
        
        # 3. Cari semua file CSV atau Google Sheet di dalam folder toko
        file_query = f"'{folder['id']}' in parents and (mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet') and trashed = false"
        files_in_folder = _drive_service.files().list(q=file_query, fields="files(id, name, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])

        # 4. Iterasi melalui setiap file
        for file_item in files_in_folder:
            try:
                # 5. Download atau Export file menjadi format yang bisa dibaca Pandas
                if file_item.get("mimeType") == 'application/vnd.google-apps.spreadsheet':
                    # Jika file adalah Google Sheet, export sebagai CSV
                    request = _drive_service.files().export_media(fileId=file_item.get("id"), mimeType="text/csv", supportsAllDrives=True)
                    content = io.BytesIO(request.execute())
                else:
                    # Jika file adalah CSV, download langsung
                    request = _drive_service.files().get_media(fileId=file_item.get("id"), supportsAllDrives=True)
                    buf = io.BytesIO()
                    downloader = MediaIoBaseDownload(buf, request)
                    done = False
                    while not done: _, done = downloader.next_chunk()
                    content = buf

                # 6. Baca file CSV menggunakan fungsi aman yang sudah kita buat
                df = read_csv_safely(content)

                # 7. Tambahkan data-data penting (metadata)
                df[TOKO_COL] = folder["name"] # Nama Toko
                
                # Ekstrak tanggal dari nama file
                match = date_regex.search(file_item.get("name"))
                df[TANGGAL_COL] = pd.to_datetime(match.group(1).replace("_", "-")) if match else pd.NaT
                
                # Tentukan status 'Tersedia' atau 'Habis' dari nama file
                df[STATUS_COL] = "Tersedia" if "ready" in file_item.get("name").lower() else "Habis"

                # 8. Validasi: Pastikan kolom wajib ada
                if not REQUIRED_COLUMNS.issubset(df.columns):
                    st.warning(f"File '{file_item.get('name')}' di folder '{folder['name']}' dilewati karena ada kolom wajib yang hilang.")
                    continue

                all_data.append(df)
            except Exception as e:
                st.error(f"Gagal memproses file '{file_item.get('name')}' di folder '{folder['name']}'. Error: {e}. File dilewati.")
                continue

    progress_bar.empty()
    if not all_data: return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

# =====================================================================================
# Bagian 4: Fungsi-fungsi Pemrosesan Data
# Di sinilah data mentah dibersihkan, diperkaya, dan diberi label (brand & kategori).
# =====================================================================================

def process_raw_data(raw_df: pd.DataFrame, brand_db: List[str], kamus_brand: Dict[str, str], db_kategori: pd.DataFrame):
    """
    Orkestrator utama untuk membersihkan, memperkaya, dan melabeli data mentah.
    """
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()
    st.write("Memulai pembersihan dan standarisasi data...")

    # 1. Konversi tipe data yang aman, mengisi nilai kosong dengan 0
    df[HARGA_COL] = pd.to_numeric(df.get(HARGA_COL), errors="coerce").fillna(0)
    df[TERJUAL_COL] = pd.to_numeric(df.get(TERJUAL_COL), errors="coerce").fillna(0).astype(int)

    # 2. Hitung Omzet
    df[OMZET_COL] = df[HARGA_COL] * df[TERJUAL_COL]

    # 3. Labeling Brand (dengan optimasi cache internal)
    st.write("Memulai proses labeling brand...")
    df = label_brands(df, brand_db, kamus_brand)

    # 4. Pemetaan Kategori
    st.write("Memulai proses pemetaan kategori...")
    df = map_categories(df, db_kategori)

    st.success("Semua proses pengolahan data selesai!")
    return df


def label_brands(df: pd.DataFrame, brand_db: List[str], kamus_brand: Dict[str, str], fuzzy_threshold: int = 88):
    """
    Melabeli brand pada nama produk menggunakan logika 3 tahap yang dioptimalkan.
    Menggunakan RegEx untuk akurasi dan cache internal untuk kecepatan.
    """
    # Mengurutkan DB brand dari yang terpanjang agar tidak salah cocok (misal: "ASUS ROG" vs "ASUS")
    brand_db_sorted = sorted([b for b in brand_db if isinstance(b, str)], key=len, reverse=True)

    # Pra-kompilasi pola RegEx untuk pencarian super cepat
    alias_patterns = [
        (re.compile(rf"\\b{re.escape(str(alias).upper())}\\b"), str(main).upper())
        for alias, main in kamus_brand.items()
    ]
    brand_patterns = [
        (re.compile(rf"\\b{re.escape(b.upper())}\\b"), b)
        for b in brand_db_sorted
    ]

    # Cache internal untuk menyimpan hasil agar tidak menganalisis nama produk yang sama berulang kali
    name_cache: Dict[str, str] = {}

    def _find_brand(name_upper: str) -> str:
        # Prioritas 1: Pencocokan dengan Kamus Alias (paling akurat)
        for pat, main_brand in alias_patterns:
            if pat.search(name_upper):
                return main_brand
        
        # Prioritas 2: Pencocokan dengan Database Brand
        for pat, brand in brand_patterns:
            if pat.search(name_upper):
                return brand
        
        # Prioritas 3: Fuzzy Matching (jika semua cara di atas gagal)
        best_match = process.extractOne(name_upper, brand_db_sorted, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold:
            return best_match[0]
            
        return "TIDAK DIKETAHUI"

    # Terapkan fungsi _find_brand ke setiap baris di kolom Nama Produk
    brands = []
    for product_name in df[NAMA_PRODUK_COL].astype(str):
        upper_name = product_name.upper()
        if upper_name in name_cache:
            brands.append(name_cache[upper_name])
        else:
            found_brand = _find_brand(upper_name)
            name_cache[upper_name] = found_brand
            brands.append(found_brand)

    df[BRAND_COL] = brands
    return df


@st.cache_data(show_spinner="Memetakan kategori produk...")
def map_categories(_df: pd.DataFrame, _db_kategori: pd.DataFrame):
    """
    Memetakan setiap produk ke kategori yang paling sesuai dari database
    menggunakan fuzzy matching.
    """
    df_copy = _df.copy()
    df_copy[KATEGORI_COL] = "Lainnya" # Default kategori
    
    # Cek jika database kategori valid
    if _db_kategori.empty or "NAMA" not in _db_kategori.columns or "KATEGORI" not in _db_kategori.columns:
        return df_copy

    # Siapkan data untuk pencocokan
    db_map = _db_kategori.set_index("NAMA")["KATEGORI"]
    
    # Terapkan pemetaan kategori
    for idx, row in df_copy.iterrows():
        product_name = str(row.get(NAMA_PRODUK_COL, ""))
        if product_name:
            # Cari nama produk di database yang paling mirip
            match, score = process.extractOne(product_name, db_map.index, scorer=fuzz.token_set_ratio)
            # Jika tingkat kemiripan tinggi, petakan kategorinya
            if score >= 95:
                df_copy.at[idx, KATEGORI_COL] = db_map[match]
                
    return df_copy

# =====================================================================================
# Bagian 5: Fungsi-fungsi Tampilan (Frontend)
# Semua fungsi yang berhubungan dengan antarmuka pengguna (UI) dan visualisasi.
# =====================================================================================

# --- Fungsi Bantuan untuk UI (UI Helper Functions) ---

def format_harga(x):
    """Mengubah angka menjadi format mata uang Rupiah."""
    if pd.isnull(x): return "N/A"
    return f"Rp {float(x):,.0f}"

def format_wow_growth(pct_change):
    """Memberi ikon ▲/▼ pada persentase pertumbuhan mingguan."""
    if pd.isna(pct_change) or pct_change == float("inf"): return "N/A"
    if pct_change > 0.001: return f"▲ {pct_change:.1%}"
    if pct_change < -0.001: return f"▼ {pct_change:.1%}"
    return f"▬ 0.0%"

def colorize_growth(val):
    """Memberi warna hijau/merah pada teks pertumbuhan."""
    color = "grey"
    if isinstance(val, str):
        if "▲" in val: color = "#28a745" # Hijau
        elif "▼" in val: color = "#dc3545" # Merah
    return f"color: {color}"

@st.cache_data
def convert_df_to_csv(df):
    """Mengubah DataFrame menjadi CSV untuk di-download."""
    return df.to_csv(index=False).encode("utf-8")

# --- Tampilan Mode Koreksi Cerdas (Interactive Batch Correction) ---

# =====================================================================================
# (Letakkan kode ini di Bagian 6: Fungsi-fungsi Tampilan (Frontend))
# =====================================================================================

def _tokenize_words_upper(name: str) -> List[str]:
    """Fungsi bantuan untuk memecah nama produk menjadi kata-kata penting (token)."""
    up = str(name).upper()
    # Memisahkan berdasarkan karakter non-alfanumerik
    tokens = re.split(r'[^A-Z0-9]+', up)
    # Mengabaikan kata-kata yang terlalu pendek (kurang dari 3 huruf)
    return [t for t in tokens if len(t) > 2]


def display_correction_mode(gsheets_service):
    st.header("🧠 Ruang Kontrol: Perbaikan Data Brand (Interaktif)")
    st.warning(
        "Ditemukan produk yang brand-nya tidak dikenali. Mohon ajari sistem satu per satu."
    )

    df_to_fix = st.session_state.df_to_fix
    unknown_products = df_to_fix[df_to_fix[BRAND_COL] == "TIDAK DIKETAHUI"]

    # Jika sudah tidak ada lagi yang perlu diperbaiki, simpan otomatis dan beralih ke dasbor
    if unknown_products.empty:
        st.success("🎉 Semua brand sudah dikenali! Menyimpan data bersih ke cache...")
        save_data_to_cache(
            st.session_state.drive_service,
            st.session_state.data_olahan_folder_id,
            CACHE_FILE_NAME,
            df_to_fix,
        )
        st.session_state.mode = "dashboard"
        st.session_state.master_df = df_to_fix.copy()
        time.sleep(1)
        st.rerun()
        return

    st.info(f"Tersisa **{len(unknown_products)} produk** yang perlu direview.")
    
    # Ambil produk pertama yang tidak dikenal untuk direview
    product_to_review = unknown_products.iloc[0]
    st.divider()
    st.write("Produk yang direview saat ini:")
    st.info(f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review[TOKO_COL]})")

    # Sistem akan menyarankan alias berdasarkan kata pertama dari nama produk
    suggested_tokens = _tokenize_words_upper(product_to_review[NAMA_PRODUK_COL])
    default_alias = suggested_tokens[0] if suggested_tokens else ""

    with st.form(key="review_form_interactive"):
        st.subheader("1. Apa Brand yang Benar?")
        col1, col2 = st.columns(2)
        brand_list = [""] + sorted(st.session_state.brand_db)
        selected_brand = col1.selectbox("Pilih dari brand yang ada:", options=brand_list)
        new_brand_input = col2.text_input("Atau, masukkan nama brand BARU:")

        st.subheader("2. Ajari Sistem Alias Baru")
        alias_input = st.text_input(
            "Alias untuk mendeteksi brand ini (direkomendasikan):",
            value=default_alias
        )

        st.subheader("3. Terapkan Pembelajaran Ini Untuk:")
        apply_mode = st.radio(
            "Pilih mode penerapan:",
            options=[
                "Hanya untuk produk ini saja",
                "Semua produk TIDAK DIKENALI yang mengandung alias di atas",
                "Semua produk TIDAK DIKENALI yang namanya mirip (fuzzy)"
            ],
            index=1,
            help="Gunakan 'mengandung alias' untuk perbaikan massal yang cepat dan akurat."
        )
        
        # Opsi fuzzy matching hanya muncul jika mode-nya dipilih
        fuzzy_threshold = 85
        if "mirip (fuzzy)" in apply_mode:
            fuzzy_threshold = st.slider("Tingkat kemiripan (%):", 80, 100, 90)

        submitted = st.form_submit_button("Ajarkan & Terapkan ke Berikutnya ▶")

        if submitted:
            final_brand = new_brand_input.strip().upper() if new_brand_input else selected_brand
            if not final_brand:
                st.error("Anda harus memilih atau memasukkan nama brand.")
                return

            # --- PROSES PEMBELAJARAN SISTEM ---
            # 1. Simpan brand baru ke Google Sheet jika ada
            if new_brand_input and final_brand not in st.session_state.brand_db:
                try:
                    sheet = gsheets_service.open_by_key(SPREADSHEET_ID).worksheet(DB_SHEET_NAME)
                    sheet.append_row([final_brand])
                    st.session_state.brand_db.append(final_brand) # Update list brand di memori
                    st.toast(f"Brand baru '{final_brand}' ditambahkan ke database.", icon="➕")
                except Exception as e: st.error(f"Gagal menyimpan brand baru: {e}")

            # 2. Simpan alias baru ke Google Sheet jika diisi
            alias_to_save = alias_input.strip().upper()
            if alias_to_save:
                try:
                    sheet = gsheets_service.open_by_key(SPREADSHEET_ID).worksheet(KAMUS_SHEET_NAME)
                    sheet.append_row([alias_to_save, final_brand])
                    st.toast(f"Alias '{alias_to_save}' disimpan.", icon="📚")
                except Exception as e: st.error(f"Gagal menyimpan alias: {e}")

            # --- PROSES PENERAPAN BATCH ---
            # 3. Tentukan baris mana saja yang akan diupdate
            indices_to_update = set()
            indices_to_update.add(product_to_review.name) # Selalu update produk saat ini

            unknown_df_subset = df_to_fix[df_to_fix[BRAND_COL] == "TIDAK DIKETAHUI"]

            if "mengandung alias" in apply_mode and alias_to_save:
                mask = unknown_df_subset[NAMA_PRODUK_COL].str.upper().str.contains(re.escape(alias_to_save), na=False)
                indices_to_update.update(unknown_df_subset[mask].index)

            elif "mirip (fuzzy)" in apply_mode:
                base_name = product_to_review[NAMA_PRODUK_COL]
                for idx, row in unknown_df_subset.iterrows():
                    score = fuzz.token_set_ratio(base_name, row[NAMA_PRODUK_COL])
                    if score >= fuzzy_threshold:
                        indices_to_update.add(idx)
            
            # 4. Terapkan brand baru ke semua baris yang terpilih
            st.session_state.df_to_fix.loc[list(indices_to_update), BRAND_COL] = final_brand
            st.success(f"Berhasil mengupdate brand '{final_brand}' ke **{len(indices_to_update)} produk**.")
            
            time.sleep(1)
            st.rerun()

# --- Tampilan Dasbor Utama (Multi-halaman) ---

def display_main_dashboard(df):
    st.sidebar.header("Navigasi Halaman")
    page = st.sidebar.radio("Pilih Halaman:", ["📈 Ringkasan Eksekutif", "🔍 Analisis Mendalam"])
    st.sidebar.divider()

    st.sidebar.header("Filter Global")
    # Filter Rentang Tanggal
    min_date, max_date = df[TANGGAL_COL].min().date(), df[TANGGAL_COL].max().date()
    selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date])

    if len(selected_date_range) != 2: st.stop()
    start_date, end_date = selected_date_range
    df_filtered = df[(df[TANGGAL_COL].dt.date >= start_date) & (df[TANGGAL_COL].dt.date <= end_date)]

    # --- Halaman 1: Ringkasan Eksekutif ---
    if page == "📈 Ringkasan Eksekutif":
        st.header("Ringkasan Eksekutif")
        latest_date = df_filtered[TANGGAL_COL].max()
        st.markdown(f"Menampilkan data terbaru per tanggal **{latest_date.strftime('%d %B %Y')}**")

        df_latest = df_filtered[df_filtered[TANGGAL_COL] == latest_date]
        
        # Metrik Utama
        total_omzet = df_latest[OMZET_COL].sum()
        total_unit = df_latest[TERJUAL_COL].sum()
        total_produk = df_latest[NAMA_PRODUK_COL].nunique()
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Omzet (Hari Ini)", format_harga(total_omzet))
        col2.metric("Total Unit Terjual (Hari Ini)", f"{total_unit:,.0f}")
        col3.metric("Jumlah Produk Unik (Hari Ini)", f"{total_produk:,.0f}")

        st.divider()

        # Visualisasi
        st.subheader("Perbandingan Omzet per Toko (Data Terbaru)")
        omzet_per_store = df_latest.groupby(TOKO_COL)[OMZET_COL].sum().sort_values(ascending=False).reset_index()
        fig_bar = px.bar(omzet_per_store, x=TOKO_COL, y=OMZET_COL, title=f"Total Omzet per Toko", text_auto=True)
        st.plotly_chart(fig_bar, use_container_width=True)

        st.subheader("Tabel Pertumbuhan Omzet Mingguan per Toko (%)")
        df_filtered['Minggu'] = df_filtered[TANGGAL_COL].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
        weekly_pivot = df_filtered.groupby(['Minggu', TOKO_COL])[OMZET_COL].sum().unstack().pct_change()
        st.dataframe(weekly_pivot.style.format(format_wow_growth).applymap(colorize_growth), use_container_width=True)

    # --- Halaman 2: Analisis Mendalam ---
    elif page == "🔍 Analisis Mendalam":
        st.header("Analisis Mendalam")
        
        # Filter tambahan khusus halaman ini
        all_stores = sorted(df_filtered[TOKO_COL].unique())
        selected_store = st.sidebar.selectbox("Pilih Toko untuk Dianalisis:", all_stores)

        df_store_filtered = df_filtered[df_filtered[TOKO_COL] == selected_store]

        tab1, tab2, tab3 = st.tabs(["🏆 Kinerja Brand", "📦 Analisis Produk", "📊 Analisis Kategori"])

        with tab1:
            st.subheader(f"Kinerja Brand di Toko {selected_store}")
            brand_performance = df_store_filtered.groupby(BRAND_COL).agg(
                Total_Omzet=(OMZET_COL, 'sum'),
                Jumlah_Produk=(NAMA_PRODUK_COL, 'nunique')
            ).sort_values("Total_Omzet", ascending=False).reset_index()

            fig_treemap = px.treemap(brand_performance, path=[px.Constant(selected_store), BRAND_COL], values='Total_Omzet', title="Peta Omzet Brand")
            st.plotly_chart(fig_treemap, use_container_width=True)

        with tab2:
            st.subheader(f"Produk Terlaris di Toko {selected_store}")
            top_products = df_store_filtered.sort_values(TERJUAL_COL, ascending=False).head(20)
            st.dataframe(top_products[[NAMA_PRODUK_COL, HARGA_COL, TERJUAL_COL, OMZET_COL, BRAND_COL, KATEGORI_COL]], use_container_width=True)
            
        with tab3:
            st.subheader(f"Kinerja Kategori di Toko {selected_store}")
            category_performance = df_store_filtered.groupby(KATEGORI_COL)[OMZET_COL].sum().sort_values(ascending=False).reset_index()
            fig_pie = px.pie(category_performance, names=KATEGORI_COL, values=OMZET_COL, title="Distribusi Omzet per Kategori")
            st.plotly_chart(fig_pie, use_container_width=True)
            
# =====================================================================================
# Bagian 6: Alur Kerja Utama Aplikasi (Main Flow)
# Bagian ini adalah "sutradara" yang mengontrol logika utama aplikasi.
# =====================================================================================

st.title("📊 Dasbor Analisis Penjualan & Kompetitor")
st.markdown("Sebuah aplikasi cerdas untuk mengubah data mentah menjadi wawasan bisnis.")

# --- Inisialisasi Session State ---
# `setdefault` hanya akan mengatur nilai jika kunci tersebut belum ada.
# Ini penting agar state tidak ter-reset setiap kali ada interaksi.
st.session_state.setdefault("mode", "initial")
st.session_state.setdefault("master_df", pd.DataFrame())
st.session_state.setdefault("df_to_fix", pd.DataFrame())
st.session_state.setdefault("brand_db", [])
st.session_state.setdefault("drive_service", None)
st.session_state.setdefault("gsheets_service", None)
st.session_state.setdefault("data_olahan_folder_id", None)

# --- Tombol Pemicu Utama di Sidebar ---
st.sidebar.header("Kontrol Utama")
st.sidebar.info(
    "Proses akan sangat cepat jika cache cerdas sudah ada di Google Drive."
)
if st.sidebar.button("🚀 Tarik & Proses Data Terbaru", type="primary"):
    with st.spinner("Memulai proses... Menghubungkan ke Google API..."):
        # Langkah 1: Otentikasi dan siapkan koneksi
        drive_service, gsheets_service = get_google_apis()
        st.session_state.drive_service = drive_service
        st.session_state.gsheets_service = gsheets_service

        # Langkah 2: Cari folder data mentah & olahan
        data_mentah_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_MENTAH_FOLDER_NAME)
        data_olahan_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_OLAHAN_FOLDER_NAME)
        st.session_state.data_olahan_folder_id = data_olahan_folder_id # Simpan untuk nanti

    with st.spinner("Memeriksa status cache cerdas di Google Drive..."):
        # Langkah 3: Cek apakah cache sudah ada
        cache_file_info = check_cache_exists(drive_service, data_olahan_folder_id, CACHE_FILE_NAME)

    # Langkah 4: Tentukan alur kerja berdasarkan keberadaan cache
    if cache_file_info:
        # --- JALUR CEPAT (CACHE DITEMUKAN) ---
        st.toast("Cache cerdas ditemukan! Memuat data...", icon="⚡")
        df = load_data_from_cache(drive_service, cache_file_info[0]["id"])
        st.session_state.master_df = df
        st.session_state.mode = "dashboard"
        st.success("Data dari cache berhasil dimuat!")

    else:
        # --- JALUR BERAT (CACHE TIDAK DITEMUKAN, PROSES DARI AWAL) ---
        st.toast("Cache cerdas tidak ditemukan. Memulai proses dari awal...", icon="⚙️")
        
        # Muat data "otak"
        brand_db, kamus_brand, db_kategori = load_intelligence_data(gsheets_service, SPREADSHEET_ID)
        st.session_state.brand_db = brand_db # Simpan DB brand untuk mode koreksi

        # Ambil semua data mentah dari semua folder toko
        raw_df = get_raw_data_from_drive(drive_service, data_mentah_folder_id)

        if raw_df.empty:
            st.warning("Tidak ada data mentah valid yang bisa diproses.")
            st.session_state.mode = "initial"
        else:
            # Olah data mentah menjadi data bersih
            processed_df = process_raw_data(raw_df, brand_db, kamus_brand, db_kategori)

            # GERBANG KUALITAS DATA: Cek apakah ada brand yang tidak dikenali
            unknown_brands_count = (processed_df[BRAND_COL] == "TIDAK DIKETAHUI").sum()

            if unknown_brands_count > 0:
                # Jika ada, masuk ke mode koreksi
                st.warning(f"Ditemukan {unknown_brands_count} produk yang brand-nya perlu diperbaiki.")
                st.session_state.df_to_fix = processed_df
                st.session_state.mode = "correction"
            else:
                # Jika semua bersih, simpan ke cache dan tampilkan dasbor
                st.success("Data berhasil diolah dan semua brand dikenali!")
                save_data_to_cache(drive_service, data_olahan_folder_id, CACHE_FILE_NAME, processed_df)
                st.session_state.master_df = processed_df
                st.session_state.mode = "dashboard"

    # Langkah 5: Jalankan ulang skrip untuk menampilkan UI yang sesuai
    st.rerun()


# --- Logika Tampilan Berdasarkan Mode Aplikasi (Saklar Utama) ---
st.divider()

if st.session_state.mode == "initial":
    st.info("👈 Silakan klik tombol **'Tarik & Proses Data Terbaru'** di sidebar untuk memulai analisis.")
    st.image("https://storage.googleapis.com/gweb-cloudblog-publish/images/Google_Drive_logo.max-2200x2200.png", width=150)
    st.subheader("Struktur Folder yang Diharapkan di Google Drive:")
    st.code(
        f"""
{PARENT_FOLDER_ID}/ (Folder Induk)
|
|-- 📂 {DATA_MENTAH_FOLDER_NAME}/
|   |-- 📂 NAMA_TOKO_1/
|   |   |-- 📜 2025-08-12-ready.csv
|   |   `-- ...
|   `-- 📂 NAMA_TOKO_2/
|
`-- 📂 {DATA_OLAHAN_FOLDER_NAME}/
    `-- (Folder ini akan diisi otomatis oleh cache)
        """, language="text")

elif st.session_state.mode == "correction":
    display_correction_mode(st.session_state.gsheets_service)

elif st.session_state.mode == "dashboard":
    if not st.session_state.master_df.empty:
        display_main_dashboard(st.session_state.master_df)
    else:
        st.error("Gagal memuat data master. Silakan coba tarik data kembali.")
        st.session_state.mode = "initial"
        
