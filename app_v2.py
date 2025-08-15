# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 3.2 (Batch Correction)
#  Dibuat oleh: Firman & Asisten AI (revisi menyeluruh)
#
#  Pembaruan kunci v3.2:
#   - ✅ Ruang Kontrol: *Batch correction* — ajarkan sekali, terapkan ke banyak baris
#   - ✅ Opsi 3 cara terapkan: (1) produk ini saja, (2) semua yang mengandung alias/frasa,
#       (3) semua nama produk yang mirip (fuzzy, threshold bisa diatur)
#   - ✅ Simpan alias ke kamus (sheet "kamus_brand") lalu *apply now* ke seluruh data yang cocok
#   - ✅ Perbaikan regex pencarian brand/alias (\b ... \b yang sebelumnya salah escape)
#   - ✅ Panel informasi sumber Database & Kamus (Spreadsheet ID + nama sheet)
#
#  Catatan kompatibilitas:
#   - Tetap backward-compatible dengan cache & alur v3.1
# ===================================================================================

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
import os
from typing import Callable, Any, Dict, List, Optional, Tuple, Set

# --- KONFIGURASI HALAMAN ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis v3.2 (Batch Correction)")

# =====================================================================================
# BLOK KONFIGURASI UTAMA
# =====================================================================================
# --- ID & Nama Aset Google Drive ---
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq"
DATA_MENTAH_FOLDER_NAME = "data_upload"
DATA_OLAHAN_FOLDER_NAME = "processed_data"
CACHE_FILE_NAME = "master_data.parquet"

# --- ID Google Sheet "Otak" ---
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg"
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
KATEGORI_SHEET_NAME = "DATABASE"

# --- Nama Kolom Konsisten ---
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

# Kolom minimal yang wajib ada setelah normalisasi
REQUIRED_COLUMNS = {NAMA_PRODUK_COL, HARGA_COL, TERJUAL_COL}

# =====================================================================================
# UTILITIES: RETRY & VALIDASI
# =====================================================================================

def with_retry(fn: Callable, max_attempts: int = 4, base_delay: float = 1.0, exc_types: Tuple = (Exception,),
               before_msg: Optional[str] = None, fail_msg: Optional[str] = None):
    """Jalankan fungsi dengan retry + backoff linear (1x, 2x, 3x...)."""
    def wrapper(*args, **kwargs):
        if before_msg:
            st.session_state.get("_log", [])
        for attempt in range(1, max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except exc_types as e:
                if attempt == max_attempts:
                    if fail_msg:
                        st.error(f"{fail_msg}: {e}")
                    raise
                sleep_for = base_delay * attempt
                st.info(f"Percobaan {attempt}/{max_attempts-1} gagal. Coba lagi dalam {sleep_for:.1f}s...")
                time.sleep(sleep_for)
    return wrapper

CSV_POSSIBLE_ENCODINGS = ["utf-8", "utf-8-sig", "latin1"]


def read_csv_safely(byte_stream: io.BytesIO) -> pd.DataFrame:
    """Coba baca CSV dengan beberapa encoding & delimiter umum; fallback ke pandas auto."""
    byte_stream.seek(0)
    raw = byte_stream.read()
    for enc in CSV_POSSIBLE_ENCODINGS:
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(io.BytesIO(raw), encoding=enc, sep=sep)
                if len(df.columns) > 1 or df.shape[0] > 0:
                    return df
            except Exception:
                pass
    # Fallback keras ke pandas deteksi otomatis
    try:
        return pd.read_csv(io.BytesIO(raw))
    except Exception as e:
        raise ValueError(f"Gagal membaca CSV dengan semua percobaan: {e}")


# =====================================================================================
# FUNGSI-FUNGSI INTI (BACKEND)
# =====================================================================================

# --- Fungsi Otentikasi & Koneksi ---
@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        drive_service = build("drive", "v3", credentials=creds)
        gsheets_service = gspread.authorize(creds)
        return drive_service, gsheets_service
    except Exception as e:
        st.error(f"Gagal autentikasi ke Google. Cek secrets.toml. Error: {e}")
        st.stop()


# --- Fungsi untuk Manajemen Folder di Google Drive ---
@st.cache_data(show_spinner="Mencari ID folder di Google Drive...")
def find_folder_id(_drive_service, parent_id, folder_name):
    query = (
        f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' "
        f"and name = '{folder_name}' and trashed = false"
    )

    @with_retry
    def _list():
        return _drive_service.files().list(q=query, fields="files(id, name)").execute()

    try:
        response = _list()
        files = response.get("files", [])
        if files:
            return files[0].get("id")
        st.error(
            f"Folder '{folder_name}' tidak ditemukan di dalam folder induk. Periksa kembali nama folder."
        )
        st.stop()
    except Exception as e:
        st.error(f"Kesalahan saat mencari folder '{folder_name}': {e}")
        st.stop()


# --- Fungsi untuk Memuat Data "Otak" (Database Brand, Kamus, Kategori) ---
@st.cache_data(show_spinner="Memuat 'otak' dari database...", ttl=3600)
def load_intelligence_data(_gsheets_service, spreadsheet_id):
    try:
        spreadsheet = _gsheets_service.open_by_key(spreadsheet_id)

        # database brand utama
        db_sheet = spreadsheet.worksheet(DB_SHEET_NAME)
        brand_db_list = [item for item in db_sheet.col_values(1) if item]

        # kamus alias brand
        kamus_sheet = spreadsheet.worksheet(KAMUS_SHEET_NAME)
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        if kamus_df.empty or "Alias" not in kamus_df.columns or "Brand_Utama" not in kamus_df.columns:
            kamus_dict = {}
        else:
            kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()

        # database kategori
        kategori_sheet = spreadsheet.worksheet(KATEGORI_SHEET_NAME)
        db_kategori_df = pd.DataFrame(kategori_sheet.get_all_records())
        db_kategori_df.columns = [str(col).strip().upper() for col in db_kategori_df.columns]

        return brand_db_list, kamus_dict, db_kategori_df
    except gspread.exceptions.WorksheetNotFound as e:
        st.error(f"GAGAL: Sheet '{e.args[0]}' tidak ditemukan di Google Sheet 'Otak'.")
        st.stop()
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet 'Otak'. Error: {e}")
        st.stop()


# --- Fungsi untuk Membaca Data Mentah dari Drive ---
@st.cache_data(show_spinner="Membaca semua data mentah dari folder kompetitor...", ttl=3600)
def get_raw_data_from_drive(_drive_service, data_mentah_folder_id):
    all_data = []

    query_subfolders = (
        f"'{data_mentah_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )

    @with_retry
    def _list_subfolders():
        return _drive_service.files().list(q=query_subfolders, fields="files(id, name)").execute()

    results = _list_subfolders()
    subfolders = results.get("files", [])

    if not subfolders:
        st.warning("Tidak ada subfolder (toko) yang ditemukan di dalam folder data mentah.")
        return pd.DataFrame()

    progress_bar = st.progress(0, text="Membaca data...")

    # pola tanggal yang diterima: YYYY-MM-DD atau YYYY_MM_DD
    date_regex = re.compile(r"(\d{4}[-_]\d{2}[-_]\d{2})")

    for i, folder in enumerate(subfolders):
        progress_text = f"Membaca folder toko: {folder['name']}..."
        progress_bar.progress((i + 1) / len(subfolders), text=progress_text)

        file_query = (
            f"'{folder['id']}' in parents and (mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet') and trashed = false"
        )

        @with_retry
        def _list_files():
            return _drive_service.files().list(q=file_query, fields="files(id, name, mimeType)").execute()

        files_in_folder = _list_files().get("files", [])

        for file_item in files_in_folder:
            file_id = file_item.get("id")
            file_name = file_item.get("name")
            mime_type = file_item.get("mimeType")

            try:
                # Unduh (konversi GSheet -> CSV jika perlu)
                if mime_type == "application/vnd.google-apps.spreadsheet":
                    request = _drive_service.files().export_media(fileId=file_id, mimeType="text/csv")
                    content = io.BytesIO(request.execute())
                else:
                    request = _drive_service.files().get_media(fileId=file_id)
                    buf = io.BytesIO()
                    downloader = MediaIoBaseDownload(buf, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                    content = buf

                if content.getbuffer().nbytes == 0:
                    st.warning(f"FILE KOSONG: '{file_name}' di folder '{folder['name']}' dilewati.")
                    continue

                # Baca CSV dengan strategi multi-encoding/multi-separator
                df = read_csv_safely(content)

                # Normalisasi kolom yang umum salah ketik / variasi nama
                rename_map_variants = {
                    "Nama Produk": NAMA_PRODUK_COL,
                    "Nama": NAMA_PRODUK_COL,
                    "nama": NAMA_PRODUK_COL,
                    "nama_produk": NAMA_PRODUK_COL,
                    "HARGA": HARGA_COL,
                    "Harga": HARGA_COL,
                    "harga": HARGA_COL,
                    "Terjual/BLN": TERJUAL_COL,
                    "Terjual/Bln": TERJUAL_COL,
                    "Terjual per Bulan": TERJUAL_COL,
                    "Terjual per bulan": TERJUAL_COL,
                    "terjual/bln": TERJUAL_COL,
                    "Link": LINK_COL,
                }
                df.rename(columns=lambda c: rename_map_variants.get(str(c).strip(), str(c).strip()), inplace=True)

                # Tambahkan informasi dari folder & file
                df[TOKO_COL] = folder["name"]
                m = date_regex.search(file_name)
                if m:
                    # normalisasi '_' menjadi '-'
                    date_str = m.group(1).replace("_", "-")
                    df[TANGGAL_COL] = pd.to_datetime(date_str, errors="coerce")
                else:
                    df[TANGGAL_COL] = pd.NaT

                low_name = str(file_name).lower()
                if "ready" in low_name:
                    df[STATUS_COL] = "Tersedia"
                elif "habis" in low_name:
                    df[STATUS_COL] = "Habis"
                else:
                    df[STATUS_COL] = "N/A"

                # Validasi kolom minimum
                missing = REQUIRED_COLUMNS - set(df.columns)
                if missing:
                    st.warning(
                        f"Lewati file '{file_name}' di '{folder['name']}' karena kolom wajib hilang: {sorted(missing)}"
                    )
                    continue

                all_data.append(df)
            except Exception as file_error:
                st.error(
                    f"GAGAL BACA FILE: Error saat memproses '{file_name}' di folder '{folder['name']}'."
                )
                st.error(f"Detail: {file_error}")
                st.info("File dilewati. Lanjut memproses file lain.")
                continue

    progress_bar.empty()

    if not all_data:
        st.warning("Tidak ada data valid yang ditemukan di semua folder toko.")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    return final_df


# --- Fungsi-fungsi Pemrosesan Data (Labeling, Cleaning, dll.) ---
def process_raw_data(raw_df, brand_db, kamus_brand, db_kategori):
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()

    # Standarisasi final (jaga-jaga)
    rename_map = {
        "Nama Produk": NAMA_PRODUK_COL,
        "Harga": HARGA_COL,
        "Terjual per Bulan": TERJUAL_COL,
        "Link": LINK_COL,
    }
    df.rename(columns=lambda c: rename_map.get(str(c).strip(), str(c).strip()), inplace=True)

    # Pastikan kolom esensial ada
    if NAMA_PRODUK_COL not in df.columns:
        st.error(
            f"KOLOM HILANG: Data gabungan tidak memiliki kolom '{NAMA_PRODUK_COL}'. Proses dihentikan."
        )
        st.stop()

    # Konversi tipe data aman
    df[HARGA_COL] = pd.to_numeric(df.get(HARGA_COL), errors="coerce").fillna(0).astype(float)
    df[TERJUAL_COL] = pd.to_numeric(df.get(TERJUAL_COL), errors="coerce").fillna(0).astype(int)

    # Hitung Omzet
    df[OMZET_COL] = df[HARGA_COL] * df[TERJUAL_COL]

    # Labeling Brand (dengan cache per nama produk unik)
    st.write("Memulai proses labeling brand (optimized cache)...")
    df = label_brands(df, brand_db, kamus_brand)

    # Pemetaan Kategori
    st.write("Memulai proses pemetaan kategori...")
    df = map_categories(df, db_kategori)

    st.write("Semua proses data selesai.")
    return df


def _tokenize_words_upper(name: str) -> List[str]:
    """Pisahkan name jadi token huruf/angka (UPPER), buang token pendek (<=2)."""
    up = str(name).upper()
    tokens = re.split(r"[^A-Z0-9]+", up)
    return [t for t in tokens if len(t) > 2]


def label_brands(df: pd.DataFrame, brand_db: List[str], kamus_brand: Dict[str, str], fuzzy_threshold: int = 88):
    brand_db_sorted = sorted([b for b in brand_db if isinstance(b, str)], key=len, reverse=True)

    # Perbaikan: gunakan boundary yang benar (\b), bukan literal \\b
    alias_patterns = [
        (re.compile(rf"\b{re.escape(str(alias).upper())}\b"), str(main).upper())
        for alias, main in kamus_brand.items()
        if isinstance(alias, str) and isinstance(main, str)
    ]
    brand_patterns = [
        (re.compile(rf"\b{re.escape(b.upper())}\b"), b)
        for b in brand_db_sorted if isinstance(b, str)
    ]

    # Cache hasil untuk nama produk unik
    name_cache: Dict[str, str] = {}

    def _find_brand(name_upper: str) -> str:
        # Prioritas 1: Kamus alias (exact token)
        for pat, main in alias_patterns:
            if pat.search(name_upper):
                return main
        # Prioritas 2: Database brand (eksak token atau non-spasi di nama)
        name_compact = name_upper.replace(" ", "")
        for pat, brand in brand_patterns:
            if pat.search(name_upper) or brand.upper().replace(" ", "") in name_compact:
                return brand
        # Prioritas 3: Fuzzy
        best_match = process.extractOne(name_upper, brand_db_sorted, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold:
            return best_match[0]
        return "TIDAK DIKETAHUI"

    brands = []
    for product_name in df[NAMA_PRODUK_COL].astype(str):
        upper = product_name.upper()
        if upper in name_cache:
            brands.append(name_cache[upper])
        else:
            found = _find_brand(upper)
            name_cache[upper] = found
            brands.append(found)

    df[BRAND_COL] = brands
    return df


@st.cache_data(show_spinner="Memetakan kategori produk...")
def map_categories(_df, _db_kategori, fuzzy_threshold: int = 95):
    _df = _df.copy()
    _df[KATEGORI_COL] = "Lainnya"
    if _db_kategori.empty or "NAMA" not in _db_kategori.columns or "KATEGORI" not in _db_kategori.columns:
        return _df

    db_unique = _db_kategori.drop_duplicates(subset=["NAMA"]).copy()
    db_map = db_unique.set_index("NAMA")["KATEGORI"]

    # Cache untuk penghematan fuzzy
    name_cache: Dict[str, Optional[str]] = {}

    for idx, row in _df.iterrows():
        name = str(row.get(NAMA_PRODUK_COL, ""))
        if not name:
            continue
        if name in name_cache:
            match_label = name_cache[name]
        else:
            match, score = process.extractOne(name, db_map.index, scorer=fuzz.token_set_ratio)
            match_label = db_map[match] if match and score >= fuzzy_threshold else None
            name_cache[name] = match_label
        if match_label:
            _df.at[idx, KATEGORI_COL] = match_label
    return _df


# --- Fungsi Manajemen Cache Cerdas (Parquet) ---
def check_cache_exists(drive_service, folder_id, filename):
    query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"

    @with_retry
    def _list():
        return drive_service.files().list(q=query, fields="files(id)").execute()

    response = _list()
    return response.get("files", [])


def load_data_from_cache(drive_service, file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_parquet(fh)


def save_data_to_cache(drive_service, folder_id, filename, df_to_save: pd.DataFrame):
    buffer = io.BytesIO()
    df_to_save.to_parquet(buffer, index=False)
    buffer.seek(0)

    existing_files = check_cache_exists(drive_service, folder_id, filename)

    media_body = MediaIoBaseUpload(buffer, mimetype="application/x-parquet", resumable=True)

    if existing_files:
        file_id = existing_files[0]["id"]
        drive_service.files().update(fileId=file_id, media_body=media_body).execute()
        st.toast(f"Cache cerdas '{filename}' berhasil diperbarui.", icon="🔄")
    else:
        file_metadata = {"name": filename, "parents": [folder_id], "mimeType": "application/x-parquet"}
        drive_service.files().create(body=file_metadata, media_body=media_body, fields="id").execute()
        st.toast(f"Cache cerdas '{filename}' berhasil dibuat.", icon="✅")


# --- Fungsi Bantuan untuk UI ---
def format_harga(x):
    if pd.isnull(x):
        return "N/A"
    try:
        return f"Rp {float(x):,.0f}"
    except (ValueError, TypeError):
        return str(x)


def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float("inf"):
        return "N/A"
    elif pct_change > 0.001:
        return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001:
        return f"▼ {pct_change:.1%}"
    else:
        return f"▬ 0.0%"


def colorize_growth(val):
    color = "grey"
    if isinstance(val, str):
        if "▲" in val:
            color = "#28a745"
        elif "▼" in val:
            color = "#dc3545"
    return f"color: {color}"


@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode("utf-8")


def paginate_dataframe(df: pd.DataFrame, key: str, page_size: int = 50):
    """Komponen pagination sederhana untuk dataframe besar."""
    total = len(df)
    if total <= page_size:
        st.dataframe(df, use_container_width=True, hide_index=True)
        return
    pages = (total - 1) // page_size + 1
    col_a, col_b, col_c = st.columns([1, 2, 1])
    page = col_b.number_input("Halaman", min_value=1, max_value=pages, value=1, key=f"{key}_page")
    start = (page - 1) * page_size
    end = start + page_size
    st.caption(f"Menampilkan {start+1:,}-{min(end, total):,} dari {total:,} baris")
    st.dataframe(df.iloc[start:end], use_container_width=True, hide_index=True)


# =====================================================================================
# FUNGSI-FUNGSI TAMPILAN (FRONTEND)
# =====================================================================================

def display_correction_mode(gsheets_service):
    st.header("🧠 Ruang Kontrol: Perbaikan Data Brand (Batch)")
    st.warning(
        "Ditemukan beberapa produk yang brand-nya tidak dikenali. Perbaiki data di bawah ini sebelum lanjut."
    )

    # Panel info lokasi Database/Kamus
    with st.expander("🔎 Sumber Database & Kamus (klik untuk lihat)"):
        st.markdown(
            f"""
            - **Spreadsheet 'Otak' ID**: `{SPREADSHEET_ID}`  
            - **Sheet Database Brand**: `{DB_SHEET_NAME}`  
            - **Sheet Kamus Alias Brand**: `{KAMUS_SHEET_NAME}`  
            - **Buka Spreadsheet**: [Klik di sini](https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit)
            """
        )

    df_to_fix = st.session_state.df_to_fix
    unknown_products = df_to_fix[df_to_fix[BRAND_COL] == "TIDAK DIKETAHUI"]

    if unknown_products.empty:
        st.success("🎉 Semua brand sudah dikenali! Menyimpan data bersih...")

        save_data_to_cache(
            st.session_state.drive_service,
            st.session_state.data_olahan_folder_id,
            CACHE_FILE_NAME,
            df_to_fix,
        )

        st.session_state.mode = "dashboard"
        st.session_state.master_df = df_to_fix.copy()
        if "df_to_fix" in st.session_state:
            del st.session_state.df_to_fix
        time.sleep(1.2)
        st.rerun()
        return

    st.info(f"Tersisa **{len(unknown_products)} produk** yang perlu direview.")

    product_to_review = unknown_products.iloc[0]
    st.divider()
    st.write("Produk yang perlu direview:")
    st.info(
        f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review[TOKO_COL]})"
    )

    # --- SUGGEST alias candidates dari nama produk ---
    suggested_tokens = _tokenize_words_upper(product_to_review[NAMA_PRODUK_COL])
    default_alias = suggested_tokens[0] if suggested_tokens else ""

    with st.form(key="review_form_single"):
        st.subheader("Apa brand yang benar untuk produk ini?")

        col1, col2 = st.columns(2)
        brand_list = [""] + sorted(st.session_state.brand_db)
        selected_brand = col1.selectbox(
            "1. Pilih dari brand yang sudah ada:",
            options=brand_list,
            help="Pilih brand yang paling sesuai dari daftar.",
        )
        new_brand_input = col2.text_input(
            "2. Atau, masukkan nama brand BARU:",
            help="Isi jika brand tidak ada di daftar sebelah.",
        )

        st.divider()
        st.subheader("Ajari sistem tentang Alias (Nama Lain)")
        alias_input = st.text_input(
            "Alias/frasa untuk mendeteksi brand ini (opsional, direkomendasikan)",
            value=default_alias,
            help="Contoh: 'ARMAGGEDDON' agar semua produk yang mengandung frasa itu dipetakan ke brand utama.",
        )

        st.divider()
        st.subheader("Cara menerapkan pembelajaran ini")
        apply_mode = st.radio(
            "Pilih mode penerapan:",
            [
                "Produk ini saja",
                "Semua produk yang MENGANDUNG frasa/alias di atas",
                "Semua produk yang MIRIP dengan produk ini (fuzzy)",
            ],
            index=1,
            help=(
                "Pilih 'Mengandung frasa' untuk update massal berdasarkan kata/alias. "
                "Pilih 'Mirip (fuzzy)' untuk menjaring nama-nama yang sangat mirip."
            ),
        )
        fuzzy_threshold = st.slider(
            "Ambang kemiripan (fuzzy) %",
            min_value=80,
            max_value=100,
            value=90,
            step=1,
            help="Dipakai ketika memilih mode fuzzy.",
        )

        submitted = st.form_submit_button("Ajarkan ke Sistem & Terapkan ▶")

        if submitted:
            final_brand = ""
            if new_brand_input:
                final_brand = new_brand_input.strip().upper()
            elif selected_brand:
                final_brand = selected_brand

            if not final_brand:
                st.error("Anda harus memilih brand yang sudah ada atau memasukkan brand baru.")
            else:
                # 1) Simpan brand baru ke database brand jika perlu
                if new_brand_input and final_brand not in st.session_state.brand_db:
                    try:
                        sheet = gsheets_service.open_by_key(SPREADSHEET_ID).worksheet(DB_SHEET_NAME)
                        sheet.append_row([final_brand], value_input_option="USER_ENTERED")
                        st.session_state.brand_db.append(final_brand)
                        st.toast(
                            f"Brand baru '{final_brand}' berhasil ditambahkan ke database.",
                            icon="➕",
                        )
                    except Exception as e:
                        st.error(f"Gagal menyimpan brand baru ke Google Sheet: {e}")

                # 2) Simpan alias ke kamus jika diisi
                alias_saved = False
                alias_used = alias_input.strip().upper() if alias_input else ""
                if alias_used:
                    try:
                        sheet = gsheets_service.open_by_key(SPREADSHEET_ID).worksheet(KAMUS_SHEET_NAME)
                        sheet.append_row([alias_used, final_brand], value_input_option="USER_ENTERED")
                        alias_saved = True
                        st.toast(
                            f"Alias '{alias_used}' → '{final_brand}' disimpan ke kamus.",
                            icon="📚",
                        )
                    except Exception as e:
                        st.error(f"Gagal menyimpan alias ke Google Sheet: {e}")

                # 3) Tentukan baris-baris yang akan diupdate (BATCH)
                indices_to_update: Set[int] = set()

                # Selalu update produk ini minimal
                name_now = product_to_review[NAMA_PRODUK_COL]
                cur_idx = df_to_fix[
                    (df_to_fix[NAMA_PRODUK_COL] == name_now) & (df_to_fix[BRAND_COL] == "TIDAK DIKETAHUI")
                ].index
                indices_to_update.update(cur_idx)

                unknown_df = df_to_fix[df_to_fix[BRAND_COL] == "TIDAK DIKETAHUI"][NAMA_PRODUK_COL].astype(str)

                if apply_mode == "Semua produk yang MENGANDUNG frasa/alias di atas":
                    key = alias_used or final_brand
                    key = key.strip().upper()
                    if key:
                        mask = unknown_df.str.upper().str.contains(re.escape(key), regex=True)
                        indices_to_update.update(unknown_df[mask].index.tolist())

                elif apply_mode == "Semua produk yang MIRIP dengan produk ini (fuzzy)":
                    base_name = str(name_now)
                    for i, p in unknown_df.items():
                        score = fuzz.token_set_ratio(base_name, p)
                        if score >= fuzzy_threshold:
                            indices_to_update.add(i)

                # 4) Terapkan brand ke semua indeks terpilih
                if not indices_to_update:
                    st.warning("Tidak ada baris yang cocok untuk diupdate.")
                else:
                    st.session_state.df_to_fix.loc[list(indices_to_update), BRAND_COL] = final_brand
                    st.success(f"Berhasil update brand '{final_brand}' ke {len(indices_to_update)} baris.")

                st.toast("Sistem telah belajar! Menampilkan produk berikutnya...", icon="✅")
                time.sleep(0.8)
                st.rerun()


def display_main_dashboard(df):
    st.sidebar.header("Navigasi Halaman")
    page = st.sidebar.radio(
        "Pilih Halaman:", ["Ringkasan Eksekutif", "Analisis Mendalam", "Analisis Produk Tunggal"]
    )
    st.sidebar.divider()

    st.sidebar.header("Filter Global")

    # Filter Toko Utama
    all_stores = sorted(df[TOKO_COL].unique())
    try:
        default_store_index = all_stores.index("DB_KLIK")
    except ValueError:
        default_store_index = 0 if all_stores else 0
    main_store = st.sidebar.selectbox("Pilih Toko Utama Anda:", all_stores, index=default_store_index)

    # Filter Rentang Tanggal
    df_with_dates = df.dropna(subset=[TANGGAL_COL]).copy()
    if df_with_dates.empty:
        st.error("Tidak ada data bertanggal. Pastikan nama file mengandung tanggal.")
        st.stop()

    min_date, max_date = df_with_dates[TANGGAL_COL].min().date(), df_with_dates[TANGGAL_COL].max().date()
    selected_date_range = st.sidebar.date_input(
        "Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date
    )

    # Filter Akurasi Fuzzy
    accuracy_cutoff = st.sidebar.slider(
        "Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1, key="global_accuracy", help="Digunakan untuk membandingkan produk antar toko."
    )

    st.sidebar.divider()
    st.sidebar.header("Download Data")

    csv_to_download = convert_df_to_csv(df)
    st.sidebar.download_button(
        label="📥 Download Data Olahan (CSV)",
        data=csv_to_download,
        file_name="data_olahan_lengkap.csv",
        mime="text/csv",
    )

    # Validasi range tanggal
    if len(selected_date_range) != 2:
        st.warning("Harap pilih rentang tanggal yang valid.")
        st.stop()

    start_date, end_date = selected_date_range
    df_filtered = df_with_dates[
        (df_with_dates[TANGGAL_COL].dt.date >= start_date)
        & (df_with_dates[TANGGAL_COL].dt.date <= end_date)
    ].copy()
    if df_filtered.empty:
        st.error("Tidak ada data pada rentang tanggal yang dipilih.")
        st.stop()

    df_filtered["Minggu"] = (
        df_filtered[TANGGAL_COL].dt.to_period("W-SUN").apply(lambda p: p.start_time).dt.date
    )
    main_store_df = df_filtered[df_filtered[TOKO_COL] == main_store].copy()
    competitor_df = df_filtered[df_filtered[TOKO_COL] != main_store].copy()

    # ============================= Ringkasan Eksekutif =============================
    if page == "Ringkasan Eksekutif":
        st.header("📈 Ringkasan Eksekutif")

        latest_date_in_data = df_filtered[TANGGAL_COL].max()
        st.markdown(
            f"Menampilkan data terbaru per tanggal **{latest_date_in_data.strftime('%d %b %Y')}**"
        )

        df_latest = df_filtered[df_filtered[TANGGAL_COL] == latest_date_in_data]
        df_latest_main_store = df_latest[df_latest[TOKO_COL] == main_store]

        omzet_today_main = df_latest_main_store[OMZET_COL].sum()
        units_today_main = df_latest_main_store[TERJUAL_COL].sum()

        total_ready_latest_main = len(
            df_latest_main_store[df_latest_main_store[STATUS_COL] == "Tersedia"]
        )
        total_habis_latest_main = len(
            df_latest_main_store[df_latest_main_store[STATUS_COL] == "Habis"]
        )
        total_produk_latest_main = total_ready_latest_main + total_habis_latest_main

        units_sold_latest_ready_main = df_latest_main_store[
            df_latest_main_store[STATUS_COL] == "Tersedia"
        ][TERJUAL_COL].sum()

        col1, col2, col3 = st.columns(3)
        col1.metric(
            f"Omzet {main_store} (Hari Ini)", format_harga(omzet_today_main), f"{int(units_today_main)} unit terjual"
        )
        col2.metric(
            f"Jumlah Produk {main_store} (Hari Ini)",
            f"{total_produk_latest_main:,} Produk",
            f"Tersedia: {total_ready_latest_main:,} | Habis: {total_habis_latest_main:,}",
        )
        col3.metric(
            f"Unit Terjual {main_store} (Ready, Hari Ini)", f"{int(units_sold_latest_ready_main):,} Unit"
        )

        st.divider()

        st.subheader("Perbandingan Omzet per Toko (Data Terbaru)")
        omzet_latest_per_store = (
            df_latest.groupby(TOKO_COL)[OMZET_COL].sum().sort_values(ascending=False).reset_index()
        )
        fig_bar = px.bar(
            omzet_latest_per_store,
            x=TOKO_COL,
            y=OMZET_COL,
            title=f"Total Omzet per Toko pada {latest_date_in_data.strftime('%d %b %Y')}",
            text_auto=True,
        )
        fig_bar.update_traces(texttemplate="%{value:,.0f}")
        st.plotly_chart(fig_bar, use_container_width=True)

        st.divider()

        st.subheader("Tabel Pertumbuhan Omzet Mingguan per Toko (%)")
        weekly_omzet_pivot = df_filtered.groupby(["Minggu", TOKO_COL])[OMZET_COL].sum().unstack()
        weekly_growth_pivot = weekly_omzet_pivot.pct_change()
        weekly_growth_pivot.index = pd.to_datetime(weekly_growth_pivot.index).strftime("%Y-%m-%d")
        st.dataframe(
            weekly_growth_pivot.style.format(format_wow_growth).applymap(colorize_growth),
            use_container_width=True,
        )

    # ============================= Analisis Mendalam =============================
    elif page == "Analisis Mendalam":
        st.header("🔍 Analisis Mendalam")
        tab_titles = [
            f"⭐ Toko Saya ({main_store})",
            "⚖️ Perbandingan Harga",
            "🏆 Brand Kompetitor",
            "📦 Status Stok",
            "📈 Kinerja Penjualan",
            "📊 Produk Baru",
        ]
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_titles)

        with tab1:
            st.subheader("1. Produk Terlaris")
            top_products = (
                main_store_df.sort_values(TERJUAL_COL, ascending=False)
                .head(15)[[NAMA_PRODUK_COL, TERJUAL_COL, OMZET_COL]]
                .copy()
            )
            st.dataframe(
                top_products.style.format({OMZET_COL: format_harga, TERJUAL_COL: "{:,.0f}"}),
                use_container_width=True,
                hide_index=True,
            )

            st.subheader("2. Distribusi Omzet Brand")
            brand_omzet_main = main_store_df.groupby(BRAND_COL)[OMZET_COL].sum().reset_index()
            fig_brand_pie = px.pie(
                brand_omzet_main, names=BRAND_COL, values=OMZET_COL, title="Distribusi Omzet Brand"
            )
            st.plotly_chart(fig_brand_pie, use_container_width=True)

        with tab2:
            st.subheader(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
            if not main_store_df.empty:
                latest_date = main_store_df[TANGGAL_COL].max()
                main_store_latest = main_store_df[main_store_df[TANGGAL_COL] == latest_date].copy()

                product_list = sorted(main_store_latest[NAMA_PRODUK_COL].unique())
                selected_product = st.selectbox(
                    "Pilih produk dari toko Anda untuk dibandingkan:", product_list
                )

                if selected_product:
                    product_info = main_store_latest[
                        main_store_latest[NAMA_PRODUK_COL] == selected_product
                    ].iloc[0]
                    st.markdown(f"**Produk Pilihan:** *{product_info[NAMA_PRODUK_COL]}*")
                    col1, col2 = st.columns(2)
                    col1.metric(f"Harga di {main_store}", format_harga(product_info[HARGA_COL]))
                    col2.metric("Status", product_info[STATUS_COL])

                    st.markdown("---")
                    st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                    competitor_latest = competitor_df[competitor_df[TANGGAL_COL] == latest_date]
                    if competitor_latest.empty:
                        st.warning("Tidak ada data kompetitor pada tanggal terbaru.")
                    else:
                        matches = process.extract(
                            product_info[NAMA_PRODUK_COL],
                            competitor_latest[NAMA_PRODUK_COL].tolist(),
                            limit=10,
                            scorer=fuzz.token_set_ratio,
                        )
                        valid_matches = [m for m in matches if m[1] >= accuracy_cutoff]
                        if not valid_matches:
                            st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                        else:
                            rows = []
                            for product, score in valid_matches:
                                sub = competitor_latest[
                                    competitor_latest[NAMA_PRODUK_COL] == product
                                ].iloc[0]
                                price_diff = float(sub[HARGA_COL]) - float(product_info[HARGA_COL])
                                rows.append(
                                    {
                                        "Toko": sub[TOKO_COL],
                                        "Nama Produk": sub[NAMA_PRODUK_COL],
                                        "Kemiripan": f"{int(score)}%",
                                        "Harga Kompetitor": format_harga(sub[HARGA_COL]),
                                        "Selisih Harga": f"Rp {price_diff:,.0f}",
                                        "Status": sub[STATUS_COL],
                                    }
                                )
                            comp_df = pd.DataFrame(rows)
                            paginate_dataframe(comp_df, key="comp_table", page_size=15)

        with tab3:
            st.subheader("Analisis Brand di Toko Kompetitor")
            if competitor_df.empty:
                st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
            else:
                brand_analysis = (
                    competitor_df.groupby([TOKO_COL, BRAND_COL])
                    .agg(Total_Omzet=(OMZET_COL, "sum"), Total_Unit_Terjual=(TERJUAL_COL, "sum"))
                    .reset_index()
                )
                fig = px.treemap(
                    brand_analysis,
                    path=[TOKO_COL, BRAND_COL],
                    values="Total_Omzet",
                    title="Peta Omzet Brand per Toko Kompetitor",
                )
                st.plotly_chart(fig, use_container_width=True)

        with tab4:
            st.subheader("Tren Status Stok Mingguan per Toko")
            stock_trends = (
                df_filtered.groupby(["Minggu", TOKO_COL, STATUS_COL]).size().unstack(fill_value=0).reset_index()
            )
            if "Tersedia" not in stock_trends.columns:
                stock_trends["Tersedia"] = 0
            if "Habis" not in stock_trends.columns:
                stock_trends["Habis"] = 0
            stock_trends_melted = stock_trends.melt(
                id_vars=["Minggu", TOKO_COL],
                value_vars=["Tersedia", "Habis"],
                var_name="Tipe Stok",
                value_name="Jumlah Produk",
            )
            fig_stock_trends = px.line(
                stock_trends_melted,
                x="Minggu",
                y="Jumlah Produk",
                color=TOKO_COL,
                line_dash="Tipe Stok",
                markers=True,
                title="Jumlah Produk Tersedia vs. Habis per Minggu",
            )
            st.plotly_chart(fig_stock_trends, use_container_width=True)

        with tab5:
            st.subheader("Grafik Omzet Mingguan")
            weekly_omzet = (
                df_filtered.groupby(["Minggu", TOKO_COL])[OMZET_COL].sum().reset_index()
            )
            fig_weekly_omzet = px.line(
                weekly_omzet,
                x="Minggu",
                y=OMZET_COL,
                color=TOKO_COL,
                markers=True,
                title="Perbandingan Omzet Mingguan Antar Toko",
            )
            st.plotly_chart(fig_weekly_omzet, use_container_width=True)

        with tab6:
            st.subheader("Perbandingan Produk Baru Antar Minggu")
            weeks = sorted(df_filtered["Minggu"].unique())
            if len(weeks) < 2:
                st.info("Butuh setidaknya 2 minggu data untuk perbandingan.")
            else:
                col1, col2 = st.columns(2)
                week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0, key="week_before")
                week_after = col2.selectbox(
                    "Pilih Minggu Penentu:", weeks, index=len(weeks) - 1, key="week_after"
                )
                if week_before >= week_after:
                    st.error("Minggu Penentu harus setelah Minggu Pembanding.")
                else:
                    for store in sorted(df_filtered[TOKO_COL].unique()):
                        with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                            products_before = set(
                                df_filtered[(df_filtered[TOKO_COL] == store) & (df_filtered["Minggu"] == week_before)][
                                    NAMA_PRODUK_COL
                                ]
                            )
                            products_after = set(
                                df_filtered[(df_filtered[TOKO_COL] == store) & (df_filtered["Minggu"] == week_after)][
                                    NAMA_PRODUK_COL
                                ]
                            )
                            new_products = products_after - products_before
                            if not new_products:
                                st.write("Tidak ada produk baru yang terdeteksi.")
                            else:
                                st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                                new_products_df = df_filtered[
                                    (df_filtered[NAMA_PRODUK_COL].isin(new_products))
                                    & (df_filtered[TOKO_COL] == store)
                                    & (df_filtered["Minggu"] == week_after)
                                ][[NAMA_PRODUK_COL, HARGA_COL, STATUS_COL, TERJUAL_COL]]
                                st.dataframe(
                                    new_products_df.style.format({HARGA_COL: format_harga}),
                                    use_container_width=True,
                                    hide_index=True,
                                )

    # ============================= Analisis Produk Tunggal =============================
    elif page == "Analisis Produk Tunggal":
        st.header("🎯 Analisis Produk Tunggal")
        st.info("Gunakan filter di bawah untuk menemukan produk spesifik dan melihat trennya di seluruh pasar.")

        all_brands = ["Semua Brand"] + sorted(df_filtered[BRAND_COL].unique())
        selected_brand_filter = st.selectbox("Filter berdasarkan Brand (Opsional):", all_brands)

        product_pool = df_filtered if selected_brand_filter == "Semua Brand" else df_filtered[df_filtered[BRAND_COL] == selected_brand_filter]

        all_products = sorted(product_pool[NAMA_PRODUK_COL].unique())
        if all_products:
            selected_product = st.selectbox("Cari dan pilih produk:", all_products)

            if selected_product:
                product_df = df_filtered[df_filtered[NAMA_PRODUK_COL] == selected_product].copy()
                st.subheader(f"Tren Historis untuk: {selected_product}")
                product_df_sorted = product_df.sort_values(by=TANGGAL_COL)
                fig_trend = px.line(
                    product_df_sorted,
                    x=TANGGAL_COL,
                    y=HARGA_COL,
                    color=TOKO_COL,
                    markers=True,
                    title="Tren Harga dari Waktu ke Waktu",
                )
                st.plotly_chart(fig_trend, use_container_width=True)

                st.subheader("Perbandingan Kompetitif Saat Ini")
                latest_date_overall = df_filtered[TANGGAL_COL].max()
                latest_products_df = df_filtered[df_filtered[TANGGAL_COL] == latest_date_overall]
                matches = process.extract(
                    selected_product, latest_products_df[NAMA_PRODUK_COL].unique(), limit=None, scorer=fuzz.token_set_ratio
                )
                similar_product_names = [match[0] for match in matches if match[1] >= accuracy_cutoff]
                competitor_landscape = latest_products_df[
                    latest_products_df[NAMA_PRODUK_COL].isin(similar_product_names)
                ][[TOKO_COL, NAMA_PRODUK_COL, HARGA_COL, STATUS_COL, TERJUAL_COL]]
                competitor_landscape = competitor_landscape.copy()
                competitor_landscape["Harga"] = competitor_landscape[HARGA_COL].apply(format_harga)
                competitor_landscape.drop(columns=[HARGA_COL], inplace=True)
                paginate_dataframe(competitor_landscape, key="single_product_comp", page_size=20)
        else:
            st.warning("Tidak ada produk untuk ditampilkan dengan filter brand yang dipilih.")


# =====================================================================================
# ALUR KERJA UTAMA APLIKASI
# =====================================================================================

st.title("📊 Dashboard Analisis Penjualan & Kompetitor v3.2 (Batch Correction)")
st.markdown("Versi dengan *Cache Cerdas*, *Gerbang Kualitas Data*, *Batch Correction*, dan perbaikan regex.")

# Inisialisasi session_state
st.session_state.setdefault("mode", "initial")
st.session_state.setdefault("master_df", pd.DataFrame())
st.session_state.setdefault("df_to_fix", pd.DataFrame())

# --- Tombol Pemicu Utama ---
st.sidebar.header("Kontrol Utama")
st.sidebar.info(
    "Proses akan cepat jika cache cerdas sudah ada. Jika tidak, aplikasi akan membangun cache baru."
)
if st.sidebar.button("🚀 Tarik & Proses Data Terbaru", type="primary"):
    with st.spinner("Memeriksa status data..."):
        # 1. Otentikasi
        drive_service, gsheets_service = get_google_apis()
        st.session_state.drive_service = drive_service
        st.session_state.gsheets_service = gsheets_service

        # 2. Cari folder data mentah & olahan
        data_mentah_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_MENTAH_FOLDER_NAME)
        data_olahan_folder_id = find_folder_id(drive_service, PARENT_FOLDER_ID, DATA_OLAHAN_FOLDER_NAME)
        st.session_state.data_olahan_folder_id = data_olahan_folder_id

        # 3. Cek cache
        cache_file = check_cache_exists(drive_service, data_olahan_folder_id, CACHE_FILE_NAME)

        if cache_file:
            # Jalur cepat
            st.toast("Cache cerdas ditemukan! Memuat data...", icon="⚡")
            df = load_data_from_cache(drive_service, cache_file[0]["id"])
            st.session_state.master_df = df
            st.session_state.mode = "dashboard"
        else:
            # Jalur berat
            st.toast("Cache cerdas tidak ditemukan. Memulai proses data dari awal...", icon="🐌")

            # Muat data "otak"
            brand_db, kamus_brand, db_kategori = load_intelligence_data(gsheets_service, SPREADSHEET_ID)
            st.session_state.brand_db = brand_db

            # Baca semua data mentah
            raw_df = get_raw_data_from_drive(drive_service, data_mentah_folder_id)

            if raw_df.empty:
                st.warning("Tidak ada data mentah yang bisa diproses.")
                st.session_state.mode = "initial"
            else:
                # Proses data mentah
                processed_df = process_raw_data(raw_df, brand_db, kamus_brand, db_kategori)

                # Gerbang Kualitas Data
                unknown_brands_count = (processed_df[BRAND_COL] == "TIDAK DIKETAHUI").sum()

                if unknown_brands_count > 0:
                    st.session_state.df_to_fix = processed_df
                    st.session_state.mode = "correction"
                else:
                    save_data_to_cache(drive_service, data_olahan_folder_id, CACHE_FILE_NAME, processed_df)
                    st.session_state.master_df = processed_df
                    st.session_state.mode = "dashboard"

    st.rerun()

# --- Logika Tampilan Berdasarkan Mode Aplikasi ---
if st.session_state.mode == "initial":
    st.info("👈 Klik **'Tarik & Proses Data Terbaru'** di sidebar untuk memulai.")
    st.markdown("---")
    st.subheader("Struktur Folder yang Diharapkan di Google Drive:")
    st.code(
        f"""
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
        """,
        language="text",
    )
elif st.session_state.mode == "correction":
    display_correction_mode(st.session_state.gsheets_service)
elif st.session_state.mode == "dashboard":
    if not st.session_state.master_df.empty:
        display_main_dashboard(st.session_state.master_df)
    else:
        st.error("Terjadi kesalahan, data master tidak berhasil dimuat.")
        st.session_state.mode = "initial"
        st.rerun()
