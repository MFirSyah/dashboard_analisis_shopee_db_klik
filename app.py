# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 7.1
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Mendukung pembacaan otomatis DATA_REKAP.xlsx (lokal/cloud) dan Google Sheets.
# ===================================================================================

import os
import re
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    import gspread
    GSPREAD_AVAILABLE = True
except ImportError:
    gspread = None
    GSPREAD_AVAILABLE = False

# ================================
# KONFIGURASI HALAMAN & KONSTANTA
# ================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis v7.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EXCEL_PATH = os.path.join(BASE_DIR, "DATA_REKAP.xlsx")

REQUIRED_GCP_SECRETS = (
    "spreadsheet_key",
    "gcp_type",
    "gcp_project_id",
    "gcp_private_key_id",
    "gcp_private_key_raw",
    "gcp_client_email",
    "gcp_client_id",
    "gcp_auth_uri",
    "gcp_token_uri",
    "gcp_auth_provider_x509_cert_url",
    "gcp_client_x509_cert_url",
)


def _get_secret(key, default=None):
    """Membaca st.secrets secara aman meskipun secrets.toml tidak tersedia."""
    try:
        return st.secrets[key]
    except Exception:
        return default


def _has_gsheets_config():
    """Memeriksa apakah gspread terpasang dan seluruh secrets GCP tersedia."""
    if not GSPREAD_AVAILABLE:
        return False
    return all(_get_secret(k) for k in REQUIRED_GCP_SECRETS)


MY_STORE_NAME = _get_secret("my_store_name", "DB KLIK")
SPREADSHEET_KEY = _get_secret("spreadsheet_key", "")


# ================================
# FUNGSI HELPER PEMROSESAN DATA
# ================================
def _infer_store_and_status(sheet_title):
    """Ekstrak nama toko dan status stok dari judul sheet REKAP (termasuk nama terpotong 31 char)."""
    store_match = re.match(r"^(.*?) - REKAP", sheet_title, re.IGNORECASE)
    toko_name = store_match.group(1).strip() if store_match else "Toko Tak Dikenal"

    title_upper = sheet_title.strip().upper()
    if "READY" in title_upper or title_upper.endswith("- RE") or title_upper.endswith(" RE"):
        status = "Tersedia"
    else:
        status = "Habis"
    return toko_name, status


def _parse_price(val):
    """Mengonversi nilai harga dari Excel (float/int) maupun string ('Rp 100.000') menjadi float."""
    if pd.isna(val):
        return np.nan
    if isinstance(val, (int, float, np.number)):
        return float(val)
    text = str(val).strip()
    if not text:
        return np.nan
    if re.fullmatch(r"\d+\.\d+", text):
        return float(text)
    cleaned = re.sub(r"[^\d]", "", text)
    return float(cleaned) if cleaned else np.nan


def _clean_unnamed_columns(df):
    """Menghapus kolom kosong atau 'Unnamed: ...' hasil pembacaan sheet."""
    drop_cols = [
        c for c in df.columns
        if str(c).strip() == "" or str(c).strip().upper().startswith("UNNAMED:")
    ]
    return df.drop(columns=drop_cols) if drop_cols else df


def _enrich_from_database(rekap_df, database_df):
    """Mengisi kolom SKU pada rekap_df dari sheet DATABASE berdasarkan Nama Produk (Kategori diambil dari sheet REKAP)."""
    if database_df is None or database_df.empty:
        return rekap_df

    db_cols_upper = {str(c).strip().upper(): c for c in database_df.columns}
    nama_col = db_cols_upper.get("NAMA") or db_cols_upper.get("NAMA PRODUK")
    sku_col = db_cols_upper.get("SKU")
    if not nama_col or not sku_col:
        return rekap_df

    db_lookup = database_df[[nama_col, sku_col]].copy()
    db_lookup["_nama_key"] = db_lookup[nama_col].astype(str).str.strip().str.upper()
    db_lookup = db_lookup[db_lookup["_nama_key"] != ""].drop_duplicates(subset=["_nama_key"], keep="last")

    rekap_keys = rekap_df["Nama Produk"].astype(str).str.strip().str.upper()

    if "SKU" not in rekap_df.columns or rekap_df["SKU"].isnull().all():
        sku_map = db_lookup.set_index("_nama_key")[sku_col].replace("", np.nan).to_dict()
        rekap_df["SKU"] = rekap_keys.map(sku_map).fillna("N/A")

    return rekap_df


def _prepare_loaded_data(rekap_list_df, database_df, kamus_df=None):
    """Membersihkan, menormalisasi, dan menggabungkan data REKAP, DATABASE, dan kamus_brand."""
    if not rekap_list_df:
        return None, None

    rekap_df = pd.concat(rekap_list_df, ignore_index=True)
    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]

    final_rename = {
        "NAMA": "Nama Produk",
        "TERJUAL/BLN": "Terjual per Bulan",
        "TANGGAL": "Tanggal",
        "HARGA": "Harga",
        "BRAND": "Brand",
        "STOK": "Stok",
        "TOKO": "Toko",
        "STATUS": "Status",
        "KATEGORI": "KATEGORI",
        "SKU": "SKU",
    }
    rekap_df.rename(columns=final_rename, inplace=True)

    for required_col in ("Tanggal", "Nama Produk", "Harga", "Toko"):
        if required_col not in rekap_df.columns:
            rekap_df[required_col] = np.nan

    rekap_df["Nama Produk"] = rekap_df["Nama Produk"].astype(str).str.strip()
    rekap_df.loc[rekap_df["Nama Produk"].isin(["", "nan", "None"]), "Nama Produk"] = np.nan
    rekap_df["Tanggal"] = pd.to_datetime(rekap_df["Tanggal"], errors="coerce", dayfirst=True)
    rekap_df["Harga"] = rekap_df["Harga"].apply(_parse_price)

    if "Terjual per Bulan" in rekap_df.columns:
        rekap_df["Terjual per Bulan"] = pd.to_numeric(rekap_df["Terjual per Bulan"], errors="coerce").fillna(0)
    else:
        rekap_df["Terjual per Bulan"] = 0

    rekap_df.dropna(subset=["Tanggal", "Nama Produk", "Harga", "Toko"], inplace=True)
    if rekap_df.empty:
        return None, None

    if "Brand" not in rekap_df.columns or rekap_df["Brand"].isnull().all():
        rekap_df["Brand"] = rekap_df["Nama Produk"].str.split(n=1).str[0].str.upper()
    else:
        fallback_brand = rekap_df["Nama Produk"].str.split(n=1).str[0].str.upper()
        rekap_df["Brand"] = rekap_df["Brand"].astype(str).str.strip()
        rekap_df.loc[rekap_df["Brand"].isin(["", "nan", "None"]), "Brand"] = fallback_brand

    rekap_df = _enrich_from_database(rekap_df, database_df)
    rekap_df["Omzet"] = (rekap_df["Harga"].fillna(0) * rekap_df["Terjual per Bulan"].fillna(0)).round().astype("int64")

    if kamus_df is not None and not kamus_df.empty and {"Alias", "Brand_Utama"}.issubset(kamus_df.columns):
        kamus_brand = {
            str(row["Alias"]).strip().upper(): str(row["Brand_Utama"]).strip().upper()
            for _, row in kamus_df.iterrows()
            if pd.notna(row["Alias"]) and pd.notna(row["Brand_Utama"])
        }
        rekap_df["Brand_Utama"] = rekap_df["Brand"].str.upper().map(kamus_brand).fillna(rekap_df["Brand"].str.upper())
    else:
        rekap_df["Brand_Utama"] = rekap_df["Brand"].str.upper()

    if database_df is None:
        database_df = pd.DataFrame()
    else:
        database_df = _clean_unnamed_columns(database_df.copy())
        database_df.columns = [str(c).strip().upper() for c in database_df.columns]

    return rekap_df.sort_values("Tanggal").reset_index(drop=True), database_df


# ================================
# FUNGSI MEMUAT DATA (EXCEL & GSHEETS)
# ================================
@st.cache_data(show_spinner="Memuat data rekap...")
def load_all_data_from_excel(excel_path):
    """Memuat seluruh sheet REKAP, DATABASE, dan kamus_brand dari file Excel lokal."""
    xl = pd.ExcelFile(excel_path, engine="openpyxl")

    rekap_list_df = []
    database_df = pd.DataFrame()
    kamus_df = None

    for sheet_name in xl.sheet_names:
        upper_name = sheet_name.strip().upper()
        if "REKAP" in upper_name:
            try:
                df_sheet = _clean_unnamed_columns(xl.parse(sheet_name))
                if df_sheet.empty:
                    continue
                toko_name, inferred_status = _infer_store_and_status(sheet_name)
                df_sheet["Toko"] = toko_name
                if "Status" not in df_sheet.columns and "STATUS" not in [str(c).upper() for c in df_sheet.columns]:
                    df_sheet["Status"] = inferred_status
                rekap_list_df.append(df_sheet)
            except Exception as exc:
                st.warning(f"Gagal memproses sheet rekap '{sheet_name}': {exc}")
        elif upper_name == "DATABASE":
            database_df = _clean_unnamed_columns(xl.parse(sheet_name))
        elif upper_name == "KAMUS_BRAND":
            kamus_df = _clean_unnamed_columns(xl.parse(sheet_name))

    return _prepare_loaded_data(rekap_list_df, database_df, kamus_df)


@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    creds_dict = {
        "type": st.secrets["gcp_type"],
        "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"],
        "private_key": st.secrets["gcp_private_key_raw"].replace("\\n", "\n"),
        "client_email": st.secrets["gcp_client_email"],
        "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"],
        "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"],
    }
    return gspread.service_account_from_dict(creds_dict)


@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_all_data_from_gsheets(spreadsheet_key):
    gc = connect_to_gsheets()
    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
    except Exception as exc:
        st.error(f"GAGAL KONEKSI/OPEN SPREADSHEET: {exc}")
        return None, None

    rekap_list_df = []
    for ws in spreadsheet.worksheets():
        if "REKAP" not in ws.title.upper():
            continue
        try:
            all_values = ws.get_all_values()
            if not all_values or len(all_values) < 2:
                continue
            df_sheet = _clean_unnamed_columns(pd.DataFrame(all_values[1:], columns=all_values[0]))
            toko_name, inferred_status = _infer_store_and_status(ws.title)
            df_sheet["Toko"] = toko_name
            if "Status" not in df_sheet.columns and "STATUS" not in [str(c).upper() for c in df_sheet.columns]:
                df_sheet["Status"] = inferred_status
            rekap_list_df.append(df_sheet)
        except Exception as exc:
            st.warning(f"Gagal memproses sheet rekap '{ws.title}': {exc}")

    database_df = pd.DataFrame()
    try:
        db_ws = spreadsheet.worksheet("DATABASE")
        all_values = db_ws.get_all_values()
        if all_values and len(all_values) >= 2:
            database_df = _clean_unnamed_columns(pd.DataFrame(all_values[1:], columns=all_values[0]))
    except Exception:
        pass

    kamus_df = None
    try:
        kamus_ws = spreadsheet.worksheet("kamus_brand")
        kamus_df = pd.DataFrame(kamus_ws.get_all_records())
    except Exception:
        pass

    return _prepare_loaded_data(rekap_list_df, database_df, kamus_df)


def load_all_data():
    """Memuat data dari DATA_REKAP.xlsx jika tersedia, atau dari Google Sheets jika terkonfigurasi."""
    if os.path.exists(DEFAULT_EXCEL_PATH):
        return load_all_data_from_excel(DEFAULT_EXCEL_PATH)
    if _has_gsheets_config():
        df, db_df = load_all_data_from_gsheets(SPREADSHEET_KEY)
        if df is not None and not df.empty:
            return df, db_df
    st.error("File 'DATA_REKAP.xlsx' tidak ditemukan dan konfigurasi Google Sheets belum tersedia.")
    return None, None


# ================================
# FUNGSI-FUNGSI PEMBANTU (UTILITY)
# ================================
def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float("inf"):
        return "N/A"
    elif pct_change > 0.001:
        return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001:
        return f"▼ {pct_change:.1%}"
    return "▬ 0.0%"


def style_wow_growth(val):
    color = "black"
    if isinstance(val, str):
        if "▲" in val:
            color = "green"
        elif "▼" in val:
            color = "red"
    return f"color: {color}"


def apply_cell_style(styler, func, subset):
    """Kompatibel dengan pandas >= 2.1/3.0 (.map) maupun pandas lama (.applymap)."""
    if hasattr(styler, "map"):
        return styler.map(func, subset=subset)
    return styler.applymap(func, subset=subset)


@st.cache_data
def convert_df_for_download(df):
    return df.to_csv(index=False).encode("utf-8")


def format_rupiah(val):
    """Memformat angka ke format mata uang Rupiah penuh (contoh: Rp 1.425.000)."""
    if pd.isna(val) or not isinstance(val, (int, float, np.number)):
        return "N/A"
    val_int = int(round(val))
    formatted = f"{abs(val_int):,}".replace(",", ".")
    return f"-Rp {formatted}" if val_int < 0 else f"Rp {formatted}"


def format_rupiah_singkat(val):
    """Memformat angka ke penyingkatan Rupiah Indonesia (Rb, Jt, M, T) untuk label grafik."""
    if pd.isna(val) or not isinstance(val, (int, float, np.number)):
        return "N/A"
    sign = "-" if val < 0 else ""
    v = abs(float(val))
    if v >= 1_000_000_000_000:
        return f"{sign}Rp {v / 1_000_000_000_000:.2f}".replace(".", ",") + " T"
    if v >= 1_000_000_000:
        return f"{sign}Rp {v / 1_000_000_000:.2f}".replace(".", ",") + " M"
    if v >= 1_000_000:
        return f"{sign}Rp {v / 1_000_000:.1f}".replace(".", ",") + " Jt"
    if v >= 1_000:
        return f"{sign}Rp {v / 1_000:.0f} Rb"
    return f"{sign}Rp {int(round(v))}"


# ================================
# APLIKASI UTAMA (MAIN APP)
# ================================
st.title("📊 Dashboard Analisis Penjualan & Bisnis")

if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if not st.session_state.data_loaded:
    _, col_center, _ = st.columns([2, 3, 2])
    with col_center:
        if st.button("Tarik Data & Mulai Analisis 🚀", type="primary"):
            df, db_df = load_all_data()
            if df is not None and not df.empty and db_df is not None:
                st.session_state.df, st.session_state.db_df = df, db_df
                st.session_state.data_loaded = True
                st.rerun()
            else:
                st.error("Gagal memuat data. Pastikan file 'DATA_REKAP.xlsx' atau akses Google Sheets tersedia.")
    st.info("👆 Klik tombol untuk menarik semua data yang diperlukan untuk analisis.")
    st.stop()

df = st.session_state.df
db_df = st.session_state.db_df if "db_df" in st.session_state else pd.DataFrame()

# ================================
# SIDEBAR (KONTROL UTAMA)
# ================================
st.sidebar.header("Mode Tampilan")
app_mode = st.sidebar.radio("Pilih Tampilan:", ("Tab Analisis", "Cari Perbandingan", "HPP Produk", "Cek Brand Toko"))
st.sidebar.divider()

if app_mode == "Tab Analisis":
    st.sidebar.header("Kontrol & Filter Analisis")
    min_date, max_date = df["Tanggal"].min().date(), df["Tanggal"].max().date()
    selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(selected_date_range) != 2:
        st.sidebar.warning("Pilih 2 tanggal.")
        st.stop()
    start_date, end_date = selected_date_range

    st.sidebar.divider()
    df_filtered_export = df[(df["Tanggal"].dt.date >= start_date) & (df["Tanggal"].dt.date <= end_date)]
    st.sidebar.header("Ekspor & Info")
    st.sidebar.info(f"Baris data dalam rentang: **{len(df_filtered_export)}**")
    csv_data = convert_df_for_download(df_filtered_export)
    st.sidebar.download_button("📥 Unduh CSV (Filter)", data=csv_data, file_name=f"analisis_{start_date}_{end_date}.csv", mime="text/csv")

elif app_mode == "Cari Perbandingan":
    st.sidebar.header("Kontrol Pencarian")
    st.sidebar.info("Skor akurasi 0.0 - 1.0. Semakin tinggi, semakin ketat pencocokan produk.")
    accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan", 0.0, 1.0, 0.5, 0.05)

elif app_mode == "Cek Brand Toko":
    st.sidebar.header("Kontrol Cek Brand")
    df_for_filters = st.session_state.df

    unique_brands = sorted(df_for_filters["Brand_Utama"].unique())
    default_brand_index = unique_brands.index("ACER") if "ACER" in unique_brands else 0
    st.sidebar.selectbox(
        "Pilih Brand:",
        options=unique_brands,
        index=default_brand_index,
        key="cek_brand_brand",
    )

    min_date_cek = df_for_filters["Tanggal"].min().date()
    max_date_cek = df_for_filters["Tanggal"].max().date()
    st.sidebar.date_input(
        "Pilih TANGGAL:",
        value=max_date_cek,
        min_value=min_date_cek,
        max_value=max_date_cek,
        key="cek_brand_date",
    )

    st.sidebar.button("Tampilkan Analisis Brand", type="primary", key="cek_brand_button", width="stretch")

else:
    st.sidebar.info("Tampilan ini menganalisis harga jual produk Anda dibandingkan dengan Harga Pokok Penjualan (HPP) dari sheet 'DATABASE'.")

# ================================
# PERSIAPAN DATA UMUM
# ================================
df_filtered = df.copy()
if app_mode == "Tab Analisis":
    start_date_dt, end_date_dt = pd.to_datetime(start_date), pd.to_datetime(end_date)
    df_filtered = df[(df["Tanggal"] >= start_date_dt) & (df["Tanggal"] <= end_date_dt)].copy()

if df_filtered.empty and app_mode == "Tab Analisis":
    st.error("Tidak ada data di rentang tanggal yang dipilih.")
    st.stop()

latest_entries_overall = df.loc[df.groupby(["Toko", "Nama Produk"])["Tanggal"].idxmax()]
main_store_latest_overall = latest_entries_overall[latest_entries_overall["Toko"] == MY_STORE_NAME]
competitor_latest_overall = latest_entries_overall[latest_entries_overall["Toko"] != MY_STORE_NAME]

if app_mode == "Tab Analisis":
    df_filtered["Minggu"] = df_filtered["Tanggal"].dt.to_period("W-SUN").apply(lambda p: p.start_time).dt.date
    main_store_df = df_filtered[df_filtered["Toko"] == MY_STORE_NAME]
    competitor_df = df_filtered[df_filtered["Toko"] != MY_STORE_NAME]
    latest_entries_weekly = df_filtered.loc[df_filtered.groupby(["Minggu", "Toko", "Nama Produk"])["Tanggal"].idxmax()]

# =========================================================================================
# ================================ TAMPILAN KONTEN UTAMA ================================
# =========================================================================================

if app_mode == "Tab Analisis":
    st.header("📈 Tampilan Analisis Penjualan & Kompetitor")
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "⭐ Analisis Toko Saya",
        "🏆 Analisis Brand Kompetitor",
        "📦 Status Stok Produk",
        "📈 Kinerja Penjualan",
        "📊 Analisis Mingguan",
    ])

    with tab1:
        st.header(f"Analisis Kinerja Toko: {MY_STORE_NAME}")
        section_counter = 1
        st.subheader(f"{section_counter}. Analisis Kategori Terlaris (Berdasarkan Omzet)")
        section_counter += 1
        if "KATEGORI" in main_store_latest_overall.columns:
            main_store_cat = main_store_latest_overall.copy()
            main_store_cat["KATEGORI"] = main_store_cat["KATEGORI"].fillna("Lainnya").astype(str).str.strip().replace({"": "Lainnya", "nan": "Lainnya", "None": "Lainnya"})
            category_sales = main_store_cat.groupby("KATEGORI")["Omzet"].sum().reset_index()
            if not category_sales.empty:
                cat_sales_sorted = category_sales.sort_values("Omzet", ascending=False).head(10).copy()
                cat_sales_sorted["Omzet_Singkat"] = cat_sales_sorted["Omzet"].apply(format_rupiah_singkat)
                cat_sales_sorted["Omzet_Rp"] = cat_sales_sorted["Omzet"].apply(format_rupiah)
                fig_cat = px.bar(
                    cat_sales_sorted,
                    x="KATEGORI",
                    y="Omzet",
                    title="Top 10 Kategori Berdasarkan Omzet",
                    text="Omzet_Singkat",
                    custom_data=["Omzet_Rp"],
                )
                fig_cat.update_traces(
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>Omzet: %{customdata[0]}<extra></extra>",
                )
                fig_cat.update_layout(yaxis_tickprefix="Rp ", yaxis_separatethousands=True)
                st.plotly_chart(fig_cat, width="stretch")
                st.markdown("##### Rincian Data Omzet per Kategori")
                table_cat_sales = cat_sales_sorted[["KATEGORI", "Omzet"]].copy()
                table_cat_sales["Omzet"] = table_cat_sales["Omzet"].apply(format_rupiah)
                st.dataframe(table_cat_sales, width="stretch", hide_index=True)
                st.markdown("---")
                st.subheader("Lihat Produk Terlaris per Kategori")
                category_list = category_sales.sort_values("Omzet", ascending=False)["KATEGORI"].tolist()
                selected_category = st.selectbox("Pilih Kategori untuk melihat produk terlaris:", options=category_list)
                if selected_category:
                    products_in_category = main_store_cat[main_store_cat["KATEGORI"] == selected_category].copy()
                    top_products_in_category = products_in_category.sort_values("Terjual per Bulan", ascending=False)
                    if top_products_in_category.empty:
                        st.info(f"Tidak ada produk terlaris untuk kategori '{selected_category}'.")
                    else:
                        if "SKU" not in top_products_in_category.columns:
                            top_products_in_category["SKU"] = "N/A"
                        columns_to_display = ["Nama Produk", "SKU", "Harga", "Terjual per Bulan", "Omzet"]
                        display_table = top_products_in_category[columns_to_display].copy()
                        display_table["Harga"] = display_table["Harga"].apply(format_rupiah)
                        display_table["Omzet"] = display_table["Omzet"].apply(format_rupiah)
                        st.dataframe(display_table, width="stretch", hide_index=True)
            else:
                st.info("Tidak ada data omzet per kategori untuk ditampilkan.")
        else:
            st.warning("Kolom 'KATEGORI' tidak ditemukan pada data toko Anda. Analisis ini dilewati.")

        st.subheader(f"{section_counter}. Produk Terlaris")
        section_counter += 1
        top_products = main_store_latest_overall.sort_values("Terjual per Bulan", ascending=False).head(15).copy()
        top_products["Harga_rp"] = top_products["Harga"].apply(format_rupiah)
        top_products["Omzet_rp"] = top_products["Omzet"].apply(format_rupiah)
        if "SKU" not in top_products.columns:
            top_products["SKU"] = "N/A"
        display_cols_top = ["Nama Produk", "SKU", "Harga_rp", "Omzet_rp", "Terjual per Bulan"]
        display_df_top = top_products[display_cols_top].rename(columns={"Harga_rp": "Harga", "Omzet_rp": "Omzet"})
        st.dataframe(display_df_top, width="stretch", hide_index=True)

        st.subheader(f"{section_counter}. Distribusi Omzet Brand")
        section_counter += 1
        brand_omzet_main = main_store_latest_overall.groupby("Brand")["Omzet"].sum().reset_index()
        if not brand_omzet_main.empty:
            top_brand_main = brand_omzet_main.sort_values("Omzet", ascending=False).head(7).copy()
            top_brand_main["Omzet_Singkat"] = top_brand_main["Omzet"].apply(format_rupiah_singkat)
            fig_brand_pie = px.pie(
                top_brand_main,
                names="Brand",
                values="Omzet",
                title="Distribusi Omzet Top 7 Brand (Snapshot Terakhir)",
            )
            fig_brand_pie.update_traces(
                text=top_brand_main["Omzet_Singkat"],
                textposition="outside",
                texttemplate="%{label}<br><b>%{text}</b><br>(%{percent})",
                hovertemplate="<b>%{label}</b><br>Omzet: Rp %{value:,.0f} (%{percent})<extra></extra>",
                insidetextfont=dict(color="white"),
            )
            fig_brand_pie.update_layout(showlegend=False)
            st.plotly_chart(fig_brand_pie, width="stretch")
        else:
            st.info("Tidak ada data omzet brand.")

        st.subheader(f"{section_counter}. Ringkasan Kinerja Mingguan (WoW Growth)")
        section_counter += 1
        main_store_latest_weekly = main_store_df.loc[main_store_df.groupby(["Minggu", "Nama Produk"])["Tanggal"].idxmax()]
        weekly_summary_tab1 = main_store_latest_weekly.groupby("Minggu").agg(
            Omzet=("Omzet", "sum"), Penjualan_Unit=("Terjual per Bulan", "sum")
        ).reset_index().sort_values("Minggu")
        weekly_summary_tab1["Pertumbuhan Omzet (WoW)"] = weekly_summary_tab1["Omzet"].pct_change().apply(format_wow_growth)
        weekly_summary_tab1["Omzet"] = weekly_summary_tab1["Omzet"].apply(format_rupiah)
        styled_weekly = apply_cell_style(
            weekly_summary_tab1[["Minggu", "Omzet", "Penjualan_Unit", "Pertumbuhan Omzet (WoW)"]].style,
            style_wow_growth,
            subset=["Pertumbuhan Omzet (WoW)"],
        )
        st.dataframe(styled_weekly, width="stretch", hide_index=True)

    with tab2:
        st.header("Analisis Brand di Toko Kompetitor")
        if competitor_df.empty:
            st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
        else:
            competitor_list = sorted(competitor_df["Toko"].unique())
            for competitor_store in competitor_list:
                with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                    single_competitor_df = competitor_latest_overall[competitor_latest_overall["Toko"] == competitor_store]
                    brand_analysis = single_competitor_df.groupby("Brand").agg(
                        Total_Omzet=("Omzet", "sum"),
                        Total_Unit_Terjual=("Terjual per Bulan", "sum"),
                    ).reset_index().sort_values("Total_Omzet", ascending=False)
                    if not brand_analysis.empty:
                        display_brand_analysis = brand_analysis.head(10).copy()
                        display_brand_analysis["Total_Omzet"] = display_brand_analysis["Total_Omzet"].apply(format_rupiah)
                        top7_comp = brand_analysis.head(7).copy()
                        top7_comp["Omzet_Singkat"] = top7_comp["Total_Omzet"].apply(format_rupiah_singkat)
                        fig_pie_comp = px.pie(
                            top7_comp,
                            names="Brand",
                            values="Total_Omzet",
                            title=f"Distribusi Omzet Top 7 Brand di {competitor_store} (Snapshot Terakhir)",
                        )
                        fig_pie_comp.update_traces(
                            text=top7_comp["Omzet_Singkat"],
                            textposition="outside",
                            texttemplate="%{label}<br><b>%{text}</b><br>(%{percent})",
                            hovertemplate="<b>%{label}</b><br>Total Omzet: Rp %{value:,.0f} (%{percent})<extra></extra>",
                        )
                        fig_pie_comp.update_layout(showlegend=False)

                        col_table, col_chart = st.columns([1, 1.2])
                        with col_table:
                            st.dataframe(display_brand_analysis, width="stretch", hide_index=True)
                        with col_chart:
                            st.plotly_chart(fig_pie_comp, width="stretch")
                    else:
                        st.info("Tidak ada data brand untuk toko ini.")

    with tab3:
        st.header("Tren Status Stok Mingguan per Toko")
        stock_trends = df_filtered.groupby(["Minggu", "Toko", "Status"]).size().unstack(fill_value=0).reset_index()
        if "Tersedia" not in stock_trends.columns:
            stock_trends["Tersedia"] = 0
        if "Habis" not in stock_trends.columns:
            stock_trends["Habis"] = 0
        stock_trends_melted = stock_trends.melt(
            id_vars=["Minggu", "Toko"],
            value_vars=["Tersedia", "Habis"],
            var_name="Tipe Stok",
            value_name="Jumlah Produk",
        )
        fig_stock_trends = px.line(
            stock_trends_melted,
            x="Minggu",
            y="Jumlah Produk",
            color="Toko",
            line_dash="Tipe Stok",
            markers=True,
            title="Jumlah Produk Tersedia vs. Habis per Minggu",
        )
        st.plotly_chart(fig_stock_trends, width="stretch")
        st.dataframe(stock_trends.set_index("Minggu"), width="stretch")

    with tab4:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        all_stores_latest_per_week = latest_entries_weekly.groupby(["Minggu", "Toko"])["Omzet"].sum().reset_index()
        all_stores_latest_per_week["Omzet_Rp"] = all_stores_latest_per_week["Omzet"].apply(format_rupiah)
        fig_weekly_omzet = px.line(
            all_stores_latest_per_week,
            x="Minggu",
            y="Omzet",
            color="Toko",
            markers=True,
            title="Perbandingan Omzet Mingguan Antar Toko (Berdasarkan Snapshot Terakhir)",
            custom_data=["Toko", "Omzet_Rp"],
        )
        fig_weekly_omzet.update_traces(
            hovertemplate="<b>%{customdata[0]}</b><br>Minggu: %{x}<br>Omzet: %{customdata[1]}<extra></extra>"
        )
        fig_weekly_omzet.update_layout(yaxis_tickprefix="Rp ", yaxis_separatethousands=True)
        st.plotly_chart(fig_weekly_omzet, width="stretch")
        st.subheader("Tabel Rincian Omzet per Tanggal")
        if not df_filtered.empty:
            omzet_pivot = df_filtered.pivot_table(index="Toko", columns="Tanggal", values="Omzet", aggfunc="sum").fillna(0)
            omzet_pivot.columns = [col.strftime("%d %b %Y") for col in omzet_pivot.columns]
            for col in omzet_pivot.columns:
                omzet_pivot[col] = omzet_pivot[col].apply(lambda x: format_rupiah(x) if x > 0 else "-")
            omzet_pivot.reset_index(inplace=True)
            st.info("Anda bisa scroll tabel ini ke samping untuk melihat tanggal lainnya.")
            st.dataframe(omzet_pivot, width="stretch", hide_index=True)
        else:
            st.warning("Tidak ada data untuk ditampilkan dalam tabel.")

    with tab5:
        st.header("Analisis Produk Baru Mingguan")
        weeks = sorted(df_filtered["Minggu"].unique())
        if len(weeks) < 2:
            st.info("Butuh setidaknya 2 minggu data untuk melakukan perbandingan produk baru.")
        else:
            col1, col2 = st.columns(2)
            week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0)
            week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks) - 1)
            if week_before >= week_after:
                st.error("Minggu Penentu harus setelah Minggu Pembanding.")
            else:
                all_stores = sorted(df_filtered["Toko"].unique())
                for store in all_stores:
                    with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                        products_before = set(
                            df_filtered[(df_filtered["Toko"] == store) & (df_filtered["Minggu"] == week_before) & (df_filtered["Status"] == "Tersedia")]["Nama Produk"]
                        )
                        products_after = set(
                            df_filtered[(df_filtered["Toko"] == store) & (df_filtered["Minggu"] == week_after) & (df_filtered["Status"] == "Tersedia")]["Nama Produk"]
                        )
                        new_products = products_after - products_before
                        if not new_products:
                            st.write("Tidak ada produk baru yang terdeteksi.")
                        else:
                            st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                            new_products_df = df_filtered[
                                df_filtered["Nama Produk"].isin(new_products)
                                & (df_filtered["Toko"] == store)
                                & (df_filtered["Minggu"] == week_after)
                            ].copy()
                            new_products_df["Harga_fmt"] = new_products_df["Harga"].apply(format_rupiah)
                            cols_show = ["Nama Produk", "Harga_fmt"]
                            if "Stok" in new_products_df.columns:
                                cols_show.append("Stok")
                            cols_show.append("Brand")
                            st.dataframe(new_products_df[cols_show].rename(columns={"Harga_fmt": "Harga"}), width="stretch", hide_index=True)

elif app_mode == "Cari Perbandingan":
    st.header(f"⚖️ Cari Perbandingan Produk '{MY_STORE_NAME}' (Live)")
    st.info("Pilih produk dari toko Anda untuk dicari kemiripannya di toko kompetitor secara langsung menggunakan TF-IDF.")
    if main_store_latest_overall.empty or competitor_latest_overall.empty:
        st.warning("Data toko utama atau kompetitor belum tersedia.")
    else:
        products_to_show_df = main_store_latest_overall.copy()
        brand_list = sorted(products_to_show_df["Brand"].unique())
        selected_brand = st.selectbox("Filter berdasarkan Brand:", ["Semua Brand"] + brand_list, key="brand_select_compare")
        if selected_brand != "Semua Brand":
            products_to_show_df = products_to_show_df[products_to_show_df["Brand"] == selected_brand]
        product_list = sorted(products_to_show_df["Nama Produk"].unique())
        selected_product = st.selectbox("Pilih produk dari toko Anda untuk dicari:", product_list, key="product_select_compare")
        if selected_product and st.button(f"Cari Padanan untuk '{selected_product}'", type="primary"):
            my_product_info = main_store_latest_overall[main_store_latest_overall["Nama Produk"] == selected_product].iloc[0]
            with st.spinner("Menganalisis kemiripan dengan produk kompetitor..."):
                my_product_list = [selected_product]
                competitor_product_list = competitor_latest_overall["Nama Produk"].tolist()
                vectorizer = TfidfVectorizer(analyzer="char", ngram_range=(3, 5))
                competitor_matrix = vectorizer.fit_transform(competitor_product_list)
                my_product_matrix = vectorizer.transform(my_product_list)
                sim_scores = cosine_similarity(my_product_matrix, competitor_matrix)[0]
                matching_indices = np.where(sim_scores >= accuracy_cutoff)[0]
                my_price = int(my_product_info["Harga"])
                my_sold = int(my_product_info["Terjual per Bulan"])
                my_omzet = int(my_product_info["Omzet"])
                my_status = "Ready" if my_product_info["Status"] == "Tersedia" else str(my_product_info["Status"])

                comparison_data = []
                for comp_idx in matching_indices:
                    score = sim_scores[comp_idx]
                    competitor_product_row = competitor_latest_overall.iloc[comp_idx]
                    comp_price = int(competitor_product_row["Harga"])
                    price_diff = comp_price - my_price
                    if price_diff > 0:
                        harga_label = f"{format_rupiah(comp_price)} (Lebih Mahal {format_rupiah(price_diff)})"
                    elif price_diff < 0:
                        harga_label = f"{format_rupiah(comp_price)} (Lebih Murah {format_rupiah(abs(price_diff))})"
                    else:
                        harga_label = f"{format_rupiah(comp_price)} (Harga Sama)"
                    comparison_data.append({
                        "Nama Produk": competitor_product_row["Nama Produk"],
                        "Toko": competitor_product_row["Toko"],
                        "Harga_num": comp_price,
                        "Harga": harga_label,
                        "Terjual/Bln": int(competitor_product_row["Terjual per Bulan"]),
                        "Omzet Penjualan": format_rupiah(competitor_product_row["Omzet"]),
                        "Skor Kemiripan": float(score),
                    })

            st.divider()
            st.subheader(f"📦 Produk {MY_STORE_NAME}: {my_product_info['Nama Produk']}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Status", my_status)
            m2.metric("Harga Terbaru", format_rupiah(my_price))
            m3.metric("Penjualan / Bln", f"{my_sold:,} unit".replace(",", "."))
            m4.metric("Omzet Terkini", format_rupiah(my_omzet))

            st.subheader("Hasil Perbandingan Produk Kompetitor")
            if comparison_data:
                comparison_df = pd.DataFrame(comparison_data).sort_values(by="Harga_num", ascending=True).reset_index(drop=True)
                ordered_cols = ["Nama Produk", "Toko", "Harga", "Terjual/Bln", "Omzet Penjualan", "Skor Kemiripan"]
                st.dataframe(
                    comparison_df[ordered_cols],
                    width="stretch",
                    hide_index=True,
                    column_config={"Skor Kemiripan": st.column_config.ProgressColumn("Skor Kemiripan", format="%.2f", min_value=0.0, max_value=1.0)},
                )
            else:
                st.warning(f"Tidak ditemukan produk yang cocok di toko kompetitor dengan tingkat akurasi di atas {accuracy_cutoff}.")

elif app_mode == "HPP Produk":
    st.header("💰 Tampilan Analisis Harga Pokok Penjualan (HPP)")
    if db_df.empty or "SKU" not in db_df.columns:
        st.error("Sheet 'DATABASE' tidak ditemukan atau tidak memiliki kolom 'SKU'. Analisis HPP tidak dapat dilanjutkan.")
        st.stop()
    if "HPP (LATEST)" not in db_df.columns and "HPP (AVERAGE)" not in db_df.columns:
        st.warning("Sheet 'DATABASE' pada dataset saat ini belum memiliki kolom 'HPP (LATEST)' atau 'HPP (AVERAGE)'.")
        st.stop()

    db_work = db_df.copy()
    hpp_latest = db_work["HPP (LATEST)"].apply(_parse_price) if "HPP (LATEST)" in db_work.columns else pd.Series(np.nan, index=db_work.index)
    hpp_avg = db_work["HPP (AVERAGE)"].apply(_parse_price) if "HPP (AVERAGE)" in db_work.columns else pd.Series(np.nan, index=db_work.index)
    db_work["HPP"] = hpp_latest.fillna(hpp_avg)

    hpp_data = db_work[["SKU", "HPP"]].copy()
    hpp_data["SKU"] = hpp_data["SKU"].astype(str).str.strip()
    hpp_data.dropna(subset=["SKU", "HPP"], inplace=True)
    hpp_data = hpp_data[(hpp_data["SKU"] != "") & (hpp_data["SKU"] != "N/A")]
    hpp_data.drop_duplicates(subset=["SKU"], keep="first", inplace=True)

    latest_db_klik = main_store_latest_overall.copy()
    merged_df = pd.merge(latest_db_klik, hpp_data, on="SKU", how="left")
    merged_df["Selisih"] = merged_df["Harga"] - merged_df["HPP"]
    df_rugi = merged_df[merged_df["Selisih"] < 0].copy()
    df_untung = merged_df[merged_df["Selisih"] >= 0].copy()
    df_tidak_ditemukan = merged_df[merged_df["HPP"].isnull()].copy()

    st.subheader("🔴 Produk Lebih Murah dari HPP")
    if df_rugi.empty:
        st.success("👍 Mantap! Tidak ada produk yang dijual di bawah HPP.")
    else:
        display_rugi = df_rugi[["Nama Produk", "SKU", "Harga", "HPP", "Selisih", "Terjual per Bulan", "Omzet"]].copy()
        display_rugi.rename(columns={"Terjual per Bulan": "Terjual/Bln"}, inplace=True)
        for col in ["Harga", "HPP", "Selisih", "Omzet"]:
            display_rugi[col] = display_rugi[col].apply(format_rupiah)
        st.dataframe(display_rugi, width="stretch", hide_index=True)

    st.divider()
    st.subheader("🟢 Produk Lebih Mahal dari HPP")
    if df_untung.empty:
        st.warning("Tidak ada produk yang dijual di atas HPP.")
    else:
        display_untung = df_untung[["Nama Produk", "SKU", "Harga", "HPP", "Selisih", "Terjual per Bulan", "Omzet"]].copy()
        display_untung.rename(columns={"Terjual per Bulan": "Terjual/Bln"}, inplace=True)
        for col in ["Harga", "HPP", "Selisih", "Omzet"]:
            display_untung[col] = display_untung[col].apply(format_rupiah)
        st.dataframe(display_untung, width="stretch", hide_index=True)

    st.divider()
    st.subheader("❓ Produk Tidak Terdeteksi HPP-nya")
    if df_tidak_ditemukan.empty:
        st.success("👍 Semua produk yang dijual berhasil dicocokkan dengan data HPP di DATABASE.")
    else:
        st.warning("Mohon untuk mengecek data produk lagi, sepertinya ada data yang tidak akurat atau SKU tidak cocok.")
        display_tidak_ditemukan = df_tidak_ditemukan[["Nama Produk", "SKU", "Harga", "Terjual per Bulan", "Omzet"]].copy()
        display_tidak_ditemukan.rename(columns={"Terjual per Bulan": "Terjual/Bln"}, inplace=True)
        for col in ["Harga", "Omzet"]:
            display_tidak_ditemukan[col] = display_tidak_ditemukan[col].apply(format_rupiah)
        st.dataframe(display_tidak_ditemukan, width="stretch", hide_index=True)

elif app_mode == "Cek Brand Toko":
    st.header("🏪 Cek Brand Toko")
    st.info("Pilih brand, tanggal, dan klik 'Tampilkan Analisis Brand' di sidebar untuk memuat data.")

    if st.session_state.get("cek_brand_button"):
        selected_brand = st.session_state.cek_brand_brand
        selected_date = st.session_state.cek_brand_date

        st.markdown("---")
        st.subheader(f"Hasil Analisis untuk Brand '{selected_brand}' pada TANGGAL {selected_date.strftime('%d %B %Y')}")

        filtered_df = df[(df["Brand_Utama"] == selected_brand) & (df["Tanggal"].dt.date == selected_date)]
        if filtered_df.empty:
            st.warning("Tidak ada data ditemukan untuk brand dan TANGGAL yang dipilih.")
        else:
            summary_list = []
            all_stores = sorted(df["Toko"].unique())
            for store in all_stores:
                store_data = filtered_df[filtered_df["Toko"] == store]
                if store_data.empty:
                    total_omzet, total_terjual, ready_count, habis_count = 0, 0, 0, 0
                else:
                    total_omzet = store_data["Omzet"].sum()
                    total_terjual = store_data["Terjual per Bulan"].sum()
                    ready_count = store_data[store_data["Status"] == "Tersedia"].shape[0]
                    habis_count = store_data[store_data["Status"] == "Habis"].shape[0]

                summary_list.append({
                    "Toko": store,
                    "Total Omzet per Bulan": total_omzet,
                    "Total Produk Terjual per Bulan": total_terjual,
                    "Jumlah Produk Ready": ready_count,
                    "Jumlah Produk Habis": habis_count,
                })

            summary_df_sorted = pd.DataFrame(summary_list).set_index("Toko").sort_values(by="Total Omzet per Bulan", ascending=False)
            pivoted_summary_df = summary_df_sorted.T

            st.markdown("#### Ringkasan Performa Brand per Toko")

            chart_df = summary_df_sorted.reset_index().copy()
            chart_df["Omzet_Rp"] = chart_df["Total Omzet per Bulan"].apply(format_rupiah)
            chart_df["Omzet_Singkat"] = chart_df["Total Omzet per Bulan"].apply(format_rupiah_singkat)
            fig_brand_store = px.bar(
                chart_df,
                x="Total Omzet per Bulan",
                y="Toko",
                orientation="h",
                text="Omzet_Singkat",
                title=f"Perbandingan Omzet Brand '{selected_brand}' per Toko",
                custom_data=[
                    "Omzet_Rp",
                    "Total Produk Terjual per Bulan",
                    "Jumlah Produk Ready",
                    "Jumlah Produk Habis",
                ],
            )
            fig_brand_store.update_traces(
                textposition="outside",
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Total Omzet/Bln: %{customdata[0]}<br>"
                    "Terjual/Bln: %{customdata[1]:,.0f} unit<br>"
                    "Produk Ready: %{customdata[2]:,.0f}<br>"
                    "Produk Habis: %{customdata[3]:,.0f}<extra></extra>"
                ),
            )
            fig_brand_store.update_layout(
                yaxis={"categoryorder": "total ascending"},
                xaxis_tickprefix="Rp ",
                xaxis_separatethousands=True,
                margin=dict(r=60),
            )

            col_bar, col_tooltip = st.columns([2, 1])
            with col_bar:
                st.plotly_chart(fig_brand_store, width="stretch")
            with col_tooltip:
                total_omzet_all = int(chart_df["Total Omzet per Bulan"].sum())
                total_terjual_all = int(chart_df["Total Produk Terjual per Bulan"].sum())
                total_ready_all = int(chart_df["Jumlah Produk Ready"].sum())
                total_habis_all = int(chart_df["Jumlah Produk Habis"].sum())
                top_store_row = chart_df.iloc[0]
                st.info(
                    f"**Ringkasan Tooltip ({selected_brand})**\n\n"
                    f"- **Toko Tertinggi:** {top_store_row['Toko']} ({top_store_row['Omzet_Rp']})\n"
                    f"- **Total Omzet Semua Toko:** {format_rupiah(total_omzet_all)}\n"
                    f"- **Total Terjual/Bln:** {total_terjual_all:,} unit\n"
                    f"- **Total Ready / Habis:** {total_ready_all:,} / {total_habis_all:,} produk\n\n"
                    "*Arahkan kursor ke batang grafik di samping untuk melihat detail metrik tiap toko.*"
                )

            styler = (
                pivoted_summary_df.style.format(format_rupiah, subset=(pd.IndexSlice[["Total Omzet per Bulan"]], slice(None)))
                .format("{:,.0f}", subset=(pd.IndexSlice[["Total Produk Terjual per Bulan", "Jumlah Produk Ready", "Jumlah Produk Habis"]], slice(None)))
            )
            st.dataframe(styler, width="stretch")

            with st.expander("Lihat Daftar Produk Lengkap per Toko (Diurutkan berdasarkan Omzet)"):
                for store in summary_df_sorted.index.tolist():
                    st.markdown(f"##### 🏪 **{store}**")
                    store_data_detail = filtered_df[filtered_df["Toko"] == store].copy()
                    if store_data_detail.empty:
                        st.info(f"Brand **{selected_brand}** tidak ditemukan di toko ini pada tanggal tersebut.")
                    else:
                        store_data_detail.sort_values(by=["Omzet", "Terjual per Bulan"], ascending=[False, False], inplace=True)
                        store_data_detail["HARGA (Rp)"] = store_data_detail["Harga"].apply(format_rupiah)
                        store_data_detail["Omzet (Rp)"] = store_data_detail["Omzet"].apply(format_rupiah)
                        store_data_detail["Terjual per Bulan"] = store_data_detail["Terjual per Bulan"].astype(int)
                        kolom_tampilan = ["Nama Produk", "HARGA (Rp)", "Terjual per Bulan", "Omzet (Rp)", "Status"]
                        st.dataframe(store_data_detail[kolom_tampilan], width="stretch", hide_index=True)
