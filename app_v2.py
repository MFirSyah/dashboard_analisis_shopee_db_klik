# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI FINAL & POWERFUL
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Prinsip: Mesin Pengambilan Data V2 Otomatis, Tampilan & Analisis V1
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
from io import StringIO

# --- KONFIGURASI HALAMAN ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis Penjualan")

# --- KONFIGURASI ID & NAMA KOLOM ---
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq"
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg"
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
KATEGORI_SHEET_NAME = "DATABASE" # Sheet untuk pemetaan kategori dari app.py

NAMA_PRODUK_COL = "Nama Produk"
HARGA_COL = "Harga"
TERJUAL_COL = "Terjual per bulan"
STATUS_COL = "Status"
TOKO_COL = "Toko"
BRAND_COL = "BRAND"
TANGGAL_COL = "Tanggal"
OMZET_COL = "Omzet"

# --- FUNGSI-FUNGSI INTI (Mesin V2) ---

@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    """Melakukan autentikasi sekali dan mengembalikan service object untuk Drive dan Sheets."""
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        drive_service = build('drive', 'v3', credentials=creds)
        gsheets_service = gspread.authorize(creds)
        return drive_service, gsheets_service
    except Exception as e:
        st.error(f"Gagal melakukan autentikasi ke Google. Pastikan `secrets.toml` sudah benar. Error: {e}")
        st.stop()

@st.cache_data(show_spinner="Memuat 'otak' dari database...", ttl=300)
def load_intelligence_data(_gsheets_service, spreadsheet_id):
    """Memuat semua data pendukung: database brand, kamus alias, dan database kategori."""
    try:
        spreadsheet = _gsheets_service.open_by_key(spreadsheet_id)
        
        db_sheet = spreadsheet.worksheet(DB_SHEET_NAME)
        brand_db_list = [item for item in db_sheet.col_values(1) if item]
        
        kamus_sheet = spreadsheet.worksheet(KAMUS_SHEET_NAME)
        kamus_df = pd.DataFrame(kamus_sheet.get_all_records())
        if 'Alias' not in kamus_df.columns or 'Brand_Utama' not in kamus_df.columns:
            st.error(f"Sheet '{KAMUS_SHEET_NAME}' harus memiliki kolom 'Alias' dan 'Brand_Utama'.")
            kamus_dict = {}
        else:
            kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()

        try:
            kategori_sheet = spreadsheet.worksheet(KATEGORI_SHEET_NAME)
            db_kategori_df = pd.DataFrame(kategori_sheet.get_all_records())
            db_kategori_df.columns = [str(col).strip().upper() for col in db_kategori_df.columns]
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{KATEGORI_SHEET_NAME}' tidak ditemukan. Fitur analisis kategori tidak akan aktif.")
            db_kategori_df = pd.DataFrame()

        return brand_db_list, kamus_dict, db_kategori_df
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet. Error: {e}")
        st.stop()

@st.cache_data(show_spinner="Membaca semua data dari folder kompetitor...", ttl=300)
def get_all_competitor_data(_drive_service, parent_folder_id):
    """
    Fungsi "Anti Peluru": Membaca file CSV & Google Sheets, melewati file yang error,
    dan memberikan laporan diagnosis.
    """
    all_data = []
    problematic_files = []
    try:
        query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        results = _drive_service.files().list(q=query, fields="files(id, name)").execute()
        subfolders = results.get('files', [])

        if not subfolders:
            st.warning("Tidak ada subfolder yang ditemukan di dalam folder induk.")
            return pd.DataFrame(), []

        progress_bar = st.progress(0, text="Membaca data...")
        for i, folder in enumerate(subfolders):
            progress_bar.progress((i + 1) / len(subfolders), text=f"Membaca folder: {folder['name']}")
            
            file_query = f"'{folder['id']}' in parents and (mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet')"
            file_results = _drive_service.files().list(q=file_query, fields="files(id, name, mimeType)").execute()
            csv_files = file_results.get('files', [])

            for csv_file in csv_files:
                try:
                    file_id = csv_file.get('id')
                    file_name = csv_file.get('name')
                    mime_type = csv_file.get('mimeType')
                    
                    if mime_type == 'application/vnd.google-apps.spreadsheet':
                        request = _drive_service.files().export_media(fileId=file_id, mimeType='text/csv')
                    else:
                        request = _drive_service.files().get_media(fileId=file_id)

                    downloader = io.BytesIO(request.execute())
                    
                    if downloader.getbuffer().nbytes == 0:
                        problematic_files.append(f"{folder['name']}/{file_name} (File Kosong)")
                        continue

                    df = pd.read_csv(downloader)
                    
                    if NAMA_PRODUK_COL not in df.columns:
                        problematic_files.append(f"{folder['name']}/{file_name} (Kolom '{NAMA_PRODUK_COL}' tidak ditemukan)")
                        continue

                    df[TOKO_COL] = folder['name']
                    
                    match_tanggal = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
                    df[TANGGAL_COL] = pd.to_datetime(match_tanggal.group(1), format='%Y-%m-%d') if match_tanggal else pd.NaT
                    
                    if 'ready' in file_name.lower():
                        df[STATUS_COL] = 'Tersedia'
                    elif 'habis' in file_name.lower():
                        df[STATUS_COL] = 'Habis'
                    else:
                        df[STATUS_COL] = 'N/A'
                        
                    all_data.append(df)
                except Exception as file_error:
                    problematic_files.append(f"{folder['name']}/{file_name} (Error: {file_error})")
                    continue
        
        progress_bar.empty()

        if not all_data: return pd.DataFrame(), problematic_files
        
        final_df = pd.concat(all_data, ignore_index=True)
        
        for col in [HARGA_COL, TERJUAL_COL]:
            if col not in final_df.columns:
                final_df[col] = 0
            else:
                final_df[col] = final_df[col].astype(str).str.replace(r'[^\d]', '', regex=True)
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
        
        final_df[TERJUAL_COL] = final_df[TERJUAL_COL].astype(int)
        final_df[OMZET_COL] = final_df[HARGA_COL] * final_df[TERJUAL_COL]

        return final_df, problematic_files
        
    except Exception as e:
        st.error(f"Terjadi kesalahan fatal saat mengambil data: {e}")
        return pd.DataFrame(), []

def label_brands(df, brand_db, kamus_brand, fuzzy_threshold=88):
    if NAMA_PRODUK_COL not in df.columns: st.stop()
    brand_db_sorted = sorted(brand_db, key=len, reverse=True)
    brands = []
    for _, row in df.iterrows():
        product_name = str(row[NAMA_PRODUK_COL]).upper()
        found_brand = None
        for alias, brand_utama in kamus_brand.items():
            if re.search(r'\b' + re.escape(str(alias).upper()) + r'\b', product_name):
                found_brand = brand_utama; break
        if found_brand: brands.append(found_brand); continue
        for brand in brand_db_sorted:
            if re.search(r'\b' + re.escape(brand.upper()) + r'\b', product_name) or (brand.upper() in product_name.replace(" ", "")):
                found_brand = brand; break
        if found_brand: brands.append(found_brand); continue
        best_match = process.extractOne(product_name, brand_db, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold: found_brand = best_match[0]
        brands.append(found_brand if found_brand else "TIDAK DIKETAHUI")
    df[BRAND_COL] = brands
    return df

def update_google_sheet(gsheets_service, spreadsheet_id, sheet_name, values):
    try:
        sheet = gsheets_service.open_by_key(spreadsheet_id).worksheet(sheet_name)
        sheet.append_row(values, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Gagal mengupdate Google Sheet: {e}")
        return False

# --- FUNGSI BANTU ANALISIS ---
def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

def format_harga_aman(x):
    if pd.isnull(x): return "N/A"
    try: return f"Rp {float(x):,.0f}"
    except (ValueError, TypeError): return str(x)

def colorize_growth(val):
    if isinstance(val, str):
        if '▲' in val: return 'color: #28a745'
        elif '▼' in val: return 'color: #dc3545'
    return 'color: inherit'

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# --- FUNGSI MASTER UNTUK MEMUAT SEMUA DATA (OTOMATIS) ---
@st.cache_data(show_spinner=False)
def load_master_data():
    drive_service, gsheets_service = get_google_apis()
    brand_db, kamus_brand, db_kategori = load_intelligence_data(gsheets_service, SPREADSHEET_ID)
    raw_df, problematic_files = get_all_competitor_data(drive_service, PARENT_FOLDER_ID)
    
    if raw_df is None or raw_df.empty:
        return None, None, None, None, problematic_files
        
    master_df = label_brands(raw_df.copy(), brand_db, kamus_brand)
    return master_df, brand_db, kamus_brand, db_kategori, problematic_files

# --- TAMPILAN APLIKASI STREAMLIT ---
st.title("Dashboard Analisis Penjualan & Kompetitor")

master_df, brand_db, kamus_brand, db_kategori, problematic_files = load_master_data()

if problematic_files:
    st.warning("Beberapa file gagal diproses dan dilewati:")
    with st.expander("Klik untuk melihat detail file bermasalah"):
        for file_info in problematic_files:
            st.code(file_info)

if master_df is None:
    st.error("Gagal memuat data utama atau tidak ada data valid yang ditemukan.")
    st.stop()

st.session_state.brand_db = brand_db
st.session_state.kamus_brand = kamus_brand
st.session_state.master_df = master_df

df_labeled = master_df[master_df[BRAND_COL] != 'TIDAK DIKETAHUI'].copy()

# --- Sidebar Kontrol Lanjutan ---
st.sidebar.header("Filter & Pengaturan")
all_stores = sorted(df_labeled[TOKO_COL].unique())
main_store = st.sidebar.selectbox("Pilih Toko Utama:", all_stores, index=0 if all_stores else -1)

try:
    df_with_dates = df_labeled.dropna(subset=[TANGGAL_COL]).copy()
    if df_with_dates.empty:
        st.error("Tidak ada data dengan tanggal yang valid ditemukan. Pastikan nama file CSV Anda mengandung tanggal dengan format YYYY-MM-DD.")
        st.stop()
    min_date, max_date = df_with_dates[TANGGAL_COL].min().date(), df_with_dates[TANGGAL_COL].max().date()
    selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
except Exception as e:
    st.error(f"Terjadi Error Saat Membuat Filter Tanggal! Periksa format nama file CSV Anda. Detail: {e}")
    st.stop()

if len(selected_date_range) != 2: st.stop()
start_date, end_date = selected_date_range
accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1)

df_filtered = df_with_dates[(df_with_dates[TANGGAL_COL].dt.date >= start_date) & (df_with_dates[TANGGAL_COL].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()

df_filtered['Minggu'] = df_filtered[TANGGAL_COL].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
main_store_df = df_filtered[df_filtered[TOKO_COL] == main_store].copy()
competitor_df = df_filtered[df_filtered[TOKO_COL] != main_store].copy()

csv_to_download = convert_df_to_csv(df_filtered)
st.sidebar.download_button(
   label="📥 Download Data Analisis (CSV)", data=csv_to_download,
   file_name=f'analisis_data_{start_date}_sd_{end_date}.csv', mime='text/csv',
)

st.sidebar.header("Navigasi")
page = st.sidebar.radio("Pilih Halaman:", ["Analisis Penjualan", "Produk Belum Ternamai"])

if page == "Analisis Penjualan":
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([f"⭐ Analisis Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

    with tab1:
        st.header(f"Analisis Kinerja Toko: {main_store}")
        
        st.subheader("1. Kategori Produk Terlaris")
        if not db_kategori.empty and 'KATEGORI' in db_kategori.columns and 'NAMA' in db_kategori.columns:
            @st.cache_data
            def fuzzy_merge_categories(_rekap_df, _database_df):
                _rekap_df['Kategori'] = 'Lainnya'
                db_unique = _database_df.drop_duplicates(subset=['NAMA'])
                db_map = db_unique.set_index('NAMA')['KATEGORI']
                for index, row in _rekap_df.iterrows():
                    if pd.notna(row[NAMA_PRODUK_COL]):
                        match, score = process.extractOne(row[NAMA_PRODUK_COL], db_map.index, scorer=fuzz.token_set_ratio)
                        if score >= 95: _rekap_df.loc[index, 'Kategori'] = db_map[match]
                return _rekap_df
            
            main_store_df_cat = fuzzy_merge_categories(main_store_df.copy(), db_kategori)
            category_sales = main_store_df_cat.groupby('Kategori')[TERJUAL_COL].sum().reset_index()
            
            if not category_sales.empty:
                col1, col2 = st.columns([1,2])
                sort_order_cat = col1.radio("Urutkan:", ["Terlaris", "Kurang Laris"], horizontal=True, key="cat_sort")
                
                max_categories = len(category_sales)
                default_top_n = min(10, max_categories)
                
                top_n_cat = col2.number_input("Tampilkan Top:", min_value=1, max_value=max_categories, value=default_top_n, key="cat_top_n")
                
                cat_sales_sorted = category_sales.sort_values(TERJUAL_COL, ascending=(sort_order_cat == "Kurang Laris")).head(top_n_cat)
                fig_cat = px.bar(cat_sales_sorted, x='Kategori', y=TERJUAL_COL, title=f'Top {top_n_cat} Kategori', text_auto=True)
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.info("Tidak ada data penjualan kategori untuk ditampilkan pada rentang tanggal ini.")

        else:
            st.info(f"Sheet '{KATEGORI_SHEET_NAME}' tidak ditemukan atau format salah. Analisis kategori tidak dapat ditampilkan.")

        st.subheader("2. Produk Terlaris")
        top_products = main_store_df.sort_values(TERJUAL_COL, ascending=False).head(15)[[NAMA_PRODUK_COL, TERJUAL_COL, OMZET_COL]]
        top_products[OMZET_COL] = top_products[OMZET_COL].apply(format_harga_aman)
        st.dataframe(top_products, use_container_width=True, hide_index=True)

        st.subheader("3. Distribusi Omzet Brand (Top 6)")
        brand_omzet_main = main_store_df.groupby(BRAND_COL)[OMZET_COL].sum().nlargest(6).reset_index()
        if not brand_omzet_main.empty:
            fig_brand_pie = px.pie(brand_omzet_main, names=BRAND_COL, values=OMZET_COL, title='Top 6 Brand Terlaris berdasarkan Omzet')
            fig_brand_pie.update_traces(texttemplate='%{label}<br>%{percent}<br>%{value:,.0f}')
            st.plotly_chart(fig_brand_pie, use_container_width=True)
        else:
            st.info("Tidak ada data omzet brand untuk ditampilkan.")

    with tab2:
        st.header(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
        st.subheader("1. Ringkasan Kinerja Mingguan (WoW Growth)")
        weekly_summary = main_store_df.groupby('Minggu').agg(Omzet=(OMZET_COL, 'sum'), Penjualan_Unit=(TERJUAL_COL, 'sum')).reset_index()
        weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change().apply(format_wow_growth)
        weekly_summary_display = weekly_summary.copy()
        weekly_summary_display['Omzet'] = weekly_summary_display['Omzet'].apply(format_harga_aman)
        st.dataframe(weekly_summary_display.style.applymap(colorize_growth, subset=['Pertumbuhan Omzet (WoW)']), use_container_width=True, hide_index=True)

        st.subheader("2. Detail Produk di Toko Anda (Data Terbaru)")
        if not main_store_df.empty:
            latest_date = main_store_df[TANGGAL_COL].max()
            main_store_latest = main_store_df[main_store_df[TANGGAL_COL] == latest_date].copy()
            main_store_latest[HARGA_COL] = main_store_latest[HARGA_COL].apply(format_harga_aman)
            st.dataframe(main_store_latest[[TANGGAL_COL, NAMA_PRODUK_COL, HARGA_COL, STATUS_COL]], use_container_width=True, hide_index=True)
            
            st.subheader("3. Pilih Produk untuk Dibandingkan")
            search_query = st.text_input("Cari produk berdasarkan nama, brand, atau kata kunci:", key="search_product")
            product_list = sorted(main_store_latest[NAMA_PRODUK_COL].unique())
            if search_query: product_list = [p for p in product_list if search_query.lower() in p.lower()]
            if not product_list: st.warning("Tidak ada produk yang cocok dengan pencarian Anda.")
            else:
                selected_product = st.selectbox("Pilih produk dari hasil pencarian:", product_list)
                if selected_product:
                    product_info = main_store_latest[main_store_latest[NAMA_PRODUK_COL] == selected_product].iloc[0]
                    st.markdown(f"**Produk Pilihan Anda:** *{product_info[NAMA_PRODUK_COL]}*")
                    col1, col2 = st.columns(2)
                    col1.metric(f"Harga di {main_store}", product_info[HARGA_COL])
                    col2.metric(f"Status", product_info[STATUS_COL])
                    
                    st.markdown("---"); st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                    competitor_latest = competitor_df[competitor_df[TANGGAL_COL] == latest_date]
                    if not competitor_latest.empty:
                        query_name = product_info[NAMA_PRODUK_COL]
                        competitor_product_list = competitor_latest[NAMA_PRODUK_COL].tolist()
                        matches = process.extract(query_name, competitor_product_list, limit=5, scorer=fuzz.token_set_ratio)
                        valid_matches = [m for m in matches if m[1] >= accuracy_cutoff]
                        if not valid_matches: st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                        else:
                            for product, score in valid_matches:
                                match_info = competitor_latest[competitor_latest[NAMA_PRODUK_COL] == product].iloc[0]
                                price_diff = match_info[HARGA_COL] - product_info[HARGA_COL]
                                st.markdown(f"**Toko: {match_info[TOKO_COL]}** (Kemiripan: {int(score)}%)")
                                st.markdown(f"*{match_info[NAMA_PRODUK_COL]}*")
                                c1, c2, c3 = st.columns(3)
                                c1.metric("Harga Kompetitor", format_harga_aman(match_info[HARGA_COL]), delta=f"Rp {price_diff:,.0f}")
                                c2.metric("Status", match_info[STATUS_COL])
                                c3.metric(f"Terjual/Bln", f"{int(match_info[TERJUAL_COL])}")

    # ... (Sisa kode untuk tab 3, 4, 5, 6 tetap sama) ...
    with tab3:
        st.header("Analisis Brand di Toko Kompetitor")
        if competitor_df.empty: st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
        else:
            for competitor_store in sorted(competitor_df[TOKO_COL].unique()):
                with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                    single_competitor_df = competitor_df[competitor_df[TOKO_COL] == competitor_store]
                    brand_analysis = single_competitor_df.groupby(BRAND_COL).agg(Total_Omzet=(OMZET_COL, 'sum'), Total_Unit_Terjual=(TERJUAL_COL, 'sum')).reset_index().sort_values("Total_Omzet", ascending=False)
                    col1, col2 = st.columns([3,2])
                    with col1:
                        st.markdown("**Peringkat Brand**")
                        brand_analysis['Total_Omzet'] = brand_analysis['Total_Omzet'].apply(format_harga_aman)
                        st.dataframe(brand_analysis[[BRAND_COL, 'Total_Unit_Terjual', 'Total_Omzet']], use_container_width=True, hide_index=True)
                    with col2:
                        top_6_brands_omzet = single_competitor_df.groupby(BRAND_COL)[OMZET_COL].sum().nlargest(6).reset_index()
                        fig_pie_comp = px.pie(top_6_brands_omzet, names=BRAND_COL, values=OMZET_COL, title='Top 6 Brand Omzet')
                        fig_pie_comp.update_traces(textinfo='percent+label', hovertemplate='<b>%{label}</b><br>Omzet: %{value:,.0f}<br>Persentase: %{percent}')
                        st.plotly_chart(fig_pie_comp, use_container_width=True)
    with tab4:
        st.header("Tren Status Stok Mingguan per Toko")
        stock_trends = df_filtered.groupby(['Minggu', TOKO_COL, STATUS_COL]).size().unstack(fill_value=0).reset_index()
        if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
        if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
        stock_trends_melted = stock_trends.melt(id_vars=['Minggu', TOKO_COL], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
        fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color=TOKO_COL, line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
        st.plotly_chart(fig_stock_trends, use_container_width=True)
        st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)
    with tab5:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        st.subheader("1. Grafik Omzet Mingguan")
        weekly_omzet = df_filtered.groupby(['Minggu', TOKO_COL])[OMZET_COL].sum().reset_index()
        fig_weekly_omzet = px.line(weekly_omzet, x='Minggu', y=OMZET_COL, color=TOKO_COL, markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
        st.plotly_chart(fig_weekly_omzet, use_container_width=True)
        st.subheader("2. Tabel Ringkasan Kinerja Mingguan per Toko")
        summary_list = []
        for store in sorted(df_filtered[TOKO_COL].unique()):
            store_df = df_filtered[df_filtered[TOKO_COL] == store]
            weekly_summary = store_df.groupby('Minggu').agg(
                Total_Omzet=(OMZET_COL, 'sum'),
                Total_Terjual=(TERJUAL_COL, 'sum'),
                Rata_Rata_Harga=(HARGA_COL, 'mean')
            ).reset_index()
            if not weekly_summary.empty:
                weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Total_Omzet'].pct_change()
                weekly_summary['Toko'] = store
                summary_list.append(weekly_summary)
        if summary_list:
            final_summary = pd.concat(summary_list)
            final_summary['Rata_Rata_Terjual_Harian'] = (final_summary['Total_Terjual'] / 7).round().astype(int)
            
            # PERBAIKAN: Menggunakan nama kolom yang benar (dengan underscore)
            final_summary_display = final_summary.copy()
            final_summary_display['Pertumbuhan Omzet (WoW)'] = final_summary_display['Pertumbuhan Omzet (WoW)'].apply(format_wow_growth)
            final_summary_display['Total Omzet'] = final_summary_display['Total_Omzet'].apply(format_harga_aman)
            final_summary_display['Rata-Rata Harga'] = final_summary_display['Rata_Rata_Harga'].apply(format_harga_aman)
            
            # Mengganti nama kolom hanya untuk ditampilkan
            display_df = final_summary_display.rename(columns={
                'Minggu': 'Mulai Minggu',
                'Total_Omzet': 'Total Omzet',
                'Total_Terjual': 'Total Terjual',
                'Rata_Rata_Harga': 'Rata-Rata Harga',
                'Rata_Rata_Terjual_Harian': 'Rata-Rata Terjual Harian'
            })
            
            st.dataframe(display_df[['Mulai Minggu', 'Toko', 'Total Omzet', 'Pertumbuhan Omzet (WoW)', 'Total Terjual', 'Rata-Rata Terjual Harian', 'Rata-Rata Harga']]
            .style.applymap(colorize_growth, subset=['Pertumbuhan Omzet (WoW)']), use_container_width=True, hide_index=True)

    with tab6:
        st.header("Analisis Produk Baru Mingguan")
        st.subheader("Perbandingan Produk Baru Antar Minggu")
        weeks = sorted(df_filtered['Minggu'].unique())
        if len(weeks) < 2: st.info("Butuh setidaknya 2 minggu data untuk melakukan perbandingan produk baru.")
        else:
            col1, col2 = st.columns(2)
            week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0)
            week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)
            if week_before >= week_after: st.error("Minggu Penentu harus setelah Minggu Pembanding.")
            else:
                for store in sorted(df_filtered[TOKO_COL].unique()):
                    with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                        products_before = set(df_filtered[(df_filtered[TOKO_COL] == store) & (df_filtered['Minggu'] == week_before)][NAMA_PRODUK_COL])
                        products_after = set(df_filtered[(df_filtered[TOKO_COL] == store) & (df_filtered['Minggu'] == week_after)][NAMA_PRODUK_COL])
                        new_products = products_after - products_before
                        if not new_products: st.write("Tidak ada produk baru yang terdeteksi.")
                        else:
                            st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                            new_products_df = df_filtered[df_filtered[NAMA_PRODUK_COL].isin(new_products) & (df_filtered[TOKO_COL] == store) & (df_filtered['Minggu'] == week_after)]
                            new_products_df[HARGA_COL] = new_products_df[HARGA_COL].apply(format_harga_aman)
                            st.dataframe(new_products_df[[NAMA_PRODUK_COL, HARGA_COL, STATUS_COL, TERJUAL_COL]], use_container_width=True, hide_index=True)

elif page == "Produk Belum Ternamai":
    st.header("Ruang Kontrol: Latih Sistem Anda")
    gsheets_service = get_google_apis()[1]
    unknown_df = st.session_state.master_df[st.session_state.master_df[BRAND_COL] == 'TIDAK DIKETAHUI']

    if unknown_df.empty:
        st.success("🎉 Hebat! Semua produk sudah berhasil dikenali oleh sistem.")
    else:
        st.warning(f"Ditemukan **{len(unknown_df)} produk** yang brand-nya tidak dikenali.")
        
        product_to_review = unknown_df.iloc[0]
        st.write("Produk yang perlu direview:")
        st.info(f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review[TOKO_COL]})")

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
                            if alias_input: 
                                update_google_sheet(gsheets_service, SPREADSHEET_ID, KAMUS_SHEET_NAME, [alias_input.strip().upper(), new_brand_upper])
                            correction_made = True
                    else: st.warning(f"Brand '{new_brand_upper}' sudah ada.")
                elif selected_brand and not alias_input: st.error("Untuk memilih brand yang sudah ada, Anda harus mengisi kolom ALIAS/TYPO.")
                else: st.error("Mohon isi form dengan benar.")

                if correction_made:
                    st.cache_data.clear()
                    st.cache_resource.clear()
                    st.rerun()
