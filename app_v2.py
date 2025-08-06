# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 3.7
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Update: Perbaikan ValueError pada styling tabel kinerja mingguan (WoW)
# ===================================================================================

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import io
from thefuzz import process, fuzz
import re
import plotly.express as px
import time

# --- KONFIGURASI HALAMAN ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis v3.7")

# --- KONFIGURASI ID & NAMA KOLOM (SESUAIKAN DENGAN MILIK ANDA) ---
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq" # ID Folder Google Drive Induk
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg" # ID Google Sheet "Otak"
DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
KATEGORI_SHEET_NAME = "DATABASE"

# Nama Kolom Konsisten
NAMA_PRODUK_COL = "Nama Produk"
HARGA_COL = "Harga"
TERJUAL_COL = "Terjual per bulan"
STATUS_COL = "Status"
TOKO_COL = "Toko"
BRAND_COL = "BRAND"
TANGGAL_COL = "Tanggal"
OMZET_COL = "Omzet"
KATEGORI_COL = "Kategori"

# --- FUNGSI-FUNGSI INTI ---

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
        kamus_dict = pd.Series(kamus_df.Brand_Utama.values, index=kamus_df.Alias).to_dict()

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

@st.cache_data(show_spinner="Membaca semua data dari folder kompetitor...", ttl=300)
def get_all_competitor_data(_drive_service, parent_folder_id):
    """
    (V3.4 Logic) Membaca, menstandarkan, dan membersihkan data dari semua file CSV.
    """
    all_data = []
    query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
    results = _drive_service.files().list(q=query, fields="files(id, name)").execute()
    subfolders = results.get('files', [])

    if not subfolders:
        st.warning("Tidak ada subfolder (toko) yang ditemukan di dalam folder induk.")
        return pd.DataFrame()

    progress_bar = st.progress(0, text="Membaca data...")
    for i, folder in enumerate(subfolders):
        progress_text = f"Membaca folder toko: {folder['name']}..."
        progress_bar.progress((i + 1) / len(subfolders), text=progress_text)
        
        file_query = f"'{folder['id']}' in parents and mimeType='text/csv'"
        file_results = _drive_service.files().list(q=query, fields="files(id, name)").execute()
        csv_files = file_results.get('files', [])

        for csv_file in csv_files:
            file_id = csv_file.get('id')
            file_name = csv_file.get('name')
            
            try:
                request = _drive_service.files().get_media(fileId=file_id)
                downloader = io.BytesIO(request.execute())
                
                if downloader.getbuffer().nbytes == 0:
                    st.error(f"FILE KOSONG: File '{file_name}' di folder '{folder['name']}' kosong. Proses dihentikan.")
                    st.stop()

                df = pd.read_csv(downloader)
                
                rename_map = {
                    'Nama Produk': NAMA_PRODUK_COL,
                    'Harga': HARGA_COL,
                    'Terjual per Bulan': TERJUAL_COL,
                    'Link': 'Link'
                }
                df.rename(columns=lambda c: rename_map.get(c.strip(), c.strip()), inplace=True)
                
                if NAMA_PRODUK_COL not in df.columns:
                    st.error(f"KOLOM HILANG: File '{file_name}' di folder '{folder['name']}' tidak punya kolom 'Nama Produk'.")
                    st.stop()

                df[TOKO_COL] = folder['name']
                match_tanggal = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
                df[TANGGAL_COL] = pd.to_datetime(match_tanggal.group(1)) if match_tanggal else pd.NaT
                
                if 'ready' in file_name.lower(): df[STATUS_COL] = 'Tersedia'
                elif 'habis' in file_name.lower(): df[STATUS_COL] = 'Habis'
                else: df[STATUS_COL] = 'N/A'
                    
                all_data.append(df)
            except Exception as file_error:
                st.error(f"GAGAL BACA FILE: Error saat memproses '{file_name}' di folder '{folder['name']}'.")
                st.error(f"Detail Error: {file_error}")
                st.info("Proses dihentikan. Perbaiki file sebelum mencoba lagi.")
                st.stop()
    
    progress_bar.empty()
    if not all_data: return pd.DataFrame()
    
    final_df = pd.concat(all_data, ignore_index=True)
    
    if HARGA_COL in final_df.columns:
        final_df[HARGA_COL] = pd.to_numeric(final_df[HARGA_COL], errors='coerce').fillna(0)
    else:
        final_df[HARGA_COL] = 0

    if TERJUAL_COL in final_df.columns:
        cleaned_series = final_df[TERJUAL_COL].astype(str).str.lower().str.replace('rb', '000', regex=False)
        cleaned_series = cleaned_series.str.replace(r'[^\d]', '', regex=True)
        final_df[TERJUAL_COL] = pd.to_numeric(cleaned_series, errors='coerce').fillna(0)
    else:
        final_df[TERJUAL_COL] = 0
    
    final_df[HARGA_COL] = final_df[HARGA_COL].astype(float)
    final_df[TERJUAL_COL] = final_df[TERJUAL_COL].astype(float)
    
    final_df[OMZET_COL] = final_df[HARGA_COL] * final_df[TERJUAL_COL]
    
    return final_df

def label_brands(df, brand_db, kamus_brand, fuzzy_threshold=88):
    if NAMA_PRODUK_COL not in df.columns: st.stop()
    brand_db_sorted = sorted(brand_db, key=len, reverse=True)
    brands = []
    for product_name in df[NAMA_PRODUK_COL].astype(str).str.upper():
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

@st.cache_data
def map_categories(_df, _db_kategori, fuzzy_threshold=95):
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

def update_google_sheet(gsheets_service, spreadsheet_id, sheet_name, values):
    try:
        sheet = gsheets_service.open_by_key(spreadsheet_id).worksheet(sheet_name)
        sheet.append_row(values, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        st.error(f"Gagal mengupdate Google Sheet: {e}")
        return False

def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

def format_harga(x):
    if pd.isnull(x): return "N/A"
    try: return f"Rp {float(x):,.0f}"
    except (ValueError, TypeError): return str(x)

def colorize_growth(val):
    color = 'grey' # Warna default untuk N/A atau 0
    if isinstance(val, str):
        if '▲' in val: color = '#28a745' # Hijau
        elif '▼' in val: color = '#dc3545' # Merah
    return f'color: {color}'

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# --- ===== START OF STREAMLIT APP ===== ---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor v3.7")

st.sidebar.header("Kontrol Utama")
st.sidebar.info("Estimasi waktu proses: 1-3 menit tergantung jumlah file & koneksi.")
if st.sidebar.button("🚀 Tarik & Proses Data Terbaru", type="primary"):
    start_process_time = time.time()
    with st.spinner("Memulai proses... Harap tunggu."):
        drive_service, gsheets_service = get_google_apis()
        brand_db, kamus_brand, db_kategori = load_intelligence_data(gsheets_service, SPREADSHEET_ID)
        raw_df = get_all_competitor_data(drive_service, PARENT_FOLDER_ID)
        
        if raw_df is None or raw_df.empty:
            st.error("Gagal memuat data utama atau tidak ada data valid yang ditemukan.")
            st.stop()
            
        master_df = label_brands(raw_df.copy(), brand_db, kamus_brand)
        
        st.session_state.data_loaded = True
        st.session_state.master_df = master_df
        st.session_state.brand_db = brand_db
        st.session_state.kamus_brand = kamus_brand
        st.session_state.db_kategori = db_kategori
        
        end_process_time = time.time()
        duration = end_process_time - start_process_time
        st.success(f"Data berhasil ditarik dan diproses dalam {duration:.2f} detik!")
        st.rerun()

if not st.session_state.get('data_loaded', False):
    st.info("👈 Silakan klik tombol **'Tarik & Proses Data Terbaru'** di sidebar untuk memulai.")
    st.stop()

master_df = st.session_state.master_df
db_kategori = st.session_state.db_kategori
df_labeled = master_df[master_df[BRAND_COL] != 'TIDAK DIKETAHUI'].copy()

st.sidebar.header("Navigasi Halaman")
page = st.sidebar.radio("Pilih Halaman:", ["Ringkasan Eksekutif", "Analisis Mendalam", "Analisis Produk Tunggal", "Ruang Kontrol Brand"])
st.sidebar.divider()

st.sidebar.header("Filter Global")
all_stores = sorted(df_labeled[TOKO_COL].unique())
try:
    default_store_index = all_stores.index("DB KLIK")
except ValueError:
    default_store_index = 0
main_store = st.sidebar.selectbox("Pilih Toko Utama Anda:", all_stores, index=default_store_index)

df_with_dates = df_labeled.dropna(subset=[TANGGAL_COL]).copy()
min_date, max_date = df_with_dates[TANGGAL_COL].min().date(), df_with_dates[TANGGAL_COL].max().date()
selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)

accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan (%)", 80, 100, 91, 1, key="global_accuracy", help="Digunakan untuk membandingkan produk antar toko.")

st.sidebar.divider()
csv_to_download = convert_df_to_csv(master_df)
st.sidebar.download_button(
label="📥 Download Data Olahan (CSV)", data=csv_to_download,
file_name=f'data_olahan.csv', mime='text/csv',
)

if len(selected_date_range) != 2: st.stop()
start_date, end_date = selected_date_range
df_filtered = df_with_dates[(df_with_dates[TANGGAL_COL].dt.date >= start_date) & (df_with_dates[TANGGAL_COL].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()
df_filtered['Minggu'] = df_filtered[TANGGAL_COL].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
main_store_df = df_filtered[df_filtered[TOKO_COL] == main_store].copy()
competitor_df = df_filtered[df_filtered[TOKO_COL] != main_store].copy()


if page == "Ringkasan Eksekutif":
    st.header("📈 Ringkasan Eksekutif")
    st.markdown(f"Analisis untuk periode **{start_date.strftime('%d %b %Y')}** sampai **{end_date.strftime('%d %b %Y')}**")
    
    total_omzet_main = main_store_df[OMZET_COL].sum()
    total_unit_main = main_store_df[TERJUAL_COL].sum()
    total_omzet_comp = competitor_df[OMZET_COL].sum()
    total_unit_comp = competitor_df[TERJUAL_COL].sum()

    col1, col2, col3 = st.columns(3)
    col1.metric(f"Omzet Toko Anda ({main_store})", format_harga(total_omzet_main), f"{int(total_unit_main)} unit terjual")
    col2.metric("Total Omzet Kompetitor", format_harga(total_omzet_comp), f"{int(total_unit_comp)} unit terjual")
    
    if len(df_filtered['Minggu'].unique()) > 1:
        first_week_products = set(df_filtered[df_filtered['Minggu'] == df_filtered['Minggu'].min()][NAMA_PRODUK_COL])
        last_week_products = set(df_filtered[df_filtered['Minggu'] == df_filtered['Minggu'].max()][NAMA_PRODUK_COL])
        new_products_count = len(last_week_products - first_week_products)
    else:
        new_products_count = 0
    col3.metric("Produk Baru Terdeteksi", f"{new_products_count} produk")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Komposisi Omzet Pasar")
        if total_omzet_main > 0 or total_omzet_comp > 0:
            market_share_df = pd.DataFrame({'Pihak': ['Toko Anda', 'Kompetitor'], 'Omzet': [total_omzet_main, total_omzet_comp]})
            fig_pie = px.pie(market_share_df, names='Pihak', values='Omzet', hole=0.4, color_discrete_sequence=['#1f77b4', '#ff7f0e'])
            fig_pie.update_traces(textinfo='percent+label', hovertemplate='<b>%{label}</b><br>Omzet: %{value:,.0f}<br>Persentase: %{percent}')
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Tidak ada data omzet untuk ditampilkan.")
            
    with col2:
        st.subheader("Performa Omzet Mingguan")
        weekly_omzet_all = df_filtered.groupby(['Minggu', TOKO_COL])[OMZET_COL].sum().reset_index()
        fig_line = px.line(weekly_omzet_all, x='Minggu', y=OMZET_COL, color=TOKO_COL, markers=True, title="Tren Omzet Mingguan: Anda vs Kompetitor")
        st.plotly_chart(fig_line, use_container_width=True)

elif page == "Analisis Mendalam":
    st.header("🔍 Analisis Mendalam")
    tab_titles = [f"⭐ Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Brand Kompetitor", "📦 Status Stok", "📈 Kinerja Penjualan", "📊 Produk Baru"]
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_titles)

    with tab1:
        st.header(f"Analisis Kinerja Toko: {main_store}")
        st.subheader("1. Kategori Produk Terlaris")
        if main_store == "DB KLIK":
            main_store_df_cat = map_categories(main_store_df.copy(), db_kategori)
            category_sales = main_store_df_cat.groupby(KATEGORI_COL)[TERJUAL_COL].sum().reset_index()
            if not category_sales.empty:
                col1, col2 = st.columns([1,2])
                sort_order_cat = col1.radio("Urutkan:", ["Terlaris", "Kurang Laris"], horizontal=True, key="cat_sort")
                top_n_cat = col2.number_input("Tampilkan Top:", min_value=1, max_value=len(category_sales), value=min(10, len(category_sales)), key="cat_top_n")
                cat_sales_sorted = category_sales.sort_values(TERJUAL_COL, ascending=(sort_order_cat == "Kurang Laris")).head(top_n_cat)
                fig_cat = px.bar(cat_sales_sorted, x=KATEGORI_COL, y=TERJUAL_COL, title=f'Top {top_n_cat} Kategori Terlaris', text_auto=True)
                st.plotly_chart(fig_cat, use_container_width=True)
                st.markdown("---")
                st.write("**Lihat Detail Produk per Kategori**")
                categories_in_chart = cat_sales_sorted[KATEGORI_COL].tolist()
                selected_cat_details = st.selectbox("Pilih kategori untuk melihat produknya:", options=categories_in_chart)
                if selected_cat_details:
                    detail_cat_df = main_store_df_cat[main_store_df_cat[KATEGORI_COL] == selected_cat_details]
                    st.dataframe(detail_cat_df[[NAMA_PRODUK_COL, HARGA_COL, TERJUAL_COL, STATUS_COL]].style.format({HARGA_COL: format_harga}), use_container_width=True, hide_index=True)
        else:
            st.info("Analisis Kategori saat ini hanya diaktifkan untuk toko 'DB KLIK'.")

        st.subheader("2. Produk Terlaris")
        top_products = main_store_df.sort_values(TERJUAL_COL, ascending=False).head(15)[[NAMA_PRODUK_COL, TERJUAL_COL, OMZET_COL]]
        st.dataframe(top_products.style.format({OMZET_COL: format_harga}), use_container_width=True, hide_index=True)

        st.subheader("3. Distribusi Omzet Brand")
        brand_omzet_main = main_store_df.groupby(BRAND_COL)[OMZET_COL].sum().reset_index()
        top_6_brand_omzet = brand_omzet_main.nlargest(6, OMZET_COL)
        fig_brand_pie = px.pie(top_6_brand_omzet, names=BRAND_COL, values=OMZET_COL, title='Top 6 Brand Terlaris berdasarkan Omzet')
        fig_brand_pie.update_traces(texttemplate='%{label}<br>%{percent}<br>%{value:,.0f}')
        st.plotly_chart(fig_brand_pie, use_container_width=True)
        st.markdown("---")
        st.write("**Peringkat Semua Brand Berdasarkan Omzet**")
        col1b, col2b = st.columns([1,2])
        sort_order_brand = col1b.radio("Urutkan:", ["Terbesar", "Terkecil"], horizontal=True, key="brand_sort")
        top_n_brand = col2b.number_input("Tampilkan Top:", min_value=1, max_value=len(brand_omzet_main), value=min(10, len(brand_omzet_main)), key="brand_top_n")
        brand_omzet_sorted = brand_omzet_main.sort_values(OMZET_COL, ascending=(sort_order_brand == "Terkecil")).head(top_n_brand)
        fig_brand_bar = px.bar(brand_omzet_sorted, x=BRAND_COL, y=OMZET_COL, title=f"Top {top_n_brand} Brand Berdasarkan Omzet", text=brand_omzet_sorted[OMZET_COL].apply(format_harga))
        st.plotly_chart(fig_brand_bar, use_container_width=True)

    with tab2:
        st.header(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
        st.subheader("1. Ringkasan Kinerja Mingguan (WoW Growth)")
        weekly_summary = main_store_df.groupby('Minggu').agg(Omzet=(OMZET_COL, 'sum'), Penjualan_Unit=(TERJUAL_COL, 'sum')).reset_index()
        weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change()
        weekly_summary_display = weekly_summary.copy()
        weekly_summary_display['Omzet'] = weekly_summary_display['Omzet'].apply(format_harga)
        weekly_summary_display['Pertumbuhan Omzet (WoW)'] = weekly_summary_display['Pertumbuhan Omzet (WoW)'].apply(format_wow_growth)
        
        # --- PERBAIKAN V3.7: Menggunakan .applymap dengan subset ---
        st.dataframe(
            weekly_summary_display[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet (WoW)']].style.applymap(
                colorize_growth,
                subset=['Pertumbuhan Omzet (WoW)']
            ),
            use_container_width=True,
            hide_index=True
        )

        st.subheader("2. Detail Produk di Toko Anda (Data Terbaru)")
        if not main_store_df.empty:
            latest_date = main_store_df[TANGGAL_COL].max()
            main_store_latest = main_store_df[main_store_df[TANGGAL_COL] == latest_date].copy()
            st.dataframe(main_store_latest[[NAMA_PRODUK_COL, HARGA_COL, STATUS_COL]].style.format({HARGA_COL: format_harga}), use_container_width=True, hide_index=True)
            st.subheader("3. Pilih Produk untuk Dibandingkan")
            search_query = st.text_input("Cari produk berdasarkan nama, brand, atau kata kunci:", key="search_product")
            product_list = sorted(main_store_latest[NAMA_PRODUK_COL].unique())
            if search_query: product_list = [p for p in product_list if search_query.lower() in p.lower()]
            if product_list:
                selected_product = st.selectbox("Pilih produk dari hasil pencarian:", product_list)
                if selected_product:
                    product_info = main_store_latest[main_store_latest[NAMA_PRODUK_COL] == selected_product].iloc[0]
                    st.markdown(f"**Produk Pilihan Anda:** *{product_info[NAMA_PRODUK_COL]}*")
                    col1, col2 = st.columns(2)
                    col1.metric(f"Harga di {main_store}", format_harga(product_info[HARGA_COL]))
                    col2.metric(f"Status", product_info[STATUS_COL])
                    st.markdown("---"); st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                    competitor_latest = competitor_df[competitor_df[TANGGAL_COL] == latest_date]
                    if not competitor_latest.empty:
                        matches = process.extract(product_info[NAMA_PRODUK_COL], competitor_latest[NAMA_PRODUK_COL].tolist(), limit=5, scorer=fuzz.token_set_ratio)
                        valid_matches = [m for m in matches if m[1] >= accuracy_cutoff]
                        if not valid_matches: st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
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
        st.header("Analisis Brand di Toko Kompetitor")
        if competitor_df.empty: st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
        else:
            for competitor_store in sorted(competitor_df[TOKO_COL].unique()):
                with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                    single_competitor_df = competitor_df[competitor_df[TOKO_COL] == competitor_store]
                    st.markdown("**Peringkat & Visualisasi Brand**")
                    brand_analysis = single_competitor_df.groupby(BRAND_COL).agg(Total_Omzet=(OMZET_COL, 'sum'), Total_Unit_Terjual=(TERJUAL_COL, 'sum')).reset_index()
                    c1, c2 = st.columns(2)
                    with c1:
                        top_6_brands_comp = brand_analysis.nlargest(6, "Total_Omzet")
                        fig_pie_comp = px.pie(top_6_brands_comp, names=BRAND_COL, values='Total_Omzet', title='Top 6 Brand Omzet')
                        st.plotly_chart(fig_pie_comp, use_container_width=True)
                    with c2:
                        st.write("**Peringkat Brand (Bar Chart)**")
                        sort_order_brand_comp = st.radio("Urutkan:", ["Terbesar", "Terkecil"], horizontal=True, key=f"brand_sort_{competitor_store}")
                        top_n_brand_comp = st.number_input("Tampilkan Top:", 1, len(brand_analysis), min(10, len(brand_analysis)), key=f"brand_top_n_{competitor_store}")
                        brand_comp_sorted = brand_analysis.sort_values('Total_Omzet', ascending=(sort_order_brand_comp == "Terkecil")).head(top_n_brand_comp)
                        fig_bar_comp = px.bar(brand_comp_sorted, x=BRAND_COL, y='Total_Omzet', title=f"Top {top_n_brand_comp} Brand", text=brand_comp_sorted['Total_Omzet'].apply(format_harga))
                        st.plotly_chart(fig_bar_comp, use_container_width=True)
                    st.write("**Tabel Peringkat Brand**")
                    st.dataframe(brand_analysis.sort_values("Total_Omzet", ascending=False).style.format({'Total_Omzet': format_harga}), use_container_width=True, hide_index=True)
                    st.markdown("---")
                    st.write("**Lihat Detail Penjualan per Brand**")
                    brand_options = sorted(single_competitor_df[BRAND_COL].dropna().unique())
                    if brand_options:
                        inspect_brand = st.selectbox("Pilih Brand untuk dilihat:", brand_options, key=f"select_brand_{competitor_store}")
                        brand_detail_df = single_competitor_df[single_competitor_df[BRAND_COL] == inspect_brand].sort_values(OMZET_COL, ascending=False)
                        st.dataframe(brand_detail_df[[NAMA_PRODUK_COL, HARGA_COL, TERJUAL_COL, OMZET_COL]].style.format({HARGA_COL: format_harga, OMZET_COL: format_harga}), use_container_width=True, hide_index=True)

    with tab4:
        st.header("Tren Status Stok Mingguan per Toko")
        stock_trends = df_filtered.groupby(['Minggu', TOKO_COL, STATUS_COL]).size().unstack(fill_value=0).reset_index()
        if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
        if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
        stock_trends_melted = stock_trends.melt(id_vars=['Minggu', TOKO_COL], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
        fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color=TOKO_COL, line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
        st.plotly_chart(fig_stock_trends, use_container_width=True)
        st.subheader("Data Angka dari Visualisasi")
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
            if not store_df.empty:
                weekly_summary_store = store_df.groupby('Minggu').agg(Total_Omzet=(OMZET_COL, 'sum'), Total_Terjual=(TERJUAL_COL, 'sum'), Rata_Rata_Harga=(HARGA_COL, 'mean')).reset_index()
                if not weekly_summary_store.empty:
                    weekly_summary_store['Pertumbuhan Omzet (WoW)'] = weekly_summary_store['Total_Omzet'].pct_change()
                    weekly_summary_store['Toko'] = store
                    summary_list.append(weekly_summary_store)
        if summary_list:
            final_summary = pd.concat(summary_list, ignore_index=True)
            final_summary['Rata-Rata Terjual Harian'] = (final_summary['Total_Terjual'] / 7).round().astype(int)
            display_cols = {'Minggu': 'Mulai Minggu', 'Toko': 'Toko', 'Total_Omzet': 'Total Omzet', 'Pertumbuhan Omzet (WoW)': 'Pertumbuhan Omzet (WoW)', 'Total_Terjual': 'Total Terjual', 'Rata-Rata Terjual Harian': 'Rata-Rata Terjual Harian', 'Rata_Rata_Harga': 'Rata-Rata Harga'}
            final_summary_display = final_summary.rename(columns=display_cols)
            
            # --- PERBAIKAN V3.7: Menggunakan .applymap dengan subset ---
            st.dataframe(
                final_summary_display.style.format({
                    'Total Omzet': format_harga,
                    'Rata-Rata Harga': format_harga,
                    'Pertumbuhan Omzet (WoW)': format_wow_growth
                }).applymap(
                    colorize_growth,
                    subset=['Pertumbuhan Omzet (WoW)']
                ),
                use_container_width=True,
                hide_index=True
            )

    with tab6:
        st.header("Analisis Produk Baru Mingguan")
        st.subheader("Perbandingan Produk Baru Antar Minggu")
        weeks = sorted(df_filtered['Minggu'].unique())
        if len(weeks) < 2:
            st.info("Butuh setidaknya 2 minggu data untuk perbandingan.")
        else:
            col1, col2 = st.columns(2)
            week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0, key="week_before")
            week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1, key="week_after")
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
        
        st.dataframe(competitor_landscape[[TOKO_COL, NAMA_PRODUK_COL, HARGA_COL, STATUS_COL, TERJUAL_COL]].style.format({HARGA_COL: format_harga}), use_container_width=True, hide_index=True)


elif page == "Ruang Kontrol Brand":
    st.header("🧠 Ruang Kontrol: Latih Sistem Pengenalan Brand")
    gsheets_service = get_google_apis()[1]
    unknown_df = st.session_state.master_df[st.session_state.master_df[BRAND_COL] == 'TIDAK DIKETAHUI'].copy()

    if unknown_df.empty:
        st.success("🎉 Hebat! Semua produk sudah berhasil dikenali oleh sistem.")
        st.balloons()
    else:
        st.warning(f"Ditemukan **{len(unknown_df)} produk** yang brand-nya tidak dikenali oleh sistem.")
        
        product_to_review = unknown_df.iloc[0]
        st.divider()
        st.write("Produk yang perlu direview:")
        st.info(f"**{product_to_review[NAMA_PRODUK_COL]}** (dari toko: {product_to_review[TOKO_COL]})")

        with st.form(key="review_form_single"):
            st.subheader("Apa brand yang benar untuk produk ini?")
            
            col1, col2 = st.columns(2)
            
            brand_list = [""] + sorted(st.session_state.brand_db)
            selected_brand = col1.selectbox("1. Pilih dari brand yang sudah ada:", options=brand_list, help="Pilih brand yang paling sesuai dari daftar.")
            
            new_brand_input = col2.text_input("2. Atau, masukkan nama brand BARU:", help="Isi ini jika brand tidak ada di daftar sebelah.")
            
            st.divider()
            
            st.subheader("Ajari sistem tentang Alias (Nama Lain)")
            alias_input = st.text_input("Jika produk ini punya nama lain/singkatan, masukkan di sini:", 
                                        help="Contoh: Nama produk 'MI NOTEBOOK', Brand Utama 'XIAOMI'. Maka isi alias ini dengan 'MI'.")
            
            submitted = st.form_submit_button("Ajarkan ke Sistem")

            if submitted:
                final_brand = ""
                if new_brand_input:
                    final_brand = new_brand_input.strip().upper()
                elif selected_brand:
                    final_brand = selected_brand
                
                if not final_brand:
                    st.error("Anda harus memilih brand yang sudah ada atau memasukkan brand baru.")
                else:
                    correction_made = False
                    if new_brand_input and final_brand not in st.session_state.brand_db:
                        if update_google_sheet(gsheets_service, SPREADSHEET_ID, DB_SHEET_NAME, [final_brand]):
                            st.success(f"Brand baru '{final_brand}' berhasil ditambahkan ke database.")
                            st.session_state.brand_db.append(final_brand)
                            correction_made = True
                    
                    if alias_input:
                        if update_google_sheet(gsheets_service, SPREADSHEET_ID, KAMUS_SHEET_NAME, [alias_input.strip().upper(), final_brand]):
                            st.success(f"Pelajaran baru disimpan: Alias '{alias_input.upper()}' sekarang akan dikenali sebagai '{final_brand}'.")
                            correction_made = True
                    
                    if not correction_made and not new_brand_input:
                        st.warning("Tidak ada pelajaran baru yang ditambahkan. Jika hanya ingin mengklasifikasikan tanpa alias, pastikan brand dipilih.")
                    
                    if selected_brand and not alias_input:
                        st.info(f"Produk akan coba diklasifikasikan sebagai '{selected_brand}' pada penarikan data berikutnya.")
                    
                    st.info("Perubahan akan terlihat setelah Anda menarik data kembali.")
                    st.cache_data.clear()
                    st.cache_resource.clear()
                    st.rerun()
