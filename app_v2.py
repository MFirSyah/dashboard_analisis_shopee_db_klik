# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI GABUNGAN (FINAL)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Prinsip: Mesin Pengambilan Data V2 (Otomatis) + Tampilan & Analisis V1 (Teruji)
# ===================================================================================

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import io
from thefuzz import process, fuzz
import plotly.express as px
import re

# --- KONFIGURASI HALAMAN ---
st.set_page_config(layout="wide", page_title="Dashboard Analisis Gabungan")

# --- KONFIGURASI ID & NAMA KOLOM (DARI V2) ---
# Ganti dengan ID folder utama di Google Drive Anda
PARENT_FOLDER_ID = "1z0Ex2Mjw0pCWt6BwdV1OhGLB8TJ9EPWq" 
# Ganti dengan ID Google Sheet yang berisi database brand & kamus
SPREADSHEET_ID = "1iX-LpYJrHRqD5-c2-D27kVY7PArYLaSCCd-nvd2y6Yg" 

DB_SHEET_NAME = "database_brand"
KAMUS_SHEET_NAME = "kamus_brand"
# Sheet untuk pemetaan kategori, namanya disamakan dengan di app.py
KATEGORI_SHEET_NAME = "DATABASE" 

# --- FUNGSI-FUNGSI INTI PENGAMBILAN DATA (DARI V2) ---

@st.cache_resource(show_spinner="Menghubungkan ke Google API...")
def get_google_apis():
    """Melakukan autentikasi sekali dan mengembalikan service object untuk Drive dan Sheets."""
    try:
        # Menggunakan format st.secrets yang lebih modern dari V2
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        drive_service = build('drive', 'v3', credentials=creds)
        gsheets_service = gspread.authorize(creds)
        return drive_service, gsheets_service
    except Exception as e:
        st.error(f"Gagal melakukan autentikasi ke Google. Pastikan `secrets.toml` sudah benar dan berisi key 'gcp_service_account'. Error: {e}")
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

        try:
            kategori_sheet = spreadsheet.worksheet(KATEGORI_SHEET_NAME)
            db_kategori_df = pd.DataFrame(kategori_sheet.get_all_records())
            # Pastikan nama kolom konsisten (UPPERCASE)
            db_kategori_df.columns = [str(col).strip().upper() for col in db_kategori_df.columns]
        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Sheet '{KATEGORI_SHEET_NAME}' tidak ditemukan. Fitur analisis kategori tidak akan aktif.")
            db_kategori_df = pd.DataFrame()

        return brand_db_list, kamus_dict, db_kategori_df
    except Exception as e:
        st.error(f"Gagal memuat data intelijen dari Google Sheet. Error: {e}")
        st.stop()

@st.cache_data(show_spinner="Membaca semua data dari folder kompetitor...", ttl=300)
def get_all_competitor_data(_drive_service, parent_folder_id):
    """Membaca semua file CSV & Google Sheets dari semua subfolder di dalam folder induk."""
    all_data = []
    problematic_files = []
    try:
        query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        results = _drive_service.files().list(q=query, fields="files(id, name)").execute()
        subfolders = results.get('files', [])

        if not subfolders:
            st.warning("Tidak ada subfolder (folder per toko) yang ditemukan di dalam folder induk.")
            return pd.DataFrame(), []

        progress_bar = st.progress(0, text="Membaca data...")
        for i, folder in enumerate(subfolders):
            progress_bar.progress((i + 1) / len(subfolders), text=f"Membaca folder: {folder['name']}")
            
            file_query = f"'{folder['id']}' in parents and (mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet')"
            file_results = _drive_service.files().list(q=file_query, fields="files(id, name, mimeType)").execute()
            files_in_folder = file_results.get('files', [])

            for file_item in files_in_folder:
                try:
                    file_id, file_name, mime_type = file_item.get('id'), file_item.get('name'), file_item.get('mimeType')
                    
                    request = _drive_service.files().export_media(fileId=file_id, mimeType='text/csv') if mime_type == 'application/vnd.google-apps.spreadsheet' else _drive_service.files().get_media(fileId=file_id)
                    downloader = io.BytesIO(request.execute())
                    
                    if downloader.getbuffer().nbytes == 0:
                        problematic_files.append(f"{folder['name']}/{file_name} (File Kosong)"); continue

                    df_file = pd.read_csv(downloader)
                    
                    if "Nama Produk" not in df_file.columns:
                        problematic_files.append(f"{folder['name']}/{file_name} (Kolom 'Nama Produk' tidak ditemukan)"); continue

                    df_file['Toko'] = folder['name']
                    match_tanggal = re.search(r'(\d{4}-\d{2}-\d{2})', file_name)
                    df_file['Tanggal'] = pd.to_datetime(match_tanggal.group(1)) if match_tanggal else pd.NaT
                    df_file['Status'] = 'Tersedia' if 'ready' in file_name.lower() else 'Habis'
                    all_data.append(df_file)

                except Exception as file_error:
                    problematic_files.append(f"{folder['name']}/{file_name} (Error: {file_error})")
        
        progress_bar.empty()
        if not all_data: return pd.DataFrame(), problematic_files
        
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df, problematic_files
        
    except Exception as e:
        st.error(f"Terjadi kesalahan fatal saat mengambil data dari Drive: {e}")
        return pd.DataFrame(), []

def label_brands(df, brand_db, kamus_brand, fuzzy_threshold=88):
    """Memberi label brand pada setiap produk berdasarkan database dan kamus."""
    brands = []
    for _, row in df.iterrows():
        product_name = str(row["Nama Produk"]).upper()
        found_brand = None
        
        # Prioritas 1: Kamus Alias
        for alias, brand_utama in kamus_brand.items():
            if re.search(r'\b' + re.escape(str(alias).upper()) + r'\b', product_name):
                found_brand = brand_utama; break
        if found_brand: brands.append(found_brand); continue
        
        # Prioritas 2: Database Brand (Pencocokan Tepat)
        for brand in sorted(brand_db, key=len, reverse=True):
            if re.search(r'\b' + re.escape(brand.upper()) + r'\b', product_name):
                found_brand = brand; break
        if found_brand: brands.append(found_brand); continue
        
        # Prioritas 3: Pencocokan Fuzzy
        best_match = process.extractOne(product_name, brand_db, scorer=fuzz.token_set_ratio)
        if best_match and best_match[1] > fuzzy_threshold:
            found_brand = best_match[0]
            
        brands.append(found_brand if found_brand else "TIDAK DIKETAHUI")
        
    df['Brand'] = brands
    return df

# --- FUNGSI-FUNGSI BANTU & PERHITUNGAN (DARI V1) ---

def get_smart_matches(query_product_info, competitor_df, score_cutoff=90):
    """Mencari produk yang cocok di kompetitor menggunakan fuzzy matching."""
    query_name = query_product_info['Nama Produk']
    competitor_product_list = competitor_df['Nama Produk'].tolist()
    # Menggunakan scorer yang lebih baik untuk nama produk
    candidates = process.extract(query_name, competitor_product_list, limit=20, scorer=fuzz.token_set_ratio)
    # Mengambil 5 kandidat terbaik yang lolos score cutoff
    return [match for match in candidates if match[1] >= score_cutoff][:5]

def format_wow_growth(pct_change):
    """Memberi format pada pertumbuhan mingguan dengan ikon panah."""
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"
    
def format_rupiah(x):
    """Memberi format angka menjadi string Rupiah."""
    if pd.isnull(x): return "N/A"
    try: return f"Rp {float(x):,.0f}"
    except (ValueError, TypeError): return str(x)

# --- FUNGSI MASTER UNTUK MEMUAT & MEMPROSES SEMUA DATA ---

@st.cache_data(show_spinner=False)
def load_and_process_master_data():
    """Satu fungsi untuk menjalankan seluruh proses ETL (Extract, Transform, Load)."""
    with st.spinner("Menggabungkan semua data untuk Anda... Ini mungkin butuh waktu sejenak."):
        drive_service, gsheets_service = get_google_apis()
        brand_db, kamus_brand, db_kategori = load_intelligence_data(gsheets_service, SPREADSHEET_ID)
        raw_df, problematic_files = get_all_competitor_data(drive_service, PARENT_FOLDER_ID)
        
        if raw_df is None or raw_df.empty:
            return None, None, problematic_files
            
        # 1. Memberi Label Brand
        processed_df = label_brands(raw_df.copy(), brand_db, kamus_brand)
        
        # 2. Membersihkan dan Menyesuaikan Kolom (PENTING!)
        # Menyesuaikan nama kolom dari V2 ke V1
        rename_mapping = {
            'Terjual per bulan': 'Terjual per Bulan',
            'Stok': 'Stok' # Pastikan kolom stok ada
        }
        processed_df.rename(columns=rename_mapping, inplace=True)

        # Membersihkan kolom numerik
        for col in ['Harga', 'Terjual per Bulan']:
            if col in processed_df.columns:
                processed_df[col] = processed_df[col].astype(str).str.replace(r'[^\d]', '', regex=True)
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0)
        
        # Jika kolom Stok tidak ada di semua file, buat kolom kosong
        if 'Stok' not in processed_df.columns:
            processed_df['Stok'] = 'N/A'

        # 3. Menghapus data yang tidak valid
        processed_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga'], inplace=True)

        # 4. Konversi Tipe Data & Perhitungan
        for col in ['Harga', 'Terjual per Bulan']:
            processed_df[col] = processed_df[col].astype(int)
        processed_df['Omzet'] = processed_df['Harga'] * processed_df['Terjual per Bulan']
        
        # 5. Menghapus Duplikat
        cols_for_dedup = ['Nama Produk', 'Toko', 'Tanggal']
        processed_df.drop_duplicates(subset=cols_for_dedup, inplace=True, keep='last')

    st.success("Data berhasil dimuat dan diproses!")
    return processed_df.sort_values('Tanggal'), db_kategori, problematic_files

# --- INTERFACE DASHBOARD UTAMA (DARI V1) ---
st.title("📊 Dashboard Analisis Penjualan & Kompetitor (Versi Final)")

# Memanggil fungsi master untuk memuat data
df, db_df, problematic_files = load_and_process_master_data()

# Menampilkan file yang bermasalah jika ada
if problematic_files:
    with st.expander("⚠️ Beberapa file gagal diproses dan dilewati. Klik untuk melihat detail."):
        for file_info in problematic_files:
            st.code(file_info)

if df is None or df.empty:
    st.error("Gagal memuat data utama atau tidak ada data valid yang ditemukan. Periksa folder Google Drive dan file di dalamnya.")
    st.stop()

# --- Sidebar Filter & Pengaturan (dari V1) ---
st.sidebar.header("Filter & Pengaturan")
all_stores = sorted(df['Toko'].unique())
# Menentukan toko utama, defaultnya adalah 'DB KLIK' jika ada
my_store_name_default = "DB KLIK" if "DB KLIK" in all_stores else all_stores[0]
main_store = st.sidebar.selectbox("Pilih Toko Utama:", all_stores, index=all_stores.index(my_store_name_default))

min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
if len(selected_date_range) != 2:
    st.warning("Silakan pilih rentang tanggal yang valid."); st.stop()
start_date, end_date = selected_date_range

accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan Produk (%)", 80, 100, 91, 1)

# Filter data utama berdasarkan input sidebar
df_filtered = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)].copy()
if df_filtered.empty:
    st.error("Tidak ada data pada rentang tanggal yang dipilih."); st.stop()

df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
main_store_df = df_filtered[df_filtered['Toko'] == main_store].copy()
competitor_df = df_filtered[df_filtered['Toko'] != main_store].copy()

# --- Tampilan Tab-Tab Analisis (dari V1) ---
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([f"⭐ Analisis Toko Saya ({main_store})", "⚖️ Perbandingan Harga", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])

with tab1:
    st.header(f"Analisis Kinerja Toko: {main_store}")
    
    st.subheader("1. Kategori Produk Terlaris")
    if not db_df.empty and 'KATEGORI' in db_df.columns and 'NAMA' in db_df.columns:
        @st.cache_data
        def fuzzy_merge_categories(_rekap_df, _database_df):
            _rekap_df['Kategori'] = 'Lainnya'
            db_unique = _database_df.drop_duplicates(subset=['NAMA'])
            db_map = db_unique.set_index('NAMA')['KATEGORI']
            for index, row in _rekap_df.iterrows():
                if pd.notna(row['Nama Produk']):
                    match, score = process.extractOne(row['Nama Produk'], db_map.index, scorer=fuzz.token_set_ratio)
                    if score >= 95:
                        _rekap_df.loc[index, 'Kategori'] = db_map[match]
            return _rekap_df
        
        main_store_df_cat = fuzzy_merge_categories(main_store_df.copy(), db_df)
        category_sales = main_store_df_cat.groupby('Kategori')['Terjual per Bulan'].sum().reset_index()
        
        col1, col2 = st.columns([1,2])
        sort_order_cat = col1.radio("Urutkan:", ["Terlaris", "Kurang Laris"], horizontal=True, key="cat_sort")
        top_n_cat = col2.number_input("Tampilkan Top:", 1, len(category_sales), 10, key="cat_top_n")
        
        cat_sales_sorted = category_sales.sort_values('Terjual per Bulan', ascending=(sort_order_cat == "Kurang Laris")).head(top_n_cat)
        fig_cat = px.bar(cat_sales_sorted, x='Kategori', y='Terjual per Bulan', title=f'Top {top_n_cat} Kategori', text_auto=True)
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.info(f"Analisis Kategori tidak tersedia. Pastikan Sheet '{KATEGORI_SHEET_NAME}' ada di Google Sheet dan formatnya benar.")

    st.subheader("2. Produk Terlaris")
    top_products = main_store_df.sort_values('Terjual per Bulan', ascending=False).head(15)[['Nama Produk', 'Terjual per Bulan', 'Omzet']]
    top_products['Omzet'] = top_products['Omzet'].apply(format_rupiah)
    st.dataframe(top_products, use_container_width=True, hide_index=True)

    st.subheader("3. Distribusi Omzet Brand (Top 6)")
    brand_omzet_main = main_store_df.groupby('Brand')['Omzet'].sum().nlargest(6).reset_index()
    fig_brand_pie = px.pie(brand_omzet_main, names='Brand', values='Omzet', title='Top 6 Brand Terlaris berdasarkan Omzet')
    fig_brand_pie.update_traces(texttemplate='%{label}<br>%{percent}<br>%{value:,.0f}')
    st.plotly_chart(fig_brand_pie, use_container_width=True)

with tab2:
    st.header(f"Perbandingan Produk '{main_store}' dengan Kompetitor")
    
    st.subheader("1. Ringkasan Kinerja Mingguan (WoW Growth)")
    weekly_summary = main_store_df.groupby('Minggu').agg(Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual per Bulan', 'sum')).reset_index()
    weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change().apply(format_wow_growth)
    weekly_summary['Omzet'] = weekly_summary['Omzet'].apply(format_rupiah)
    st.dataframe(weekly_summary, use_container_width=True, hide_index=True)

    st.subheader("2. Detail Produk di Toko Anda (Data Terbaru)")
    if not main_store_df.empty:
        latest_date = main_store_df['Tanggal'].max()
        main_store_latest = main_store_df[main_store_df['Tanggal'] == latest_date].copy()
        
        main_store_latest['Harga'] = main_store_latest['Harga'].apply(format_rupiah)
        cols_to_show = ['Tanggal', 'Nama Produk', 'Harga', 'Status', 'Stok']
        st.dataframe(main_store_latest[cols_to_show], use_container_width=True, hide_index=True)
        
        st.subheader("3. Pilih Produk untuk Dibandingkan")
        search_query = st.text_input("Cari produk berdasarkan nama, brand, atau kata kunci:", key="search_product")
        product_list = sorted(main_store_latest['Nama Produk'].unique())
        if search_query:
            product_list = [p for p in product_list if search_query.lower() in p.lower()]

        if not product_list:
            st.warning("Tidak ada produk yang cocok dengan pencarian Anda.")
        else:
            selected_product = st.selectbox("Pilih produk dari hasil pencarian:", product_list)
            if selected_product:
                product_info = main_store_latest[main_store_latest['Nama Produk'] == selected_product].iloc[0]
                
                st.markdown(f"**Produk Pilihan Anda:** *{product_info['Nama Produk']}*")
                col1, col2, col3 = st.columns(3)
                col1.metric(f"Harga di {main_store}", product_info['Harga'])
                col2.metric(f"Status", product_info['Status'])
                col3.metric(f"Stok", product_info['Stok'])
                
                st.markdown("---")
                st.markdown(f"**Perbandingan di Toko Kompetitor:**")
                competitor_latest = competitor_df[competitor_df['Tanggal'] == latest_date]
                if not competitor_latest.empty:
                    matches = get_smart_matches(product_info, competitor_latest, score_cutoff=accuracy_cutoff)
                    if not matches:
                        st.warning("Tidak ditemukan produk yang sangat mirip di toko kompetitor.")
                    else:
                        for product, score in matches:
                            match_info = competitor_latest[competitor_latest['Nama Produk'] == product].iloc[0]
                            # Mengambil nilai numerik dari harga untuk kalkulasi
                            price_info_num = int(re.sub(r'[^\d]', '', str(product_info['Harga'])))
                            price_diff = match_info['Harga'] - price_info_num
                            
                            st.markdown(f"**Toko: {match_info['Toko']}** (Kemiripan: {int(score)}%)")
                            st.markdown(f"*{match_info['Nama Produk']}*")
                            c1, c2, c3 = st.columns(3)
                            c1.metric("Harga Kompetitor", format_rupiah(match_info['Harga']), delta=f"Rp {price_diff:,.0f}")
                            c2.metric("Status", match_info['Status'])
                            c3.metric("Stok", match_info['Stok'])

with tab3:
    st.header("Analisis Brand di Toko Kompetitor")
    if competitor_df.empty:
        st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
    else:
        competitor_list = sorted(competitor_df['Toko'].unique())
        for competitor_store in competitor_list:
            with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                single_competitor_df = competitor_df[competitor_df['Toko'] == competitor_store]

                brand_analysis = single_competitor_df.groupby('Brand').agg(
                    Total_Omzet=('Omzet', 'sum'),
                    Total_Unit_Terjual=('Terjual per Bulan', 'sum')
                ).reset_index().sort_values("Total_Omzet", ascending=False)
                
                col1, col2 = st.columns([3,2])
                with col1:
                    st.markdown("**Peringkat Brand**")
                    brand_analysis['Total_Omzet'] = brand_analysis['Total_Omzet'].apply(format_rupiah)
                    st.dataframe(brand_analysis[['Brand', 'Total_Unit_Terjual', 'Total_Omzet']], use_container_width=True, hide_index=True)
                with col2:
                    top_6_brands_omzet = single_competitor_df.groupby('Brand')['Omzet'].sum().nlargest(6).reset_index()
                    fig_pie_comp = px.pie(top_6_brands_omzet, names='Brand', values='Omzet', title='Top 6 Brand Omzet')
                    fig_pie_comp.update_traces(textinfo='percent+label', hovertemplate='<b>%{label}</b><br>Omzet: Rp %{value:,.0f}<br>Persentase: %{percent}')
                    st.plotly_chart(fig_pie_comp, use_container_width=True)

                st.markdown("**Analisis Mendalam per Brand**")
                brand_options = sorted([str(b) for b in single_competitor_df['Brand'].dropna().unique() if b != 'TIDAK DIKETAHUI'])
                if brand_options:
                    inspect_brand = st.selectbox("Pilih Brand untuk dilihat:", brand_options, key=f"select_brand_{competitor_store}")
                    brand_detail = single_competitor_df[single_competitor_df['Brand'] == inspect_brand].sort_values("Terjual per Bulan", ascending=False)
                    brand_detail['Harga'] = brand_detail['Harga'].apply(format_rupiah)
                    brand_detail['Omzet'] = brand_detail['Omzet'].apply(format_rupiah)
                    st.dataframe(brand_detail[['Nama Produk', 'Terjual per Bulan', 'Harga', 'Omzet']], use_container_width=True, hide_index=True)

with tab4:
    st.header("Tren Status Stok Mingguan per Toko")
    stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
    # Memastikan kedua kolom status ada untuk mencegah error
    if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
    if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
    
    stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
    
    fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
    st.plotly_chart(fig_stock_trends, use_container_width=True)
    st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

with tab5:
    st.header("Analisis Kinerja Penjualan (Semua Toko)")
    
    st.subheader("1. Grafik Omzet Mingguan")
    weekly_omzet = df_filtered.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
    fig_weekly_omzet = px.line(weekly_omzet, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
    st.plotly_chart(fig_weekly_omzet, use_container_width=True)

    st.subheader("2. Tabel Ringkasan Kinerja Mingguan per Toko")
    for store in all_stores:
        with st.expander(f"Ringkasan untuk: **{store}**"):
            store_df = df_filtered[df_filtered['Toko'] == store]
            weekly_summary_store = store_df.groupby('Minggu').agg(
                total_omzet=('Omzet', 'sum'),
                total_terjual=('Terjual per Bulan', 'sum'),
                avg_harga=('Harga', 'mean')
            ).reset_index()
            
            if not weekly_summary_store.empty:
                weekly_summary_store['Pertumbuhan Omzet (WoW)'] = weekly_summary_store['total_omzet'].pct_change().apply(format_wow_growth)
                # Rumus rata-rata harian diperbaiki (dibagi 7 hari, bukan 30)
                weekly_summary_store['Rata-Rata Terjual Harian'] = (weekly_summary_store['total_terjual'] / 7).round()

                # Format Rupiah
                weekly_summary_store['total_omzet_rp'] = weekly_summary_store['total_omzet'].apply(format_rupiah)
                weekly_summary_store['avg_harga_rp'] = weekly_summary_store['avg_harga'].apply(format_rupiah)

                st.dataframe(weekly_summary_store[['Minggu', 'total_omzet_rp', 'Pertumbuhan Omzet (WoW)', 'total_terjual', 'Rata-Rata Terjual Harian', 'avg_harga_rp']].rename(
                    columns={'Minggu': 'Mulai Minggu', 'total_omzet_rp': 'Total Omzet', 'total_terjual': 'Total Terjual', 'avg_harga_rp': 'Rata-Rata Harga'}
                ), use_container_width=True, hide_index=True)
            else:
                st.info(f"Tidak ada data untuk {store} pada rentang ini.")

with tab6:
    st.header("Analisis Produk Baru Mingguan")
    
    st.subheader("Perbandingan Produk Baru Antar Minggu")
    weeks = sorted(df_filtered['Minggu'].unique())
    if len(weeks) < 2:
        st.info("Butuh setidaknya 2 minggu data untuk melakukan perbandingan produk baru.")
    else:
        col1, col2 = st.columns(2)
        week_before = col1.selectbox("Pilih Minggu Pembanding:", weeks, index=0)
        week_after = col2.selectbox("Pilih Minggu Penentu:", weeks, index=len(weeks)-1)

        if week_before >= week_after:
            st.error("Minggu Penentu harus setelah Minggu Pembanding.")
        else:
            for store in all_stores:
                with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                    products_before = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_before)]['Nama Produk'])
                    products_after = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)]['Nama Produk'])
                    new_products = products_after - products_before
                    
                    if not new_products:
                        st.write("Tidak ada produk baru yang terdeteksi.")
                    else:
                        st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                        new_products_df = df_filtered[(df_filtered['Nama Produk'].isin(new_products)) & (df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)]
                        new_products_df['Harga'] = new_products_df['Harga'].apply(format_rupiah)
                        st.dataframe(new_products_df[['Nama Produk', 'Harga', 'Stok', 'Terjual per Bulan']], use_container_width=True, hide_index=True)
