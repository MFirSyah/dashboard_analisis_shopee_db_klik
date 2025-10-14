# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR - VERSI 6.0 (FITUR PENCARIAN)
#  Dibuat oleh: Firman & Asisten AI Gemini
#  Versi ini merombak alur kerja:
#  1. Menghapus Tab 2 (Perbandingan Harga) untuk menyederhanakan dasbor.
#  2. Memindahkan fungsi pencocokan produk (TF-IDF) ke fitur baru di sidebar.
#  3. Logika "HASIL_MATCHING" tidak lagi menjadi dependensi utama dasbor.
# ===================================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import re
import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ================================
# KONFIGURASI HALAMAN & KONSTANTA
# ================================
st.set_page_config(layout="wide", page_title="Dashboard Analisis v6.0")

SPREADSHEET_KEY = "1hl7YPEPg4aaEheN5fBKk65YX3-KdkQBRHCJWhVr9kVQ"
MY_STORE_NAME = "DB KLIK"

# ================================
# FUNGSI KONEKSI GOOGLE SHEETS
# ================================
@st.cache_resource(show_spinner="Menghubungkan ke Google Sheets...")
def connect_to_gsheets():
    creds_dict = {
        "type": st.secrets["gcp_type"], "project_id": st.secrets["gcp_project_id"],
        "private_key_id": st.secrets["gcp_private_key_id"], "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
        "client_email": st.secrets["gcp_client_email"], "client_id": st.secrets["gcp_client_id"],
        "auth_uri": st.secrets["gcp_auth_uri"], "token_uri": st.secrets["gcp_token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
    }
    gc = gspread.service_account_from_dict(creds_dict)
    return gc

def _load_all_rekap_data(spreadsheet):
    """Fungsi helper untuk membaca dan menggabungkan semua sheet REKAP."""
    sheet_objs = [s for s in spreadsheet.worksheets() if "REKAP" in s.title.upper()]
    rekap_list_df = []
    for s in sheet_objs:
        try:
            all_values = s.get_all_values()
            if not all_values or len(all_values) < 2: continue
            header, data = all_values[0], all_values[1:]
            df_sheet = pd.DataFrame(data, columns=header)
            if '' in df_sheet.columns: df_sheet = df_sheet.drop(columns=[''])
            store_name_match = re.match(r"^(.*?) - REKAP", s.title, re.IGNORECASE)
            toko_name = store_name_match.group(1).strip() if store_name_match else "Toko Tak Dikenal"
            df_sheet['Toko'] = toko_name
            if 'Status' not in df_sheet.columns:
                df_sheet['Status'] = 'Tersedia' if "READY" in s.title.upper() else 'Habis'
            rekap_list_df.append(df_sheet)
        except Exception as e:
            st.warning(f"Gagal memproses sheet rekap '{s.title}': {e}")
            continue
    if not rekap_list_df:
        return pd.DataFrame()
    return pd.concat(rekap_list_df, ignore_index=True)

# ================================
# FUNGSI MEMUAT SEMUA DATA
# ================================
@st.cache_data(show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_all_data(spreadsheet_key):
    gc = connect_to_gsheets()
    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
    except Exception as e:
        st.error(f"GAGAL KONEKSI/OPEN SPREADSHEET: {e}")
        return None, None

    database_df = pd.DataFrame()
    rekap_df = _load_all_rekap_data(spreadsheet)
    if rekap_df.empty:
        st.error("Tidak ada data REKAP yang berhasil dimuat.")
        return None, None
        
    try:
        db_worksheet = spreadsheet.worksheet("DATABASE")
        all_values = db_worksheet.get_all_values()
        if all_values and len(all_values) >= 2:
            header, data = all_values[0], all_values[1:]
            database_df = pd.DataFrame(data, columns=header)
            if '' in database_df.columns: database_df = database_df.drop(columns=[''])
    except gspread.exceptions.WorksheetNotFound:
        st.warning("Sheet 'DATABASE' tidak ditemukan.")
    except Exception as e:
        st.warning(f"Gagal membaca sheet 'DATABASE': {e}")

    rekap_df.columns = [str(c).strip().upper() for c in rekap_df.columns]
    final_rename = {
        'NAMA': 'Nama Produk', 'TERJUAL/BLN': 'Terjual per Bulan', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 
        'BRAND': 'Brand', 'STOK': 'Stok', 'TOKO': 'Toko', 'STATUS': 'Status'
    }
    rekap_df.rename(columns=final_rename, inplace=True)

    if 'Nama Produk' in rekap_df.columns: rekap_df['Nama Produk'] = rekap_df['Nama Produk'].astype(str).str.strip()
    if 'Tanggal' in rekap_df.columns: rekap_df['Tanggal'] = pd.to_datetime(rekap_df['Tanggal'], errors='coerce', dayfirst=True)
    if 'Harga' in rekap_df.columns: rekap_df['Harga'] = pd.to_numeric(rekap_df['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    if 'Terjual per Bulan' in rekap_df.columns: rekap_df['Terjual per Bulan'] = pd.to_numeric(rekap_df['Terjual per Bulan'], errors='coerce').fillna(0)

    rekap_df.dropna(subset=['Tanggal', 'Nama Produk', 'Harga', 'Toko'], inplace=True)
    if 'Brand' not in rekap_df.columns or rekap_df['Brand'].isnull().all():
        rekap_df['Brand'] = rekap_df['Nama Produk'].str.split(n=1).str[0].str.upper()
    rekap_df['Omzet'] = (rekap_df['Harga'].fillna(0) * rekap_df.get('Terjual per Bulan', 0).fillna(0)).astype(int)

    return rekap_df.sort_values('Tanggal'), database_df

# ====================================================================
# FUNGSI UNTUK FITUR BARU: PENCARIAN PRODUK (TF-IDF)
# ====================================================================
def run_product_matching(gc, spreadsheet_key, score_cutoff=0.6, batch_size=50):
    placeholder = st.empty()
    
    with placeholder.container():
        st.info(f"Memulai proses pencocokan (Batch Size: {batch_size})...")
        prog = st.progress(0, text="Mempersiapkan data...")

    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
        source_df_raw = _load_all_rekap_data(spreadsheet)
    except Exception as e:
        st.error(f"Gagal koneksi saat mengambil data: {e}"); return

    if source_df_raw is None or source_df_raw.empty:
        st.error("Gagal memuat data sumber. Batal."); return
    
    source_df_raw.columns = [str(c).strip().upper() for c in source_df_raw.columns]
    final_rename = { 'NAMA': 'Nama Produk', 'TANGGAL': 'Tanggal', 'HARGA': 'Harga', 'TOKO': 'Toko' }
    source_df_raw.rename(columns=final_rename, inplace=True)
    required_cols = ['Tanggal', 'Nama Produk', 'Toko', 'Harga']
    source_df_raw = source_df_raw[list(set(required_cols + list(source_df_raw.columns)))]
    if not all(col in source_df_raw.columns for col in required_cols):
        st.error("Data sumber tidak memiliki kolom yang dibutuhkan (Nama Produk, Tanggal, Harga, Toko)."); return

    source_df_raw['Tanggal'] = pd.to_datetime(source_df_raw['Tanggal'], errors='coerce', dayfirst=True)
    source_df_raw['Harga'] = pd.to_numeric(source_df_raw['Harga'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce')
    source_df_raw.dropna(subset=required_cols, inplace=True)
    idx = source_df_raw.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()
    source_df = source_df_raw.loc[idx].reset_index(drop=True)

    my_store_df = source_df[source_df['Toko'] == MY_STORE_NAME].reset_index(drop=True)
    competitor_df = source_df[source_df['Toko'] != MY_STORE_NAME].reset_index(drop=True)

    if my_store_df.empty or competitor_df.empty:
        st.warning("Data toko Anda atau kompetitor tidak cukup untuk dibandingkan."); return

    prog.progress(10, text="Membangun model TF-IDF...")
    
    my_products_list = my_store_df['Nama Produk'].tolist()
    competitor_products_list = competitor_df['Nama Produk'].tolist()
    
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(3, 5))
    competitor_products_matrix = vectorizer.fit_transform(competitor_products_list)

    all_matches = []
    total_products = len(my_products_list)

    for i in range(0, total_products, batch_size):
        batch_end = min(i + batch_size, total_products)
        my_products_batch = my_products_list[i:batch_end]
        
        if not my_products_batch: continue

        progress_pct = 15 + int((i / total_products) * 80)
        prog.progress(progress_pct, text=f"Memproses batch produk {i+1}-{batch_end} dari {total_products}")

        my_products_batch_matrix = vectorizer.transform(my_products_batch)
        cosine_sim_batch = cosine_similarity(my_products_batch_matrix, competitor_products_matrix)

        for batch_idx, _ in enumerate(my_products_batch):
            global_idx = i + batch_idx
            my_product_row = my_store_df.iloc[global_idx]
            sim_scores = cosine_sim_batch[batch_idx]
            matching_indices = np.where(sim_scores >= score_cutoff)[0]

            for comp_idx in matching_indices:
                score = sim_scores[comp_idx]
                competitor_product_row = competitor_df.iloc[comp_idx]
                all_matches.append({
                    'Produk Toko Saya': my_product_row['Nama Produk'],
                    'Harga Toko Saya': int(my_product_row['Harga']),
                    'Produk Kompetitor': competitor_product_row['Nama Produk'],
                    'Harga Kompetitor': int(competitor_product_row['Harga']),
                    'Toko Kompetitor': competitor_product_row['Toko'],
                    'Skor Kemiripan': round(score, 2),
                    'Tanggal_Update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
    
    if not all_matches:
        with placeholder.container():
            st.warning(f"Proses selesai, tetapi tidak ada produk yang cocok ditemukan dengan skor di atas {score_cutoff}.")
            st.info("Saran: Coba turunkan 'Tingkat Akurasi Pencocokan' dan jalankan lagi.")
        return

    prog.progress(95, text=f"Menyimpan {len(all_matches)} hasil...")
    try:
        spreadsheet = gc.open_by_key(spreadsheet_key)
        try:
            worksheet = spreadsheet.worksheet("HASIL_MATCHING")
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="HASIL_MATCHING", rows=1, cols=1)

        results_df = pd.DataFrame(all_matches).sort_values(by=['Produk Toko Saya', 'Skor Kemiripan'], ascending=[True, False])
        set_with_dataframe(worksheet, results_df, resize=True)
        with placeholder.container(): 
            st.success(f"Selesai! {len(results_df)} pasangan produk ditemukan dan disimpan ke sheet 'HASIL_MATCHING'.")
            st.balloons()
            
    except Exception as e:
        with placeholder.container(): 
            st.error(f"Gagal menyimpan hasil ke Google Sheets: {e}")
            st.info("Pastikan service account memiliki izin 'Editor' pada file spreadsheet Anda.")

# ================================
# FUNGSI-FUNGSI PEMBANTU (UTILITY)
# ================================
def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"

def style_wow_growth(val):
    color = 'black';
    if isinstance(val, str):
        if '▲' in val: color = 'green'
        elif '▼' in val: color = 'red'
    return f'color: {color}'

@st.cache_data
def convert_df_for_download(df):
    return df.to_csv(index=False).encode('utf-8')

def format_rupiah(val):
    if pd.isna(val) or not isinstance(val, (int, float, np.number)): return "N/A"
    return f"Rp {int(val):,}"

# ================================
# APLIKASI UTAMA (MAIN APP)
# ================================
st.title("📊 Dashboard Analisis Penjualan & Bisnis")

gc = connect_to_gsheets()

# --- Tombol untuk memuat data utama ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if not st.session_state.data_loaded:
    _, col_center, _ = st.columns([2, 3, 2])
    with col_center:
        if st.button("Tarik Data & Mulai Analisis 🚀", type="primary"):
            df, db_df = load_all_data(SPREADSHEET_KEY)
            if df is not None and not df.empty and db_df is not None:
                st.session_state.df, st.session_state.db_df = df, db_df
                st.session_state.data_loaded = True
                st.rerun()
            else:
                st.error("Gagal memuat data. Periksa akses Google Sheets dan pastikan sheet 'DATABASE' ada.")
    st.info("👆 Klik tombol untuk menarik semua data yang diperlukan untuk analisis.")
    st.stop()

df = st.session_state.df
db_df = st.session_state.db_df if 'db_df' in st.session_state else pd.DataFrame()

# ================================
# SIDEBAR (KONTROL UTAMA)
# ================================
st.sidebar.header("Mode Tampilan")
# --- PERUBAHAN: Menambahkan mode Pencocokan Produk ---
app_mode = st.sidebar.radio("Pilih Tampilan:", ("📈 Dasbor Analisis", "💰 Analisis HPP", "🔎 Pencocokan Produk (TF-IDF)"))
st.sidebar.divider()

if app_mode == "📈 Dasbor Analisis":
    st.sidebar.header("Filter Dasbor")
    min_date, max_date = df['Tanggal'].min().date(), df['Tanggal'].max().date()
    selected_date_range = st.sidebar.date_input("Rentang Tanggal:", [min_date, max_date], min_value=min_date, max_value=max_date)
    if len(selected_date_range) != 2: st.sidebar.warning("Pilih 2 tanggal."); st.stop()
    start_date, end_date = selected_date_range

    st.sidebar.divider()
    df_filtered_export = df[(df['Tanggal'].dt.date >= start_date) & (df['Tanggal'].dt.date <= end_date)]
    st.sidebar.header("Ekspor & Info")
    st.sidebar.info(f"Baris data dalam rentang: **{len(df_filtered_export)}**")
    csv_data = convert_df_for_download(df_filtered_export)
    st.sidebar.download_button("📥 Unduh CSV (Filter)", data=csv_data, file_name=f'analisis_{start_date}_{end_date}.csv', mime='text/csv')

elif app_mode == "🔎 Pencocokan Produk (TF-IDF)":
    st.sidebar.header("Kontrol Pencocokan")
    st.sidebar.info("Fitur ini akan membandingkan semua produk toko Anda dengan kompetitor dan menyimpan hasilnya ke sheet 'HASIL_MATCHING'.")
    accuracy_cutoff = st.sidebar.slider("Tingkat Akurasi Pencocokan", 0.0, 1.0, 0.6, 0.05)
    batch_size = st.sidebar.number_input("Ukuran Batch per Proses", 10, 200, 50, 10)
    
    if st.sidebar.button("🚀 Mulai Proses Pencocokan", type="primary"):
        run_product_matching(gc, SPREADSHEET_KEY, score_cutoff=accuracy_cutoff, batch_size=batch_size)

else: # Analisis HPP
    st.sidebar.info("Tampilan ini menganalisis harga jual produk Anda dibandingkan dengan Harga Pokok Penjualan (HPP) dari sheet 'DATABASE'.")


# =========================================================================================
# ================================ TAMPILAN KONTEN UTAMA ================================
# =========================================================================================

if app_mode == "📈 Dasbor Analisis":
    st.header("📈 Tampilan Analisis Penjualan & Kompetitor")
    # PERSIAPAN DATA UNTUK TABS
    start_date_dt, end_date_dt = pd.to_datetime(start_date), pd.to_datetime(end_date)
    df_filtered = df[(df['Tanggal'] >= start_date_dt) & (df['Tanggal'] <= end_date_dt)].copy()

    if df_filtered.empty: 
        st.error("Tidak ada data di rentang tanggal yang dipilih."); st.stop()

    df_filtered['Minggu'] = df_filtered['Tanggal'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
    main_store_df = df_filtered[df_filtered['Toko'] == MY_STORE_NAME]
    competitor_df = df_filtered[df_filtered['Toko'] != MY_STORE_NAME]
    latest_entries_overall = df_filtered.loc[df_filtered.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
    main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == MY_STORE_NAME]
    
    # --- PERUBAHAN: Urutan Tab Disesuaikan ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["⭐ Analisis Toko Saya", "🏆 Analisis Brand Kompetitor", "📦 Status Stok Produk", "📈 Kinerja Penjualan", "📊 Analisis Mingguan"])
    
    # KODE UNTUK SEMUA TAB DARI VERSI SEBELUMNYA TETAP SAMA DI SINI
    with tab1:
        st.header(f"Analisis Kinerja Toko: {MY_STORE_NAME}")
        
        section_counter = 1

        st.subheader(f"{section_counter}. Analisis Kategori Terlaris (Berdasarkan Omzet)")
        section_counter += 1
        
        if 'KATEGORI' in main_store_latest_overall.columns:
            main_store_cat = main_store_latest_overall.copy()
            main_store_cat['KATEGORI'] = main_store_cat['KATEGORI'].replace('', 'Lainnya').fillna('Lainnya')
            
            category_sales = main_store_cat.groupby('KATEGORI')['Omzet'].sum().reset_index()
            
            if not category_sales.empty:
                cat_sales_sorted = category_sales.sort_values('Omzet', ascending=False).head(10)
                fig_cat = px.bar(cat_sales_sorted, x='KATEGORI', y='Omzet', title='Top 10 Kategori Berdasarkan Omzet', text_auto='.2s')
                st.plotly_chart(fig_cat, use_container_width=True)

                st.markdown("##### Rincian Data Omzet per Kategori")
                table_cat_sales = cat_sales_sorted.copy()
                table_cat_sales['Omzet'] = table_cat_sales['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                st.dataframe(table_cat_sales, use_container_width=True, hide_index=True)

                st.markdown("---")
                st.subheader("Lihat Produk Terlaris per Kategori")
                
                category_list = category_sales.sort_values('Omzet', ascending=False)['KATEGORI'].tolist()
                
                selected_category = st.selectbox(
                    "Pilih Kategori untuk melihat produk terlaris:",
                    options=category_list
                )

                if selected_category:
                    products_in_category = main_store_cat[main_store_cat['KATEGORI'] == selected_category].copy()
                    top_products_in_category = products_in_category.sort_values('Terjual per Bulan', ascending=False)

                    if top_products_in_category.empty:
                        st.info(f"Tidak ada produk terlaris untuk kategori '{selected_category}'.")
                    else:
                        columns_to_display = ['Nama Produk', 'SKU', 'Harga', 'Terjual per Bulan', 'Omzet']
                        if 'SKU' not in top_products_in_category.columns:
                            top_products_in_category['SKU'] = 'N/A'
                        
                        display_table = top_products_in_category[columns_to_display].copy()
                        display_table['Harga'] = display_table['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                        display_table['Omzet'] = display_table['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                        
                        st.dataframe(display_table, use_container_width=True, hide_index=True)
            else:
                st.info("Tidak ada data omzet per kategori untuk ditampilkan.")
        else:
            st.warning("Kolom 'KATEGORI' tidak ditemukan pada data toko Anda. Analisis ini dilewati.")

        st.subheader(f"{section_counter}. Produk Terlaris")
        section_counter += 1
        top_products = main_store_latest_overall.sort_values('Terjual per Bulan', ascending=False).head(15).copy()
        top_products['Harga_rp'] = top_products['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
        top_products['Omzet_rp'] = top_products['Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
        
        display_cols_top = ['Nama Produk', 'SKU', 'Harga_rp', 'Omzet_rp', 'Terjual per Bulan']
        if 'SKU' not in top_products.columns:
            top_products['SKU'] = 'N/A'
        
        display_df_top = top_products[display_cols_top].rename(
            columns={'Harga_rp': 'Harga', 'Omzet_rp': 'Omzet'}
        )
        st.dataframe(display_df_top, use_container_width=True, hide_index=True)

        st.subheader(f"{section_counter}. Distribusi Omzet Brand")
        section_counter += 1
        brand_omzet_main = main_store_latest_overall.groupby('Brand')['Omzet'].sum().reset_index()
        if not brand_omzet_main.empty:
            fig_brand_pie = px.pie(brand_omzet_main.sort_values('Omzet', ascending=False).head(7), 
                                 names='Brand', values='Omzet', title='Distribusi Omzet Top 7 Brand (Snapshot Terakhir)')
            
            fig_brand_pie.update_traces(
                textposition='outside',
                texttemplate='%{label}<br><b>Rp %{value:,.0f}</b><br>(%{percent})',
                insidetextfont=dict(color='white')
            )
            fig_brand_pie.update_layout(showlegend=False)
            
            st.plotly_chart(fig_brand_pie, use_container_width=True)
        else:
            st.info("Tidak ada data omzet brand.")

        st.subheader(f"{section_counter}. Ringkasan Kinerja Mingguan (WoW Growth)")
        section_counter += 1
        main_store_latest_weekly = main_store_df.loc[main_store_df.groupby(['Minggu', 'Nama Produk'])['Tanggal'].idxmax()]
        weekly_summary_tab1 = main_store_latest_weekly.groupby('Minggu').agg(
            Omzet=('Omzet', 'sum'), Penjualan_Unit=('Terjual per Bulan', 'sum')
        ).reset_index().sort_values('Minggu')
        weekly_summary_tab1['Pertumbuhan Omzet (WoW)'] = weekly_summary_tab1['Omzet'].pct_change().apply(format_wow_growth)
        weekly_summary_tab1['Omzet'] = weekly_summary_tab1['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
        st.dataframe(
            weekly_summary_tab1[['Minggu', 'Omzet', 'Penjualan_Unit', 'Pertumbuhan Omzet (WoW)']].style.applymap(
                style_wow_growth, subset=['Pertumbuhan Omzet (WoW)']
            ), use_container_width=True, hide_index=True
        )

    with tab2:
        st.header("Analisis Brand di Toko Kompetitor")
        if competitor_df.empty:
            st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
        else:
            competitor_list = sorted(competitor_df['Toko'].unique())
            for competitor_store in competitor_list:
                with st.expander(f"Analisis untuk Kompetitor: **{competitor_store}**"):
                    single_competitor_df = competitor_latest_overall[competitor_latest_overall['Toko'] == competitor_store]
                    brand_analysis = single_competitor_df.groupby('Brand').agg(
                        Total_Omzet=('Omzet', 'sum'), 
                        Total_Unit_Terjual=('Terjual per Bulan', 'sum')
                    ).reset_index().sort_values("Total_Omzet", ascending=False)
                    
                    if not brand_analysis.empty:
                        display_brand_analysis = brand_analysis.head(10).copy()
                        display_brand_analysis['Total_Omzet'] = display_brand_analysis['Total_Omzet'].apply(lambda x: f"Rp {int(x):,.0f}")
                        st.dataframe(display_brand_analysis, use_container_width=True, hide_index=True)

                        fig_pie_comp = px.pie(brand_analysis.head(7), names='Brand', values='Total_Omzet', title=f'Distribusi Omzet Top 7 Brand di {competitor_store} (Snapshot Terakhir)')
                        st.plotly_chart(fig_pie_comp, use_container_width=True)
                    else:
                        st.info("Tidak ada data brand untuk toko ini.")

    with tab3:
        st.header("Tren Status Stok Mingguan per Toko")
        stock_trends = df_filtered.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
        if 'Tersedia' not in stock_trends.columns: stock_trends['Tersedia'] = 0
        if 'Habis' not in stock_trends.columns: stock_trends['Habis'] = 0
        stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['Tersedia', 'Habis'], var_name='Tipe Stok', value_name='Jumlah Produk')
        
        fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk Tersedia vs. Habis per Minggu')
        st.plotly_chart(fig_stock_trends, use_container_width=True)
        st.dataframe(stock_trends.set_index('Minggu'), use_container_width=True)

    with tab4:
        st.header("Analisis Kinerja Penjualan (Semua Toko)")
        
        all_stores_latest_per_week = latest_entries_weekly.groupby(['Minggu', 'Toko'])['Omzet'].sum().reset_index()
        fig_weekly_omzet = px.line(all_stores_latest_per_week, x='Minggu', y='Omzet', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko (Berdasarkan Snapshot Terakhir)')
        st.plotly_chart(fig_weekly_omzet, use_container_width=True)
        
        st.subheader("Tabel Rincian Omzet per Tanggal")
        if not df_filtered.empty:
            omzet_pivot = df_filtered.pivot_table(index='Toko', columns='Tanggal', values='Omzet', aggfunc='sum').fillna(0)
            omzet_pivot.columns = [col.strftime('%d %b %Y') for col in omzet_pivot.columns]
            for col in omzet_pivot.columns:
                omzet_pivot[col] = omzet_pivot[col].apply(lambda x: f"Rp {int(x):,}" if x > 0 else "-")
            omzet_pivot.reset_index(inplace=True)
            st.info("Anda bisa scroll tabel ini ke samping untuk melihat tanggal lainnya.")
            st.dataframe(omzet_pivot, use_container_width=True, hide_index=True)
        else:
            st.warning("Tidak ada data untuk ditampilkan dalam tabel.")

    with tab5:
        st.header("Analisis Produk Baru Mingguan")
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
                all_stores = sorted(df_filtered['Toko'].unique())
                for store in all_stores:
                    with st.expander(f"Lihat Produk Baru di Toko: **{store}**"):
                        products_before = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_before) & (df_filtered['Status'] == 'Tersedia')]['Nama Produk'])
                        products_after = set(df_filtered[(df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after) & (df_filtered['Status'] == 'Tersedia')]['Nama Produk'])
                        new_products = products_after - products_before
                        
                        if not new_products:
                            st.write("Tidak ada produk baru yang terdeteksi.")
                        else:
                            st.write(f"Ditemukan **{len(new_products)}** produk baru:")
                            new_products_df = df_filtered[df_filtered['Nama Produk'].isin(new_products) & (df_filtered['Toko'] == store) & (df_filtered['Minggu'] == week_after)].copy()
                            new_products_df['Harga_fmt'] = new_products_df['Harga'].apply(lambda x: f"Rp {int(x):,.0f}")
                            st.dataframe(new_products_df[['Nama Produk', 'Harga_fmt', 'Stok', 'Brand']].rename(columns={'Harga_fmt':'Harga'}), use_container_width=True, hide_index=True)

elif app_mode == "💰 Analisis HPP":
    st.header("💰 Tampilan Analisis Harga Pokok Penjualan (HPP)")
    latest_entries_overall = df.loc[df.groupby(['Toko', 'Nama Produk'])['Tanggal'].idxmax()]
    main_store_latest_overall = latest_entries_overall[latest_entries_overall['Toko'] == MY_STORE_NAME]

    if db_df.empty or 'SKU' not in db_df.columns:
        st.error("Sheet 'DATABASE' tidak ditemukan atau tidak memiliki kolom 'SKU'. Analisis HPP tidak dapat dilanjutkan.")
        st.stop()

    if 'HPP (LATEST)' not in db_df.columns: db_df['HPP (LATEST)'] = np.nan
    if 'HPP (AVERAGE)' not in db_df.columns: db_df['HPP (AVERAGE)'] = np.nan

    db_df['HPP_LATEST_NUM'] = pd.to_numeric(db_df['HPP (LATEST)'], errors='coerce')
    db_df['HPP_AVERAGE_NUM'] = pd.to_numeric(db_df['HPP (AVERAGE)'], errors='coerce')
    db_df['HPP'] = db_df['HPP_LATEST_NUM'].fillna(db_df['HPP_AVERAGE_NUM'])
    
    hpp_data = db_df[['SKU', 'HPP']].copy()
    hpp_data.dropna(subset=['SKU', 'HPP'], inplace=True)
    hpp_data = hpp_data[hpp_data['SKU'] != '']
    hpp_data.drop_duplicates(subset=['SKU'], keep='first', inplace=True)

    latest_db_klik = main_store_latest_overall.copy()
    merged_df = pd.merge(latest_db_klik, hpp_data, on='SKU', how='left')
    merged_df['Selisih'] = merged_df['Harga'] - merged_df['HPP']

    df_rugi = merged_df[merged_df['Selisih'] < 0].copy()
    df_untung = merged_df[(merged_df['Selisih'] >= 0)].copy()
    df_tidak_ditemukan = merged_df[merged_df['HPP'].isnull()].copy()

    st.subheader("🔴 Produk Lebih Murah dari HPP")
    if df_rugi.empty:
        st.success("👍 Mantap! Tidak ada produk yang dijual di bawah HPP.")
    else:
        display_rugi = df_rugi[['Nama Produk', 'SKU', 'Harga', 'HPP', 'Selisih', 'Terjual per Bulan', 'Omzet']].copy()
        display_rugi.rename(columns={'Terjual per Bulan': 'Terjual/Bln'}, inplace=True)
        for col in ['Harga', 'HPP', 'Selisih', 'Omzet']:
            display_rugi[col] = display_rugi[col].apply(format_rupiah)
        st.dataframe(display_rugi, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("🟢 Produk Lebih Mahal dari HPP")
    if df_untung.empty:
        st.warning("Tidak ada produk yang dijual di atas HPP.")
    else:
        display_untung = df_untung[['Nama Produk', 'SKU', 'Harga', 'HPP', 'Selisih', 'Terjual per Bulan', 'Omzet']].copy()
        display_untung.rename(columns={'Terjual per Bulan': 'Terjual/Bln'}, inplace=True)
        for col in ['Harga', 'HPP', 'Selisih', 'Omzet']:
            display_untung[col] = display_untung[col].apply(format_rupiah)
        st.dataframe(display_untung, use_container_width=True, hide_index=True)

    st.divider()
    
    st.subheader("❓ Produk Tidak Terdeteksi HPP-nya")
    if df_tidak_ditemukan.empty:
        st.success("👍 Semua produk yang dijual berhasil dicocokkan dengan data HPP di DATABASE.")
    else:
        st.warning("Mohon untuk mengecek data produk lagi, sepertinya ada data yang tidak akurat atau SKU tidak cocok.")
        display_tidak_ditemukan = df_tidak_ditemukan[['Nama Produk', 'SKU', 'Harga', 'Terjual per Bulan', 'Omzet']].copy()
        display_tidak_ditemukan.rename(columns={'Terjual per Bulan': 'Terjual/Bln'}, inplace=True)
        for col in ['Harga', 'Omzet']:
            display_tidak_ditemukan[col] = display_tidak_ditemukan[col].apply(format_rupiah)
        st.dataframe(display_tidak_ditemukan, use_container_width=True, hide_index=True)

elif app_mode == "🔎 Pencocokan Produk (TF-IDF)":
    st.header("🔎 Pencocokan Produk Menggunakan TF-IDF + N-Gram")
    st.info("Gunakan kontrol di sidebar untuk memulai proses pencocokan. Hasilnya akan disimpan di sheet 'HASIL_MATCHING' pada Google Sheets Anda.")
    st.warning("Proses ini bisa memakan waktu beberapa menit tergantung pada jumlah produk Anda dan kompetitor.")

