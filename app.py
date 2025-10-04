import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import gspread
from google.oauth2.service_account import Credentials
from google.api_core.exceptions import GoogleAPIError
import io
from datetime import datetime

# ===================================================================================
# KONFIGURASI HALAMAN STREAMLIT
# ===================================================================================
st.set_page_config(
    page_title="Mesin Analisis Kompetitor",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===================================================================================
# FUNGSI-FUNGSI UTAMA
# ===================================================================================

# --- Fungsi Utilitas ---
def format_rupiah(angka):
    """Mengubah angka menjadi format Rupiah."""
    if pd.isna(angka):
        return "Rp 0"
    return f"Rp {int(angka):,.0f}".replace(',', '.')

def normalize_text(name):
    """Membersihkan dan menstandarkan nama produk untuk TF-IDF."""
    if not isinstance(name, str): return ""
    text = name.lower()
    text = re.sub(r'[^a-zA-Z0-9\s.]', ' ', text) # Hapus karakter non-alphanumeric
    text = re.sub(r'\s+', ' ', text).strip() # Hapus spasi berlebih
    return text

@st.cache_resource
def get_google_sheets_connection():
    """Membuat koneksi ke Google Sheets menggunakan st.secrets."""
    try:
        creds_dict = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
        }
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Gagal terhubung ke Google API. Pastikan file secrets.toml Anda benar. Kesalahan: {e}")
        return None

@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_data_from_gsheets(_client, spreadsheet_id):
    """Memuat dan memproses semua data dari Google Sheets."""
    status_placeholder = st.empty()
    status_placeholder.info("⏳ Menghubungkan ke Google Sheets...")
    try:
        spreadsheet = _client.open_by_key(spreadsheet_id)
        worksheets = spreadsheet.worksheets()
        
        all_dfs = {}
        required_sheets = ["DATABASE", "kamus_brand"]
        
        for sheet in required_sheets:
            status_placeholder.info(f"📚 Memuat worksheet master: {sheet}...")
            worksheet = spreadsheet.worksheet(sheet)
            data = worksheet.get_all_records()
            all_dfs[sheet] = pd.DataFrame(data)

        kamus_brand = all_dfs['kamus_brand']
        kamus_brand_dict = pd.Series(kamus_brand.Brand_Utama.values, index=kamus_brand.Alias.str.lower()).to_dict()

        rekap_sheets = [ws for ws in worksheets if "REKAP" in ws.title]
        combined_data = []

        for i, worksheet in enumerate(rekap_sheets):
            status_placeholder.info(f"📈 Memuat data toko ({i+1}/{len(rekap_sheets)}): {worksheet.title}...")
            data = worksheet.get_all_records()
            if not data: continue
            
            df = pd.DataFrame(data)
            df['TANGGAL'] = pd.to_datetime(df['TANGGAL'], errors='coerce')
            
            parts = worksheet.title.split(" - ")
            toko = parts[0]
            status = parts[-1]
            
            df['Toko'] = toko
            df['Status'] = status
            df.rename(columns={'NAMA': 'Nama Produk'}, inplace=True)
            
            df['BRAND'] = df['BRAND'].str.lower().map(kamus_brand_dict).fillna(df['BRAND'])
            
            combined_data.append(df)

        status_placeholder.success("✅ Semua data berhasil dimuat dan diproses!")
        
        if not combined_data:
            st.error("Tidak ada data rekap yang ditemukan di spreadsheet.")
            return None, None
            
        final_df = pd.concat(combined_data, ignore_index=True)
        numeric_cols = ['HARGA', 'TERJUAL/BLN']
        for col in numeric_cols:
            if col in final_df.columns:
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)
        
        final_df['Omzet'] = final_df['HARGA'] * final_df['TERJUAL/BLN']

        return final_df, all_dfs['DATABASE']

    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet dengan ID '{spreadsheet_id}' tidak ditemukan. Mohon periksa kembali ID di secrets Anda.")
        return None, None
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memuat data: {e}")
        return None, None

@st.cache_data
def perform_sku_labeling(_df_db_klik, _df_database):
    """
    Melakukan pelabelan SKU dan Kategori.
    Logika argmax() akan SELALU menemukan padanan terbaik (terdekat) berdasarkan skor TF-IDF,
    sehingga tidak akan menghasilkan nilai kosong atau 'TIDAK DITEMUKAN'.
    """
    if 'Nama Produk' not in _df_db_klik.columns or 'NAMA' not in _df_database.columns:
        st.error("Kolom nama produk tidak ditemukan untuk pelabelan.")
        return _df_db_klik

    status_label = st.info("⏳ Memulai proses pelabelan SKU & Kategori...")
    progress_bar = st.progress(0, text="Mempersiapkan data...")

    corpus = pd.concat([
        _df_database['NAMA'].apply(normalize_text),
        _df_db_klik['Nama Produk'].apply(normalize_text)
    ]).unique()

    vectorizer = TfidfVectorizer()
    vectorizer.fit(corpus)

    progress_bar.progress(0.2, text="Membangun matriks TF-IDF...")
    
    db_vectors = vectorizer.transform(_df_database['NAMA'].apply(normalize_text))
    target_vectors = vectorizer.transform(_df_db_klik['Nama Produk'].apply(normalize_text))
    
    progress_bar.progress(0.4, text="Menghitung kemiripan kosinus...")

    similarity_matrix = cosine_similarity(target_vectors, db_vectors)
    
    best_matches_indices = similarity_matrix.argmax(axis=1)
    
    new_skus = []
    new_kategoris = []
    total_rows = len(_df_db_klik)

    for i, target_idx in enumerate(range(total_rows)):
        db_idx = best_matches_indices[target_idx]
        new_skus.append(_df_database.iloc[db_idx]['SKU'])
        new_kategoris.append(_df_database.iloc[db_idx]['KATEGORI'])
        
        if (i + 1) % 100 == 0 or (i + 1) == total_rows:
            progress_percentage = 0.6 + (0.4 * ((i + 1) / total_rows))
            progress_bar.progress(progress_percentage, text=f"Melabeli produk {i+1}/{total_rows}...")

    _df_db_klik['SKU'] = new_skus
    _df_db_klik['KATEGORI'] = new_kategoris
    
    progress_bar.empty()
    status_label.success("✅ Proses pelabelan SKU & Kategori selesai!")
    
    return _df_db_klik

@st.cache_data
def precompute_similarity_matrix(_df_my_store, _df_competitor):
    """Pra-komputasi matriks kemiripan untuk optimasi kecepatan."""
    similarity_data = {}
    all_brands = _df_my_store['BRAND'].dropna().unique()

    progress_bar = st.progress(0, text="Mempersiapkan pra-komputasi kemiripan...")

    for i, brand in enumerate(all_brands):
        progress_bar.progress((i + 1) / len(all_brands), text=f"Menganalisis brand: {brand}...")

        my_store_brand = _df_my_store[_df_my_store['BRAND'] == brand]
        competitor_brand = _df_competitor[_df_competitor['BRAND'] == brand]

        if my_store_brand.empty or competitor_brand.empty:
            continue

        corpus = pd.concat([
            my_store_brand['Nama Produk'].apply(normalize_text),
            competitor_brand['Nama Produk'].apply(normalize_text)
        ])
        
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(corpus)
        
        my_store_vectors = tfidf_matrix[:len(my_store_brand)]
        competitor_vectors = tfidf_matrix[len(my_store_brand):]
        
        cosine_sim = cosine_similarity(my_store_vectors, competitor_vectors)

        similarity_data[brand] = {
            'matrix': cosine_sim,
            'my_store_indices': my_store_brand.index.tolist(),
            'competitor_indices': competitor_brand.index.tolist()
        }
    
    progress_bar.empty()
    return similarity_data

def to_excel(df_dict):
    """Mengonversi dictionary of dataframes menjadi file Excel di memori."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    processed_data = output.getvalue()
    return processed_data

# ===================================================================================
# INISIALISASI APLIKASI
# ===================================================================================
st.title("🧠 Mesin Analisis Kompetitor Cerdas")
st.markdown("Selamat datang di dasbor analisis kompetitor. Aplikasi ini memuat data secara otomatis saat pertama kali dijalankan.")

if 'labeling_needed' not in st.session_state:
    st.session_state.labeling_needed = False
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'all_data' not in st.session_state:
    st.session_state.all_data = pd.DataFrame()
if 'db_data' not in st.session_state:
    st.session_state.db_data = pd.DataFrame()
if 'similarity_data' not in st.session_state:
    st.session_state.similarity_data = None

if not st.session_state.data_loaded:
    with st.spinner("Memuat data awal... Ini mungkin memakan waktu beberapa saat."):
        client = get_google_sheets_connection()
        if client:
            all_data, db_data = load_data_from_gsheets(client, st.secrets["SOURCE_SPREADSHEET_ID"])
            if all_data is not None and db_data is not None:
                st.session_state.all_data = all_data
                st.session_state.db_data = db_data
                st.session_state.data_loaded = True
                
                df_db_klik = all_data[all_data['Toko'] == 'DB KLIK'].copy()
                if 'SKU' not in df_db_klik.columns or df_db_klik['SKU'].isnull().sum() > 0.1 * len(df_db_klik):
                     st.session_state.labeling_needed = True
                st.rerun() 
        else:
            st.error("Koneksi ke Google Sheets tidak dapat dibuat. Aplikasi tidak dapat melanjutkan.")
            st.stop()

if not st.session_state.data_loaded:
    st.warning("Data belum berhasil dimuat. Mohon periksa koneksi dan konfigurasi secrets.")
    st.stop()
    
if st.session_state.labeling_needed:
    st.warning("🚨 **PELABELAN DIPERLUKAN** 🚨\n\nTerdeteksi data SKU dan KATEGORI untuk toko **DB KLIK** tidak sinkron atau belum ada. Silakan jalankan proses pelabelan untuk melanjutkan analisis.")
    if st.button("MULAI PELABELAN SKU DAN KATEGORI", type="primary"):
        with st.spinner("Harap tunggu, proses pelabelan sedang berjalan..."):
            all_data_temp = st.session_state.all_data.copy()
            df_db_klik = all_data_temp[all_data_temp['Toko'] == 'DB KLIK'].copy()
            df_others = all_data_temp[all_data_temp['Toko'] != 'DB KLIK'].copy()
            df_db_klik_labeled = perform_sku_labeling(df_db_klik, st.session_state.db_data)
            st.session_state.all_data = pd.concat([df_db_klik_labeled, df_others], ignore_index=True)
            st.session_state.labeling_needed = False
            st.success("Pelabelan selesai!")
            st.rerun()
    st.stop()

# ===================================================================================
# SIDEBAR (Filter dan Navigasi)
# ===================================================================================
st.sidebar.header("⚙️ Filter & Opsi")
all_data = st.session_state.all_data
db_data = st.session_state.db_data

min_date = all_data['TANGGAL'].min().date()
max_date = all_data['TANGGAL'].max().date()

start_date, end_date = st.sidebar.date_input(
    'Pilih Rentang Tanggal',
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    help="Pilih rentang tanggal untuk dianalisis."
)

start_datetime = pd.to_datetime(start_date)
end_datetime = pd.to_datetime(end_date)
filtered_data = all_data[(all_data['TANGGAL'] >= start_datetime) & (all_data['TANGGAL'] <= end_datetime)].copy()

st.sidebar.info(
    f"""
    📅 Data dari: **{min_date.strftime('%d %b %Y')}**
    📅 Hingga: **{max_date.strftime('%d %b %Y')}**
    ---
    **{len(filtered_data)}** baris data dianalisis.
    """
)

if st.sidebar.button("Jalankan Ulang Pelabelan"):
    st.session_state.labeling_needed = True
    st.session_state.similarity_data = None 
    st.rerun()

excel_data = to_excel({
    'Data Gabungan Terfilter': filtered_data,
    'Database Produk': db_data
})
st.sidebar.download_button(
    label="📥 Unduh Data (Excel)",
    data=excel_data,
    file_name=f"analisis_data_{start_date}_sd_{end_date}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.sidebar.header("Pilih Analisis")
analysis_choice = st.sidebar.radio(
    "Menu Analisis:",
    ["ANALISIS UTAMA", "HPP PRODUK", "SIMILARITY PRODUK"],
    captions=["Dasbor umum & performa toko", "Bandingkan harga jual vs HPP", "Cari produk serupa antar toko"]
)

# ===================================================================================
# KONTEN UTAMA APLIKASI
# ===================================================================================
if analysis_choice == "ANALISIS UTAMA":
    st.header("📊 Analisis Utama")
    tab1, tab2, tab3 = st.tabs([
        "📈 Analisis DB KLIK",
        "🌍 Analisis Kompetitor",
        "🔄 Perbandingan Produk Baru & Habis"
    ])
    
    with tab1:
        df_db_klik_filtered = filtered_data[filtered_data['Toko'] == 'DB KLIK'].copy()
        if df_db_klik_filtered.empty:
            st.warning("Tidak ada data DB KLIK pada rentang tanggal yang dipilih.")
        else:
            st.subheader("Performance Kategori Produk DB KLIK")
            col1, col2 = st.columns([1, 2])
            with col1:
                top_n = st.slider("Jumlah Kategori Tampil", 5, 20, 10)
                sort_order = st.radio("Urutkan Berdasarkan Omzet", ["Tertinggi", "Terendah"])
                
                kategori_omzet = df_db_klik_filtered.groupby('KATEGORI')['Omzet'].sum().reset_index()
                is_desc = sort_order == "Tertinggi"
                kategori_omzet_sorted = kategori_omzet.sort_values('Omzet', ascending=not is_desc).head(top_n)

                fig_kat = px.bar(
                    kategori_omzet_sorted, 
                    x='Omzet', y='KATEGORI', orientation='h',
                    title=f'Top {top_n} Kategori dengan Omzet {sort_order}',
                    labels={'Omzet': 'Total Omzet', 'KATEGORI': 'Kategori'},
                    text_auto=True
                )
                fig_kat.update_traces(texttemplate='%{x:,.0f}', textposition='outside')
                fig_kat.update_layout(yaxis={'categoryorder':'total ascending' if is_desc else 'total descending'})
                st.plotly_chart(fig_kat, use_container_width=True)
            
            with col2:
                st.dataframe(
                    kategori_omzet.sort_values('Omzet', ascending=False).style.format({'Omzet': format_rupiah}),
                    use_container_width=True,
                    height=400
                )

            st.divider()
            
            # --- BAGIAN WOW GROWTH BARU ---
            st.subheader("Analisis Pertumbuhan Week-over-Week (WoW)")
            df_db_wow = df_db_klik_filtered[['TANGGAL', 'Omzet', 'TERJUAL/BLN']].copy()
            df_db_wow = df_db_wow.set_index('TANGGAL')
            
            # Resample data per minggu, dimulai hari Senin
            weekly_summary = df_db_wow.resample('W-MON').agg({
                'Omzet': 'sum',
                'TERJUAL/BLN': 'sum'
            }).rename(columns={'TERJUAL/BLN': 'Total Unit Terjual'})
            
            # Hitung persentase pertumbuhan
            weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change()
            weekly_summary.fillna(0, inplace=True)
            
            def style_growth(val):
                """Memberi warna pada sel berdasarkan nilai positif atau negatif."""
                if val > 0:
                    return 'color: green'
                elif val < 0:
                    return 'color: red'
                else:
                    return 'color: gray'
            
            st.dataframe(
                weekly_summary.style
                .format({
                    'Omzet': format_rupiah,
                    'Total Unit Terjual': '{:,.0f}',
                    'Pertumbuhan Omzet (WoW)': '{:,.2%}'
                })
                .apply(lambda x: x.map(style_growth), subset=['Pertumbuhan Omzet (WoW)']),
                use_container_width=True
            )
            # --- AKHIR BAGIAN WOW GROWTH BARU ---

            st.divider()
            st.subheader("Analisis Brand Teratas (Berdasarkan Omzet Terbaru)")
            latest_date_db = df_db_klik_filtered['TANGGAL'].max()
            df_latest_brand = df_db_klik_filtered[df_db_klik_filtered['TANGGAL'] == latest_date_db]
            brand_omzet = df_latest_brand.groupby('BRAND').agg(
                Total_Omzet=('Omzet', 'sum'),
                Total_Unit_Terjual=('TERJUAL/BLN', 'sum')
            ).reset_index().sort_values('Total_Omzet', ascending=False)
            col_pie, col_table = st.columns(2)
            with col_pie:
                top_6_brands = brand_omzet.head(6)
                fig_pie = px.pie(
                    top_6_brands, values='Total_Omzet', names='BRAND',
                    title='Persentase Omzet 6 Brand Teratas', hole=0.4
                )
                fig_pie.update_traces(textinfo='percent+label', pull=[0.05]*6)
                st.plotly_chart(fig_pie, use_container_width=True)
            with col_table:
                st.write("Ringkasan Data Brand")
                st.dataframe(
                    brand_omzet.style.format({'Total_Omzet': format_rupiah, 'Total_Unit_Terjual': '{:,.0f}'}),
                    use_container_width=True, height=300
                )
                
            st.divider()
            st.subheader("Produk Terlaris Global (Periode Terpilih)")
            produk_terlaris = df_db_klik_filtered.sort_values('Omzet', ascending=False).head(20)
            st.dataframe(
                produk_terlaris[['Nama Produk', 'SKU', 'HARGA', 'Omzet']].style.format({'HARGA': format_rupiah, 'Omzet': format_rupiah}),
                use_container_width=True
            )

    with tab2:
        st.subheader("Performa Pendapatan Antar Toko")
        pendapatan_toko_harian = filtered_data.groupby(['TANGGAL', 'Toko'])['Omzet'].sum().reset_index()
        fig_line_pendapatan = px.line(
            pendapatan_toko_harian, x='TANGGAL', y='Omzet', color='Toko',
            title='Tren Pendapatan Harian Semua Toko',
            labels={'TANGGAL': 'Tanggal', 'Omzet': 'Total Omzet', 'Toko': 'Nama Toko'}
        )
        st.plotly_chart(fig_line_pendapatan, use_container_width=True)
        
        st.write("Tabel Nilai Pendapatan Harian (dalam jutaan Rupiah)")
        pivot_pendapatan = pendapatan_toko_harian.pivot(index='Toko', columns='TANGGAL', values='Omzet').fillna(0)
        st.dataframe((pivot_pendapatan / 1_000_000).style.format('{:,.2f} Jt'), use_container_width=True)
        
        st.divider()
        st.subheader("Tren Jumlah Produk Ready vs Habis")
        status_produk = filtered_data.groupby(['TANGGAL', 'Toko', 'Status']).size().reset_index(name='Jumlah')
        fig_line_status = px.line(
            status_produk, x='TANGGAL', y='Jumlah', color='Toko', line_dash='Status',
            title='Jumlah Produk Ready vs Habis per Hari',
            labels={'TANGGAL': 'Tanggal', 'Jumlah': 'Jumlah Produk'}
        )
        st.plotly_chart(fig_line_status, use_container_width=True)

    with tab3:
        st.subheader("Perbandingan Snapshot Produk Antar Tanggal")
        all_dates = sorted(filtered_data['TANGGAL'].dt.date.unique())
        if all_dates:
            col_tgl1, col_tgl2 = st.columns(2)
            with col_tgl1:
                tgl_pembanding = st.selectbox("Pilih Tanggal Pembanding", all_dates, index=0)
            with col_tgl2:
                tgl_target = st.selectbox("Pilih Tanggal Target", all_dates, index=len(all_dates)-1)
            
            if st.button("Bandingkan Snapshot"):
                df_pembanding = all_data[all_data['TANGGAL'].dt.date == tgl_pembanding]
                df_target = all_data[all_data['TANGGAL'].dt.date == tgl_target]
                for toko in all_data['Toko'].unique():
                    with st.expander(f"Laporan untuk: **{toko}**"):
                        produk_pembanding = set(df_pembanding[df_pembanding['Toko'] == toko]['Nama Produk'])
                        produk_target = set(df_target[df_target['Toko'] == toko]['Nama Produk'])
                        produk_baru = produk_target - produk_pembanding
                        produk_hilang = produk_pembanding - produk_target
                        st.metric(label="Total Produk Pembanding", value=len(produk_pembanding))
                        st.metric(label="Total Produk Target", value=len(produk_target))
                        col_baru, col_hilang = st.columns(2)
                        with col_baru:
                            st.success(f"Produk Baru Ditemukan: {len(produk_baru)}")
                            if produk_baru:
                                st.dataframe(pd.DataFrame(list(produk_baru), columns=["Nama Produk"]), height=200)
                        with col_hilang:
                            st.error(f"Produk Tidak Lagi Ditemukan: {len(produk_hilang)}")
                            if produk_hilang:
                                st.dataframe(pd.DataFrame(list(produk_hilang), columns=["Nama Produk"]), height=200)

elif analysis_choice == "HPP PRODUK":
    st.header("⚖️ Analisis Harga Pokok Penjualan (HPP)")
    df_db_klik_all = all_data[all_data['Toko'] == 'DB KLIK'].copy()
    if df_db_klik_all.empty:
        st.warning("Tidak ada data DB KLIK untuk dianalisis.")
    else:
        latest_date_hpp = df_db_klik_all['TANGGAL'].max()
        df_db_klik_latest = df_db_klik_all[df_db_klik_all['TANGGAL'] == latest_date_hpp].copy()
        db_data_hpp = db_data[['SKU', 'HPP (LATEST)']].copy()
        db_data_hpp['HPP (LATEST)'] = pd.to_numeric(db_data_hpp['HPP (LATEST)'], errors='coerce').fillna(0)
        merged_hpp = pd.merge(df_db_klik_latest, db_data_hpp, on='SKU', how='left')
        merged_hpp = merged_hpp[merged_hpp['HPP (LATEST)'] > 0] 
        produk_lebih_mahal = merged_hpp[merged_hpp['HARGA'] < merged_hpp['HPP (LATEST)']]
        produk_lebih_murah = merged_hpp[merged_hpp['HARGA'] > merged_hpp['HPP (LATEST)']]
        st.subheader("Produk dengan Harga Jual di Bawah HPP Terbaru")
        st.warning(f"Ditemukan **{len(produk_lebih_mahal)}** produk yang berpotensi merugi.")
        if not produk_lebih_mahal.empty:
            st.dataframe(
                produk_lebih_mahal[['Nama Produk', 'SKU', 'HARGA', 'HPP (LATEST)', 'Status']].style.format({'HARGA': format_rupiah, 'HPP (LATEST)': format_rupiah}), 
                use_container_width=True
            )
        st.subheader("Produk dengan Harga Jual di Atas HPP Terbaru")
        st.success(f"Ditemukan **{len(produk_lebih_murah)}** produk yang menguntungkan.")
        if not produk_lebih_murah.empty:
            st.dataframe(
                produk_lebih_murah[['Nama Produk', 'SKU', 'HARGA', 'HPP (LATEST)', 'Status']].style.format({'HARGA': format_rupiah, 'HPP (LATEST)': format_rupiah}), 
                use_container_width=True
            )

elif analysis_choice == "SIMILARITY PRODUK":
    st.header("🤝 Analisis Kemiripan Produk (TF-IDF)")
    
    latest_date_sim = all_data['TANGGAL'].max()
    df_latest = all_data[all_data['TANGGAL'] == latest_date_sim].copy()
    
    df_my_store = df_latest[df_latest['Toko'] == 'DB KLIK'].copy()
    df_competitor = df_latest[df_latest['Toko'] != 'DB KLIK'].copy()

    if st.session_state.similarity_data is None:
        with st.spinner("Melakukan pra-komputasi matriks kemiripan untuk semua produk... (satu kali proses)"):
            st.session_state.similarity_data = precompute_similarity_matrix(df_my_store, df_competitor)
        st.success("Pra-komputasi selesai! Pencarian sekarang akan sangat cepat.")
    
    # --- FILTER BRAND BARU ---
    brand_list = ['Semua Brand'] + sorted(df_my_store['BRAND'].dropna().unique())
    selected_brand = st.selectbox(
        "Filter Berdasarkan Brand:",
        brand_list
    )
    
    # Filter product list based on selected brand
    if selected_brand == 'Semua Brand':
        product_list = sorted(df_my_store['Nama Produk'].unique())
    else:
        product_list = sorted(df_my_store[df_my_store['BRAND'] == selected_brand]['Nama Produk'].unique())
        
    selected_product_name = st.selectbox(
        "Pilih produk dari DB KLIK untuk dianalisis:",
        product_list,
        index=None,
        placeholder="Ketik untuk mencari produk..."
    )

    if selected_product_name:
        selected_product_info = df_my_store[df_my_store['Nama Produk'] == selected_product_name].iloc[0]
        
        st.subheader("Produk Pilihan Anda:")
        col_info1, col_info2, col_info3 = st.columns(3)
        col_info1.metric("Harga DB KLIK", format_rupiah(selected_product_info['HARGA']))
        col_info2.metric("Brand", selected_product_info['BRAND'])
        col_info3.metric("SKU", selected_product_info['SKU'])
        st.markdown(f"**Nama Lengkap:** {selected_product_name}")
        
        matches = []
        target_brand = selected_product_info['BRAND']
        sim_data_brand = st.session_state.similarity_data.get(target_brand)

        if sim_data_brand:
            try:
                my_store_product_index_local = sim_data_brand['my_store_indices'].index(selected_product_info.name)
                scores = sim_data_brand['matrix'][my_store_product_index_local]
                sorted_competitor_indices = scores.argsort()[::-1]
                
                for comp_idx_local in sorted_competitor_indices:
                    score = scores[comp_idx_local]
                    if score > 0.5:
                        original_competitor_index = sim_data_brand['competitor_indices'][comp_idx_local]
                        match_info = df_competitor.loc[original_competitor_index]
                        matches.append({
                            'Toko': match_info['Toko'],
                            'Nama Produk': match_info['Nama Produk'],
                            'Harga': match_info['HARGA'],
                            'Status': match_info['Status'],
                            'Omzet': match_info['Omzet'],
                            'Terjual per Bulan': match_info['TERJUAL/BLN'],
                            'Skor Kemiripan': score * 100
                        })
            except ValueError:
                pass

        st.divider()
        st.subheader("Hasil Perbandingan di Toko Kompetitor")

        if matches:
            df_matches = pd.DataFrame(matches)
            
            avg_price = df_matches['Harga'].mean()
            omzet_per_toko = df_matches.groupby('Toko')['Omzet'].sum()
            
            if not omzet_per_toko.empty:
                toko_omzet_tertinggi = omzet_per_toko.idxmax()
                total_omzet_toko = omzet_per_toko.max()
            else:
                toko_omzet_tertinggi = "N/A"
                total_omzet_toko = 0

            col_sum1, col_sum2, col_sum3 = st.columns(3)
            col_sum1.metric("Rata-rata Harga Kompetitor", format_rupiah(avg_price))
            col_sum2.metric("Jumlah Toko Mirip", f"{df_matches['Toko'].nunique()} Toko")
            col_sum3.metric("Toko Omzet Tertinggi", f"{toko_omzet_tertinggi}", help=f"Total omzet dari produk serupa: {format_rupiah(total_omzet_toko)}")

            # Tampilkan dataframe dengan kolom baru
            st.dataframe(
                df_matches.style.format({
                    'Harga': format_rupiah,
                    'Omzet': format_rupiah,
                    'Terjual per Bulan': '{:,.0f}',
                    'Skor Kemiripan': '{:.2f}%'
                }).background_gradient(
                    cmap='viridis', subset=['Skor Kemiripan']
                ), use_container_width=True
            )
        else:
            st.info("Tidak ditemukan produk yang serupa di toko kompetitor berdasarkan brand dan analisis nama.")

