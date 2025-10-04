# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR CERDAS
#  Versi: 4.0 (TF-IDF + Integrasi Google Sheets)
# ===================================================================================

# --- Impor Pustaka/Library ---
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
from io import BytesIO
import time
import numpy as np

# --- Konfigurasi Halaman Utama ---
st.set_page_config(
    layout="wide",
    page_title="Dashboard Analisis Cerdas",
    page_icon="🧠"
)

# --- Judul Dashboard ---
st.title("🧠 Dashboard Analisis Penjualan & Kompetitor Cerdas")
st.markdown("Selamat datang, Firman! Dashboard ini akan membantu Anda menganalisis data penjualan dan memantau kompetitor secara *real-time*.")

# =====================================================================================
# BLOK FUNGSI-FUNGSI INTI
# =====================================================================================

# --- Fungsi untuk Koneksi dan Memuat Data dari Google Sheets ---
@st.cache_data(ttl=600) # Cache data selama 10 menit
def load_data_from_gsheets(status_placeholder):
    """
    Menghubungkan ke Google Sheets menggunakan Streamlit Secrets,
    memuat semua worksheet yang relevan, dan menggabungkannya menjadi DataFrame.
    """
    try:
        status_placeholder.info("🔄 Menghubungkan ke Google Sheets...")
        creds_dict = {
            "type": st.secrets["gcp_type"],
            "project_id": st.secrets["gcp_project_id"],
            "private_key_id": st.secrets["gcp_private_key_id"],
            "private_key": st.secrets["gcp_private_key_raw"].replace('\\n', '\n'),
            "client_email": st.secrets["gcp_client_email"],
            "client_id": st.secrets["gcp_client_id"],
            "auth_uri": st.secrets["gcp_auth_uri"],
            "token_uri": st.secrets["gcp_token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_client_x509_cert_url"]
        }
        creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        client = gspread.authorize(creds)
        
        spreadsheet_id = st.secrets["SOURCE_SPREADSHEET_ID"]
        status_placeholder.info(f"🔗 Berhasil terhubung! Membuka spreadsheet: {spreadsheet_id}")
        spreadsheet = client.open_by_key(spreadsheet_id)

        # Daftar worksheet yang akan diabaikan
        excluded_sheets = ["DATABASE_BRAND", "HASIL_MATCHING", "TEMPLATE"]
        
        all_data = []
        worksheets = spreadsheet.worksheets()
        
        for i, sheet in enumerate(worksheets):
            if sheet.title in excluded_sheets:
                continue

            status_placeholder.info(f"📚 Membaca worksheet ({i+1}/{len(worksheets)}): '{sheet.title}'...")
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            
            # Menentukan nama toko dari judul worksheet
            if "REKAP - READY" in sheet.title or "REKAP - HABIS" in sheet.title:
                toko = sheet.title.split(" - REKAP")[0].strip()
                status = "READY" if "READY" in sheet.title else "HABIS"
            elif "DATABASE" in sheet.title:
                toko = "DATABASE"
                status = "DATABASE"
            elif "kamus_brand" in sheet.title:
                toko = "KAMUS_BRAND"
                status = "KAMUS_BRAND"
            else:
                continue

            df['Toko'] = toko
            df['Status'] = status
            all_data.append(df)
        
        status_placeholder.success("✅ Semua data berhasil dimuat!")
        time.sleep(1)
        return all_data, spreadsheet

    except Exception as e:
        st.error(f"❌ Gagal memuat data dari Google Sheets: {e}")
        st.warning("Pastikan Anda sudah membagikan Google Sheet ke email service account: `streamlit-data-app-471714@streamlit-data-app-471714.iam.gserviceaccount.com`")
        return None, None

def preprocess_data(all_data_list):
    """Membersihkan dan mempersiapkan data untuk analisis."""
    if not all_data_list:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_kamus = next((df for df in all_data_list if df.iloc[0]['Toko'] == "KAMUS_BRAND"), pd.DataFrame())
    df_database = next((df for df in all_data_list if df.iloc[0]['Toko'] == "DATABASE"), pd.DataFrame())
    
    # Gabungkan semua data rekap (selain kamus dan database)
    df_rekap_list = [df for df in all_data_list if df.iloc[0]['Toko'] not in ["KAMUS_BRAND", "DATABASE"]]
    if not df_rekap_list:
        st.warning("Tidak ada data rekap yang ditemukan.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    master_df = pd.concat(df_rekap_list, ignore_index=True)

    # Konversi tipe data
    master_df['TANGGAL'] = pd.to_datetime(master_df['TANGGAL'], errors='coerce')
    numeric_cols = ['HARGA', 'TERJUAL/BLN']
    for col in numeric_cols:
        master_df[col] = pd.to_numeric(master_df[col], errors='coerce').fillna(0)
    
    # Buat kolom Omzet
    master_df['Omzet'] = master_df['HARGA'] * master_df['TERJUAL/BLN']

    # Normalisasi Brand
    if not df_kamus.empty and 'Alias' in df_kamus.columns and 'Brand_Utama' in df_kamus.columns:
        kamus_dict = df_kamus.set_index('Alias')['Brand_Utama'].to_dict()
        master_df['BRAND'] = master_df['BRAND'].replace(kamus_dict)

    # Hapus baris dengan tanggal kosong
    master_df.dropna(subset=['TANGGAL'], inplace=True)

    return master_df, df_database, df_kamus


# --- Fungsi untuk Pelabelan SKU & KATEGORI ---
def normalize_text(text):
    """Membersihkan dan menstandarkan nama produk untuk TF-IDF."""
    if not isinstance(text, str): return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text) # Hanya simpan huruf dan angka
    text = re.sub(r'\s+', ' ', text).strip() # Hapus spasi berlebih
    return text

def label_products(df_to_label, df_database, status_placeholder):
    """
    Melakukan pelabelan SKU dan Kategori menggunakan TF-IDF.
    """
    df_to_label['SKU'] = df_to_label.get('SKU', pd.Series(index=df_to_label.index, dtype=str))
    df_to_label['KATEGORI'] = df_to_label.get('KATEGORI', pd.Series(index=df_to_label.index, dtype=str))
    
    # Pastikan kolom ada di database
    if 'NAMA' not in df_database.columns or 'SKU' not in df_database.columns or 'KATEGORI' not in df_database.columns:
        status_placeholder.error("❌ Gagal pelabelan: Worksheet 'DATABASE' harus memiliki kolom 'NAMA', 'SKU', dan 'KATEGORI'.")
        return df_to_label, False
        
    df_database_clean = df_database.dropna(subset=['NAMA', 'SKU', 'KATEGORI'])
    
    # Normalisasi nama produk
    status_placeholder.info("🔄 Normalisasi nama produk...")
    df_to_label['normalized_name'] = df_to_label['NAMA'].apply(normalize_text)
    df_database_clean['normalized_name'] = df_database_clean['NAMA'].apply(normalize_text)
    
    # Inisialisasi TF-IDF
    status_placeholder.info("🤖 Membangun model TF-IDF...")
    vectorizer = TfidfVectorizer()
    tfidf_database = vectorizer.fit_transform(df_database_clean['normalized_name'])
    
    # Cari baris yang perlu dilabeli
    rows_to_label_idx = df_to_label[df_to_label['SKU'].isnull() | (df_to_label['SKU'] == '')].index
    total_to_label = len(rows_to_label_idx)
    
    if total_to_label == 0:
        status_placeholder.success("👍 Semua produk DB KLIK sudah memiliki label.")
        return df_to_label, False

    status_placeholder.warning(f"⚠️ Ditemukan {total_to_label} produk yang perlu dilabeli. Memulai proses...")
    progress_bar = st.progress(0)
    
    labeled_count = 0
    for idx in rows_to_label_idx:
        product_name = df_to_label.loc[idx, 'normalized_name']
        tfidf_product = vectorizer.transform([product_name])
        
        # Hitung kemiripan
        cosine_sims = cosine_similarity(tfidf_product, tfidf_database).flatten()
        
        # Dapatkan indeks dengan skor tertinggi
        best_match_idx = cosine_sims.argmax()
        
        # Ambil SKU dan Kategori dari produk yang paling mirip
        matched_sku = df_database_clean.iloc[best_match_idx]['SKU']
        matched_category = df_database_clean.iloc[best_match_idx]['KATEGORI']
        
        df_to_label.loc[idx, 'SKU'] = matched_sku
        df_to_label.loc[idx, 'KATEGORI'] = matched_category

        labeled_count += 1
        progress_bar.progress(labeled_count / total_to_label)
        status_placeholder.info(f"🔍 Melabeli produk ({labeled_count}/{total_to_label})...")

    progress_bar.empty()
    status_placeholder.success(f"✅ Pelabelan {total_to_label} produk selesai!")
    
    return df_to_label.drop(columns=['normalized_name']), True

def update_gsheet(spreadsheet, df_to_update, status_placeholder):
    """Memperbarui worksheet di Google Sheets dengan data yang sudah dilabeli."""
    try:
        sheets_to_update = {
            "DB KLIK - REKAP - READY": df_to_update[df_to_update['Status'] == 'READY'],
            "DB KLIK - REKAP - HABIS": df_to_update[df_to_update['Status'] == 'HABIS']
        }
        
        for sheet_name, df_sheet in sheets_to_update.items():
            if not df_sheet.empty:
                status_placeholder.info(f"💾 Menyimpan perubahan ke worksheet '{sheet_name}'...")
                worksheet = spreadsheet.worksheet(sheet_name)
                # Pastikan urutan kolom sesuai dengan di GSheet
                header = worksheet.row_values(1)
                df_sheet_final = df_sheet[header]
                worksheet.update([df_sheet_final.columns.values.tolist()] + df_sheet_final.values.tolist())
                status_placeholder.success(f"✔️ Berhasil menyimpan ke '{sheet_name}'!")
        return True
    except Exception as e:
        status_placeholder.error(f"❌ Gagal menyimpan perubahan ke Google Sheets: {e}")
        return False

# --- Fungsi untuk konversi ke Excel ---
@st.cache_data
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='AnalisisData')
    processed_data = output.getvalue()
    return processed_data
    
# =====================================================================================
# BLOK UTAMA APLIKASI
# =====================================================================================

# Inisialisasi session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'master_df' not in st.session_state:
    st.session_state.master_df = pd.DataFrame()

# Placeholder untuk status
status_placeholder = st.empty()

# Proses Pemuatan Data Otomatis saat pertama kali dijalankan
if not st.session_state.data_loaded:
    all_data, spreadsheet_obj = load_data_from_gsheets(status_placeholder)
    if all_data:
        master_df, df_database, df_kamus = preprocess_data(all_data)
        st.session_state.master_df = master_df
        st.session_state.df_database = df_database
        st.session_state.spreadsheet_obj = spreadsheet_obj
        
        # Cek Pelabelan
        df_db_klik = master_df[master_df['Toko'] == 'DB KLIK'].copy()
        needs_labeling = df_db_klik['SKU'].isnull().any() or ('' in df_db_klik['SKU'].values)
        
        if needs_labeling:
            st.session_state.needs_labeling = True
        else:
            st.session_state.needs_labeling = False
            st.session_state.data_loaded = True
            status_placeholder.empty() # Hapus pesan status setelah selesai
    else:
        st.stop() # Hentikan eksekusi jika data gagal dimuat

# Logika Tombol Pelabelan
if st.session_state.get('needs_labeling', False):
    status_placeholder.warning("⚠️ Terdeteksi pelabelan SKU dan KATEGORI tidak sinkron atau data tidak ditemukan. Silakan lakukan pelabelan.")
    if st.button("🚀 JALANKAN PELABELAN SKU DAN KATEGORI"):
        with st.spinner("Harap tunggu, proses pelabelan sedang berjalan..."):
            df_db_klik = st.session_state.master_df[st.session_state.master_df['Toko'] == 'DB KLIK'].copy()
            df_db_klik_labeled, success = label_products(df_db_klik, st.session_state.df_database, status_placeholder)
            
            if success:
                update_success = update_gsheet(st.session_state.spreadsheet_obj, df_db_klik_labeled, status_placeholder)
                if update_success:
                    # Gabungkan kembali data yang sudah dilabeli ke master_df
                    st.session_state.master_df.update(df_db_klik_labeled)
                    st.session_state.needs_labeling = False
                    st.session_state.data_loaded = True
                    st.success("🎉 Pelabelan dan penyimpanan berhasil! Muat ulang halaman untuk melihat analisis.")
                    st.rerun()

# Jika data sudah siap, tampilkan dashboard
if st.session_state.data_loaded:
    
    # --- SIDEBAR ---
    st.sidebar.header("⚙️ Panel Kontrol")

    # Filter Tanggal
    df = st.session_state.master_df
    min_date = df['TANGGAL'].min().date()
    max_date = df['TANGGAL'].max().date()
    
    st.sidebar.info(f"📅 Data tersedia dari **{min_date.strftime('%d %B %Y')}** hingga **{max_date.strftime('%d %B %Y')}**.")

    date_range = st.sidebar.date_input(
        "Pilih Rentang Waktu Analisis:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    
    # Filter data berdasarkan tanggal
    filtered_df = df[(df['TANGGAL'] >= start_date) & (df['TANGGAL'] <= end_date)].copy()
    
    st.sidebar.metric("Jumlah Baris Data Dianalisis", f"{len(filtered_df):,}")

    # Navigasi Analisis
    analysis_mode = st.sidebar.radio(
        "Pilih Mode Analisis:",
        ("ANALISIS UTAMA", "HPP PRODUK", "SIMILARITY PRODUK")
    )

    st.sidebar.divider()
    
    # Tombol Pelabelan Ulang
    if st.sidebar.button("🔄 Label Ulang SKU & Kategori"):
        st.session_state.data_loaded = False
        st.session_state.needs_labeling = True
        st.rerun()

    # Tombol Unduh Data
    excel_data = to_excel(filtered_df)
    st.sidebar.download_button(
        label="📥 Unduh Data (Excel)",
        data=excel_data,
        file_name=f"analisis_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.ms-excel"
    )

    # --- KONTEN UTAMA BERDASARKAN NAVIGASI ---
    
    if analysis_mode == "ANALISIS UTAMA":
        st.header("📊 Analisis Utama")
        
        tab1, tab2, tab3 = st.tabs(["Analisis DB KLIK", "Analisis Kompetitor", "Perbandingan Produk"])
        
        with tab1:
            st.subheader("📈 Kinerja Penjualan DB KLIK")
            df_db_klik = filtered_df[filtered_df['Toko'] == 'DB KLIK']

            if df_db_klik.empty:
                st.warning("Tidak ada data DB KLIK pada rentang waktu yang dipilih.")
            else:
                # --- Bar Chart Peringkat Kategori ---
                col1, col2 = st.columns([3,1])
                with col1:
                    st.markdown("**Peringkat Kategori (Berdasarkan Jumlah Produk)**")
                    category_counts = df_db_klik['KATEGORI'].value_counts().reset_index()
                    category_counts.columns = ['Kategori', 'Jumlah Produk']
                    
                    num_bars = col2.slider("Jumlah Kategori Tampil:", 5, 50, 10)
                    sort_order = col2.radio("Urutkan:", ('Tertinggi ke Terendah', 'Terendah ke Tertinggi'))
                    
                    is_desc = sort_order == 'Tertinggi ke Terendah'
                    category_counts = category_counts.sort_values('Jumlah Produk', ascending=not is_desc).head(num_bars)
                    
                    fig_cat = px.bar(category_counts, x='Jumlah Produk', y='Kategori', orientation='h', 
                                     title="Top Kategori Produk", text_auto=True)
                    fig_cat.update_layout(yaxis={'categoryorder':'total ascending' if not is_desc else 'total descending'})
                    st.plotly_chart(fig_cat, use_container_width=True)

                # --- Tabel Omzet per Kategori ---
                st.markdown("**Omzet per Kategori**")
                omzet_per_category = df_db_klik.groupby('KATEGORI')['Omzet'].sum().sort_values(ascending=False).reset_index()
                omzet_per_category['Omzet'] = omzet_per_category['Omzet'].apply(lambda x: f"Rp {x:,.0f}")
                st.dataframe(omzet_per_category, use_container_width=True)

                # --- Pie Chart Brand & Tabel Rincian ---
                st.markdown("**Kontribusi Omzet per Brand (Berdasarkan Data Terbaru)**")
                col_pie, col_table_pie = st.columns(2)
                with col_pie:
                    df_latest_date = df_db_klik[df_db_klik['TANGGAL'] == df_db_klik['TANGGAL'].max()]
                    brand_omzet_latest = df_latest_date.groupby('BRAND')['Omzet'].sum().nlargest(6)
                    fig_brand = px.pie(brand_omzet_latest, values='Omzet', names=brand_omzet_latest.index,
                                       title="Top 6 Brand dengan Omzet Tertinggi (Data Terbaru)", hole=.3)
                    fig_brand.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig_brand, use_container_width=True)
                
                with col_table_pie:
                    brand_summary = df_db_klik.groupby('BRAND').agg(
                        Total_Omzet=('Omzet', 'sum'),
                        Total_Unit_Terjual=('TERJUAL/BLN', 'sum')
                    ).sort_values('Total_Omzet', ascending=False).reset_index()
                    brand_summary['Total_Omzet'] = brand_summary['Total_Omzet'].apply(lambda x: f"Rp {x:,.0f}")
                    st.dataframe(brand_summary.head(10), use_container_width=True)

        with tab2:
            st.subheader("⚔️ Analisis Kompetitor")
            df_competitors = filtered_df[filtered_df['Toko'] != 'DB KLIK']

            # Line Chart Pendapatan Semua Toko
            st.markdown("**Perkembangan Omzet per Toko**")
            omzet_over_time = df_competitors.groupby(['TANGGAL', 'Toko'])['Omzet'].sum().reset_index()
            fig_omzet_line = px.line(omzet_over_time, x='TANGGAL', y='Omzet', color='Toko',
                                     title="Grafik Omzet Harian per Toko Kompetitor")
            st.plotly_chart(fig_omzet_line, use_container_width=True)
            
            # Pivot Table Nilai Pendapatan
            omzet_pivot = omzet_over_time.pivot(index='TANGGAL', columns='Toko', values='Omzet').fillna(0)
            omzet_pivot = omzet_pivot.applymap(lambda x: f"Rp {x:,.0f}")
            st.dataframe(omzet_pivot)

            # Line Chart Produk Ready vs Habis
            st.markdown("**Jumlah Produk Ready vs Habis**")
            status_over_time = df_competitors.groupby(['TANGGAL', 'Toko', 'Status']).size().reset_index(name='Jumlah')
            fig_status_line = px.line(status_over_time, x='TANGGAL', y='Jumlah', color='Toko', line_dash='Status',
                                      title="Grafik Jumlah Produk Ready vs Habis per Toko")
            st.plotly_chart(fig_status_line, use_container_width=True)

        with tab3:
            st.subheader("🔄 Perbandingan Produk Baru dan Habis")
            all_dates = sorted(df['TANGGAL'].dt.date.unique())
            
            col_date1, col_date2 = st.columns(2)
            compare_date = col_date1.selectbox("Pilih Tanggal Pembanding:", all_dates, index=len(all_dates)-2 if len(all_dates) > 1 else 0)
            target_date = col_date2.selectbox("Pilih Tanggal Target:", all_dates, index=len(all_dates)-1)
            
            if compare_date and target_date:
                df_compare = df[df['TANGGAL'].dt.date == compare_date][['NAMA', 'Toko']].drop_duplicates()
                df_target = df[df['TANGGAL'].dt.date == target_date][['NAMA', 'Toko']].drop_duplicates()
                
                merged_df = pd.merge(df_compare, df_target, on=['NAMA', 'Toko'], how='outer', indicator=True)
                
                new_products = merged_df[merged_df['_merge'] == 'right_only']
                gone_products = merged_df[merged_df['_merge'] == 'left_only']

                st.markdown(f"**Produk Baru pada {target_date} (Tidak ada di {compare_date})**")
                st.dataframe(new_products[['NAMA', 'Toko']], use_container_width=True)

                st.markdown(f"**Produk Hilang pada {target_date} (Ada di {compare_date})**")
                st.dataframe(gone_products[['NAMA', 'Toko']], use_container_width=True)

    elif analysis_mode == "HPP PRODUK":
        st.header("💰 Analisis Harga Pokok Penjualan (HPP) Produk DB KLIK")
        df_db_klik = df[df['Toko'] == 'DB KLIK'].copy()
        df_database = st.session_state.df_database.copy()

        # Pastikan HPP adalah numerik
        df_database['HPP (LATEST)'] = pd.to_numeric(df_database['HPP (LATEST)'], errors='coerce').fillna(0)

        # Gabungkan data berdasarkan SKU
        merged_hpp = pd.merge(df_db_klik, df_database[['SKU', 'HPP (LATEST)']], on='SKU', how='left')
        merged_hpp.dropna(subset=['HPP (LATEST)'], inplace=True)
        merged_hpp = merged_hpp[merged_hpp['HPP (LATEST)'] > 0]
        
        # Filter produk
        produk_lebih_mahal = merged_hpp[merged_hpp['HARGA'] > merged_hpp['HPP (LATEST)']]
        produk_lebih_murah = merged_hpp[merged_hpp['HARGA'] < merged_hpp['HPP (LATEST)']]

        st.markdown("**Produk dengan Harga Jual > HPP Terbaru**")
        st.dataframe(produk_lebih_mahal[['NAMA', 'SKU', 'HARGA', 'HPP (LATEST)', 'Status', 'TERJUAL/BLN']], use_container_width=True)
        
        st.markdown("**Produk dengan Harga Jual < HPP Terbaru**")
        st.dataframe(produk_lebih_murah[['NAMA', 'SKU', 'HARGA', 'HPP (LATEST)', 'Status', 'TERJUAL/BLN']], use_container_width=True)


    elif analysis_mode == "SIMILARITY PRODUK":
        st.header("🔗 Analisis Kemiripan Produk (Similarity)")
        
        df_latest_date = df[df['TANGGAL'] == max_date].copy()
        df_db_klik_latest = df_latest_date[df_latest_date['Toko'] == 'DB KLIK']
        df_competitors_latest = df_latest_date[df_latest_date['Toko'] != 'DB KLIK']

        product_list = df_db_klik_latest['NAMA'].unique()
        selected_product = st.selectbox("Pilih produk DB KLIK untuk dianalisis:", product_list)

        if selected_product:
            # Ambil detail produk yang dipilih
            product_details = df_db_klik_latest[df_db_klik_latest['NAMA'] == selected_product].iloc[0]
            product_brand = product_details['BRAND']
            st.write(f"**Produk Pilihan:** {product_details['NAMA']} | **Brand:** {product_brand} | **Harga:** Rp {product_details['HARGA']:,}")
            
            # Filter kompetitor berdasarkan brand
            competitor_filtered_by_brand = df_competitors_latest[df_competitors_latest['BRAND'] == product_brand].copy()

            if competitor_filtered_by_brand.empty:
                st.info("Tidak ditemukan produk dengan brand yang sama dari kompetitor pada data terbaru.")
            else:
                # Proses TF-IDF
                product_names_corpus = [product_details['NAMA']] + competitor_filtered_by_brand['NAMA'].tolist()
                
                vectorizer = TfidfVectorizer(preprocessor=normalize_text)
                tfidf_matrix = vectorizer.fit_transform(product_names_corpus)
                
                # Hitung cosine similarity terhadap produk pertama (produk kita)
                cosine_sims = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()
                
                # Buat DataFrame hasil
                matches_df = competitor_filtered_by_brand.copy()
                matches_df['Skor Kemiripan'] = (cosine_sims * 100).round(2)
                
                # Filter berdasarkan skor kemiripan > 70 (ambang batas)
                final_matches = matches_df[matches_df['Skor Kemiripan'] > 70].sort_values('Skor Kemiripan', ascending=False)

                if final_matches.empty:
                    st.warning("Tidak ditemukan produk kompetitor yang cukup mirip (skor > 70%).")
                else:
                    # Tampilkan metrik analisis
                    avg_price = final_matches['HARGA'].mean()
                    num_stores = final_matches['Toko'].nunique()
                    best_omzet_store = final_matches.loc[final_matches['Omzet'].idxmax()]

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Rata-rata Harga Kompetitor", f"Rp {avg_price:,.0f}")
                    col2.metric("Jumlah Toko Kompetitor", f"{num_stores} Toko")
                    col3.metric("Omzet Tertinggi dari", best_omzet_store['Toko'], f"Rp {best_omzet_store['Omzet']:,.0f}")

                    st.dataframe(final_matches[['NAMA', 'Toko', 'HARGA', 'Skor Kemiripan', 'Omzet']], use_container_width=True)
