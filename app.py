# ===================================================================================
#  DASHBOARD ANALISIS PENJUALAN & KOMPETITOR V.FINAL
#  Diadaptasi untuk struktur secrets.toml Firman
# ===================================================================================

# --- 1. Impor Pustaka/Library ---
import streamlit as st
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import plotly.express as px
import plotly.graph_objects as go
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
import io
from datetime import datetime
import numpy as np # Diperlukan untuk logika HPP

# --- 2. Konfigurasi Halaman Utama ---
st.set_page_config(
    layout="wide",
    page_title="Dashboard Analisis Cerdas",
    page_icon="🧠"
)

# --- 3. Fungsi-Fungsi Inti ---

def normalize_text(text):
    """Membersihkan dan menstandarkan nama produk untuk perbandingan TF-IDF."""
    if not isinstance(text, str): return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    stopwords = [
        'dan', 'untuk', 'dengan', 'garansi', 'resmi', 'original', 'promo',
        'murah', 'gaming', 'wireless', 'bluetooth', 'keyboard', 'mouse',
        'headset', 'speaker', 'monitor', 'laptop', 'led', 'pro', 'ultra'
    ]
    tokens = text.split()
    tokens = [word for word in tokens if word not in stopwords]
    return ' '.join(tokens)

@st.cache_data(ttl=600, show_spinner="Mengambil data terbaru dari Google Sheets...")
def load_data_from_gsheets():
    """Menghubungkan ke Google Sheets dan memuat semua data yang diperlukan secara dinamis."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        # --- PENYESUAIAN KUNCI ---
        # Mengambil ID dari kunci "ID_DATA" sesuai file secrets.toml Anda
        spreadsheet_id = st.secrets["ID_DATA"] 
        
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open_by_key(spreadsheet_id)
        
        worksheets = spreadsheet.worksheets()
        worksheet_titles = [ws.title for ws in worksheets]
        
        data_frames = {}
        required_sheets = ['DATABASE', 'DATABASE_BRAND', 'kamus_brand']
        
        for title in worksheet_titles:
            if 'REKAP - READY' in title or 'REKAP - HABIS' in title:
                required_sheets.append(title)
                
        for sheet_name in set(required_sheets):
            if sheet_name in worksheet_titles:
                worksheet = spreadsheet.worksheet(sheet_name)
                df = pd.DataFrame(worksheet.get_all_records())
                if 'TANGGAL' in df.columns:
                    df['TANGGAL'] = pd.to_datetime(df['TANGGAL'], errors='coerce')
                data_frames[sheet_name] = df
            
        return data_frames, spreadsheet
    
    except Exception as e:
        st.error(f"Gagal terhubung atau memuat data dari Google Sheets: {e}")
        return None, None

def label_products(db_klik_df, database_df):
    """Melakukan pelabelan SKU dan Kategori pada data DB KLIK menggunakan TF-IDF."""
    df_to_label = db_klik_df[db_klik_df['SKU'].replace('', pd.NA).isna()].copy()
    if df_to_label.empty: return db_klik_df, False

    st.write(f"Mendeteksi {len(df_to_label)} produk baru yang perlu dilabeli...")
    
    database_df['normalized_name'] = database_df['NAMA'].apply(normalize_text)
    df_to_label['normalized_name'] = df_to_label['NAMA'].apply(normalize_text)
    
    vectorizer = TfidfVectorizer()
    db_matrix = vectorizer.fit_transform(database_df['normalized_name'])
    label_matrix = vectorizer.transform(df_to_label['normalized_name'])
    
    cosine_sim = cosine_similarity(label_matrix, db_matrix)
    
    for index, row in df_to_label.iterrows():
        best_match_idx_in_sim_matrix = cosine_sim[df_to_label.index.get_loc(index)].argmax()
        best_match_row = database_df.iloc[best_match_idx_in_sim_matrix]
        
        db_klik_df.loc[index, 'SKU'] = best_match_row['SKU']
        db_klik_df.loc[index, 'KATEGORI'] = best_match_row['KATEGORI']
        
    st.write("Pelabelan selesai.")
    return db_klik_df, True

def update_gsheet(spreadsheet, worksheet_name, df):
    """Menyimpan dataframe yang sudah diperbarui kembali ke worksheet target."""
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        if 'TANGGAL' in df.columns:
            df_to_save = df.copy()
            df_to_save['TANGGAL'] = df_to_save['TANGGAL'].dt.strftime('%Y-%m-%d')
        else:
            df_to_save = df
            
        worksheet.clear()
        set_with_dataframe(worksheet, df_to_save)
        return True
    except Exception as e:
        st.error(f"Gagal menyimpan data ke '{worksheet_name}': {e}")
        return False

def format_rupiah(val):
    if pd.isna(val) or not isinstance(val, (int, float, np.number)): return "N/A"
    return f"Rp {int(val):,}"

def format_wow_growth(pct_change):
    if pd.isna(pct_change) or pct_change == float('inf'): return "N/A"
    elif pct_change > 0.001: return f"▲ {pct_change:.1%}"
    elif pct_change < -0.001: return f"▼ {pct_change:.1%}"
    else: return f"▬ 0.0%"
    
def style_wow_growth(val):
    color = 'black'
    if isinstance(val, str):
        if '▲' in val: color = 'green'
        elif '▼' in val: color = 'red'
    return f'color: {color}'

# --- 4. Tampilan Utama Aplikasi ---
st.title("🧠 Dashboard Analisis Cerdas Penjualan & Kompetitor")
st.markdown("---")

if 'data_loaded' not in st.session_state:
    all_data, spreadsheet_obj = load_data_from_gsheets()
    if all_data:
        st.session_state.all_data = all_data
        st.session_state.spreadsheet = spreadsheet_obj
        st.session_state.data_loaded = True
        st.session_state.needs_labeling = False
        db_klik_ready = all_data.get('DB KLIK - REKAP - READY', pd.DataFrame())
        db_klik_habis = all_data.get('DB KLIK - REKAP - HABIS', pd.DataFrame())
        if not db_klik_ready.empty and db_klik_ready['SKU'].replace('', pd.NA).isna().any(): st.session_state.needs_labeling = True
        if not db_klik_habis.empty and db_klik_habis['SKU'].replace('', pd.NA).isna().any(): st.session_state.needs_labeling = True
        st.rerun()
    else: st.stop()

if st.session_state.get('data_loaded'):
    if st.session_state.needs_labeling:
        st.warning("**PELABELAN DIPERLUKAN!**\n\nTerdeteksi ada produk baru di data DB KLIK yang belum memiliki SKU dan Kategori. Silakan klik tombol di bawah untuk melakukan pelabelan otomatis sebelum melanjutkan ke analisis.")
        if st.button("🚀 JALANKAN PELABELAN SKU DAN KATEGORI"):
            with st.spinner("Melakukan analisis TF-IDF dan pelabelan... Harap tunggu..."):
                all_data = st.session_state.all_data
                database_df = all_data['DATABASE']
                
                db_klik_ready = all_data.get('DB KLIK - REKAP - READY', pd.DataFrame())
                if not db_klik_ready.empty:
                    updated_ready, changed_ready = label_products(db_klik_ready, database_df)
                    if changed_ready:
                        st.write("Menyimpan data 'READY' yang sudah dilabeli kembali ke Google Sheets...")
                        update_gsheet(st.session_state.spreadsheet, 'DB KLIK - REKAP - READY', updated_ready)
                        st.session_state.all_data['DB KLIK - REKAP - READY'] = updated_ready
                
                db_klik_habis = all_data.get('DB KLIK - REKAP - HABIS', pd.DataFrame())
                if not db_klik_habis.empty:
                    updated_habis, changed_habis = label_products(db_klik_habis, database_df)
                    if changed_habis:
                        st.write("Menyimpan data 'HABIS' yang sudah dilabeli kembali ke Google Sheets...")
                        update_gsheet(st.session_state.spreadsheet, 'DB KLIK - REKAP - HABIS', updated_habis)
                        st.session_state.all_data['DB KLIK - REKAP - HABIS'] = updated_habis
            
            st.success("Pelabelan dan penyimpanan selesai! Aplikasi akan memuat ulang.")
            st.session_state.needs_labeling = False
            st.rerun()
        st.stop()

    master_df_list = []
    for name, df in st.session_state.all_data.items():
        if 'REKAP - READY' in name or 'REKAP - HABIS' in name:
            toko_name = name.split(' - REKAP')[0]
            temp_df = df.copy()
            temp_df['Toko'] = toko_name
            temp_df['Status'] = 'READY' if 'READY' in name else 'HABIS'
            master_df_list.append(temp_df)
            
    master_df = pd.concat(master_df_list, ignore_index=True)
    for col in ['HARGA', 'TERJUAL/BLN']:
        if col in master_df.columns: master_df[col] = pd.to_numeric(master_df[col], errors='coerce').fillna(0)
    master_df['OMZET'] = master_df['HARGA'] * master_df['TERJUAL/BLN']
    master_df.dropna(subset=['TANGGAL'], inplace=True)

    st.sidebar.header("⚙️ Filter & Opsi")
    analysis_mode = st.sidebar.radio("Pilih Mode Analisis:", ("ANALISIS UTAMA", "HPP PRODUK", "SIMILARITY PRODUK"))
    
    min_date, max_date = master_df['TANGGAL'].min().date(), master_df['TANGGAL'].max().date()
    start_date, end_date = st.sidebar.date_input("Pilih Rentang Tanggal:", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    st.sidebar.info(f"Data tersedia dari **{min_date}** hingga **{max_date}**.")
    
    filtered_df = master_df[(master_df['TANGGAL'].dt.date >= start_date) & (master_df['TANGGAL'].dt.date <= end_date)]
    st.sidebar.metric("Jumlah Baris Data Dianalisis", f"{len(filtered_df):,}")
    
    if st.sidebar.button("Jalankan Ulang Pelabelan"):
        st.session_state.needs_labeling = True
        st.rerun()
        
    @st.cache_data
    def to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer: df.to_excel(writer, index=False, sheet_name='AnalisisData')
        return output.getvalue()
        
    excel_data = to_excel(filtered_df)
    st.sidebar.download_button(label="📥 Unduh Data Excel", data=excel_data, file_name=f"analisis_data_{start_date}_to_{end_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if analysis_mode == "ANALISIS UTAMA":
        st.header("📊 Analisis Utama")
        
        my_store_name = "DB KLIK"
        db_klik_df = filtered_df[filtered_df['Toko'] == my_store_name].copy()
        competitor_df = filtered_df[filtered_df['Toko'] != my_store_name].copy()
        
        tab1, tab2, tab3 = st.tabs(["⭐ Analisis Toko Saya", "🏆 Analisis Kompetitor", "🔄 Perbandingan Produk"])

        with tab1:
            st.subheader(f"Analisis Kinerja Toko: {my_store_name}")
            if db_klik_df.empty:
                st.warning("Tidak ada data DB KLIK pada rentang tanggal yang dipilih.")
            else:
                db_klik_df_latest = db_klik_df.loc[db_klik_df.groupby('NAMA')['TANGGAL'].idxmax()]

                st.markdown("#### Peringkat Kategori Berdasarkan Omzet")
                category_omzet = db_klik_df.groupby('KATEGORI')['OMZET'].sum().sort_values(ascending=False).reset_index()
                num_bars = st.slider("Jumlah Kategori Ditampilkan:", 5, len(category_omzet) if len(category_omzet) > 5 else 5, 10, key="cat_slider")
                fig_cat = px.bar(category_omzet.head(num_bars), x='OMZET', y='KATEGORI', orientation='h', title=f"Top {num_bars} Kategori", text_auto='.2s')
                fig_cat.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_cat, use_container_width=True)

                st.markdown("#### Produk Terlaris (Berdasarkan Penjualan Unit Terakhir)")
                top_products = db_klik_df_latest.sort_values('TERJUAL/BLN', ascending=False).head(15).copy()
                top_products['HARGA'] = top_products['HARGA'].apply(format_rupiah)
                top_products['OMZET'] = top_products['OMZET'].apply(format_rupiah)
                st.dataframe(top_products[['NAMA', 'SKU', 'HARGA', 'TERJUAL/BLN', 'OMZET']], use_container_width=True, hide_index=True)

                st.markdown("#### Distribusi Omzet Brand (Snapshot Terakhir)")
                brand_omzet_main = db_klik_df_latest.groupby('BRAND')['OMZET'].sum().reset_index()
                fig_brand_pie = px.pie(brand_omzet_main.sort_values('OMZET', ascending=False).head(7), names='BRAND', values='OMZET', title='Distribusi Omzet Top 7 Brand')
                fig_brand_pie.update_traces(textposition='outside', texttemplate='%{label}<br><b>%{value:,.0f}</b><br>(%{percent})', hole=0.3)
                st.plotly_chart(fig_brand_pie, use_container_width=True)

                st.markdown("#### Ringkasan Kinerja Mingguan (WoW Growth)")
                db_klik_df['Minggu'] = db_klik_df['TANGGAL'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
                weekly_summary = db_klik_df.groupby('Minggu').agg(Omzet=('OMZET', 'sum'), Penjualan_Unit=('TERJUAL/BLN', 'sum')).reset_index().sort_values('Minggu')
                weekly_summary['Pertumbuhan Omzet (WoW)'] = weekly_summary['Omzet'].pct_change().apply(format_wow_growth)
                weekly_summary['Omzet'] = weekly_summary['Omzet'].apply(format_rupiah)
                st.dataframe(weekly_summary.style.applymap(style_wow_growth, subset=['Pertumbuhan Omzet (WoW)']), use_container_width=True, hide_index=True)

        with tab2:
            st.subheader("Analisis Kompetitor")
            if competitor_df.empty:
                st.warning("Tidak ada data kompetitor pada rentang tanggal ini.")
            else:
                competitor_latest = competitor_df.loc[competitor_df.groupby(['Toko', 'NAMA'])['TANGGAL'].idxmax()]
                
                st.markdown("#### Analisis Brand di Toko Kompetitor (Snapshot Terakhir)")
                competitor_list = sorted(competitor_df['Toko'].unique())
                for competitor_store in competitor_list:
                    with st.expander(f"Analisis untuk: **{competitor_store}**"):
                        single_competitor_df = competitor_latest[competitor_latest['Toko'] == competitor_store]
                        brand_analysis = single_competitor_df.groupby('BRAND').agg(Total_Omzet=('OMZET', 'sum'), Total_Unit_Terjual=('TERJUAL/BLN', 'sum')).reset_index().sort_values("Total_Omzet", ascending=False)
                        if not brand_analysis.empty:
                            brand_analysis['Total_Omzet'] = brand_analysis['Total_Omzet'].apply(format_rupiah)
                            st.dataframe(brand_analysis.head(10), use_container_width=True, hide_index=True)
                        else: st.info("Tidak ada data brand untuk toko ini.")

                st.markdown("#### Perbandingan Kinerja Penjualan (Semua Toko)")
                filtered_df['Minggu'] = filtered_df['TANGGAL'].dt.to_period('W-SUN').apply(lambda p: p.start_time).dt.date
                weekly_omzet = filtered_df.groupby(['Minggu', 'Toko'])['OMZET'].sum().reset_index()
                fig_weekly_omzet = px.line(weekly_omzet, x='Minggu', y='OMZET', color='Toko', markers=True, title='Perbandingan Omzet Mingguan Antar Toko')
                st.plotly_chart(fig_weekly_omzet, use_container_width=True)
            
                st.markdown("#### Tren Status Stok Mingguan per Toko")
                stock_trends = filtered_df.groupby(['Minggu', 'Toko', 'Status']).size().unstack(fill_value=0).reset_index()
                if 'READY' not in stock_trends.columns: stock_trends['READY'] = 0
                if 'HABIS' not in stock_trends.columns: stock_trends['HABIS'] = 0
                stock_trends_melted = stock_trends.melt(id_vars=['Minggu', 'Toko'], value_vars=['READY', 'HABIS'], var_name='Tipe Stok', value_name='Jumlah Produk')
                fig_stock_trends = px.line(stock_trends_melted, x='Minggu', y='Jumlah Produk', color='Toko', line_dash='Tipe Stok', markers=True, title='Jumlah Produk READY vs. HABIS per Minggu')
                st.plotly_chart(fig_stock_trends, use_container_width=True)

        with tab3:
            st.subheader("Perbandingan Produk Baru dan Habis Antar Tanggal")
            unique_dates = sorted(master_df['TANGGAL'].dt.date.unique(), reverse=True)
            if len(unique_dates) < 2:
                st.info("Dibutuhkan setidaknya 2 tanggal berbeda untuk melakukan perbandingan.")
            else:
                col1, col2 = st.columns(2)
                date_target = col1.selectbox("Pilih Tanggal Target:", unique_dates, index=0)
                date_compare = col2.selectbox("Pilih Tanggal Pembanding:", unique_dates, index=min(1, len(unique_dates)-1))

                if date_target and date_compare and date_target != date_compare:
                    df_target = master_df[master_df['TANGGAL'].dt.date == date_target]
                    df_compare = master_df[master_df['TANGGAL'].dt.date == date_compare]

                    products_target = set(df_target['NAMA'])
                    products_compare = set(df_compare['NAMA'])

                    new_products = products_target - products_compare
                    discontinued_products = products_compare - products_target
                    
                    st.metric("Produk Baru Ditemukan", f"{len(new_products):,}")
                    with st.expander("Lihat Daftar Produk Baru"):
                        st.dataframe(pd.DataFrame(list(new_products), columns=['Nama Produk']), use_container_width=True)

                    st.metric("Produk Tidak Lagi Ditemukan (Habis/Delist)", f"{len(discontinued_products):,}")
                    with st.expander("Lihat Daftar Produk Hilang"):
                        st.dataframe(pd.DataFrame(list(discontinued_products), columns=['Nama Produk']), use_container_width=True)
                else:
                    st.warning("Silakan pilih dua tanggal yang berbeda.")

    elif analysis_mode == "HPP PRODUK":
        st.header("💰 Analisis Harga Pokok Penjualan (HPP)")
        database_df = st.session_state.all_data.get('DATABASE', pd.DataFrame())
        if database_df.empty or 'SKU' not in database_df.columns:
            st.error("Sheet 'DATABASE' tidak ditemukan atau tidak memiliki kolom 'SKU'. Analisis HPP tidak dapat dilanjutkan."); st.stop()
        
        if 'HPP (LATEST)' not in database_df.columns: database_df['HPP (LATEST)'] = np.nan
        if 'HPP (AVERAGE)' not in database_df.columns: database_df['HPP (AVERAGE)'] = np.nan
        database_df['HPP_LATEST_NUM'] = pd.to_numeric(database_df['HPP (LATEST)'], errors='coerce')
        database_df['HPP_AVERAGE_NUM'] = pd.to_numeric(database_df['HPP (AVERAGE)'], errors='coerce')
        database_df['HPP'] = database_df['HPP_LATEST_NUM'].fillna(database_df['HPP_AVERAGE_NUM'])
        
        hpp_data = database_df[['SKU', 'HPP']].copy()
        hpp_data.dropna(subset=['SKU', 'HPP'], inplace=True)
        hpp_data = hpp_data[hpp_data['SKU'] != ''].drop_duplicates(subset=['SKU'], keep='first')
        
        db_klik_df = master_df[master_df['Toko'] == 'DB KLIK'].copy()
        latest_db_klik = db_klik_df.loc[db_klik_df.groupby('SKU')['TANGGAL'].idxmax()]
        merged_df = pd.merge(latest_db_klik, hpp_data, on='SKU', how='left')
        
        merged_df['Selisih'] = merged_df['HARGA'] - merged_df['HPP']
        df_rugi = merged_df[merged_df['Selisih'] < 0].copy()
        df_untung = merged_df[merged_df['Selisih'] >= 0].copy()
        df_tidak_ditemukan = merged_df[merged_df['HPP'].isnull()].copy()
        
        st.subheader("🔴 Produk Lebih Murah dari HPP")
        if df_rugi.empty: st.success("👍 Mantap! Tidak ada produk yang dijual di bawah HPP.")
        else:
            display_rugi = df_rugi[['NAMA', 'SKU', 'HARGA', 'HPP', 'Selisih', 'TERJUAL/BLN', 'OMZET']].copy()
            for col in ['HARGA', 'HPP', 'Selisih', 'OMZET']: display_rugi[col] = display_rugi[col].apply(format_rupiah)
            st.dataframe(display_rugi, use_container_width=True, hide_index=True)
            
        st.divider()
        st.subheader("🟢 Produk Lebih Mahal dari HPP")
        if df_untung.empty: st.warning("Tidak ada produk yang dijual di atas HPP.")
        else:
            display_untung = df_untung[['NAMA', 'SKU', 'HARGA', 'HPP', 'Selisih', 'TERJUAL/BLN', 'OMZET']].copy()
            for col in ['HARGA', 'HPP', 'Selisih', 'OMZET']: display_untung[col] = display_untung[col].apply(format_rupiah)
            st.dataframe(display_untung, use_container_width=True, hide_index=True)
            
        st.divider()
        st.subheader("❓ Produk Tidak Terdeteksi HPP-nya")
        if df_tidak_ditemukan.empty: st.success("👍 Semua produk yang dijual berhasil dicocokkan dengan data HPP di DATABASE.")
        else:
            st.warning("Produk berikut tidak memiliki HPP di sheet 'DATABASE' atau SKU tidak cocok.")
            display_na = df_tidak_ditemukan[['NAMA', 'SKU', 'HARGA', 'TERJUAL/BLN', 'OMZET']].copy()
            for col in ['HARGA', 'OMZET']: display_na[col] = display_na[col].apply(format_rupiah)
            st.dataframe(display_na, use_container_width=True, hide_index=True)

    elif analysis_mode == "SIMILARITY PRODUK":
        st.header("🤝 Analisis Kemiripan Produk")
        df_latest = master_df[master_df['TANGGAL'] == master_df['TANGGAL'].max()]
        my_store_df = df_latest[df_latest['Toko'] == 'DB KLIK'].copy()
        competitor_df = df_latest[df_latest['Toko'] != 'DB KLIK'].copy()
        
        my_product_list = sorted(my_store_df['NAMA'].unique())
        selected_product = st.selectbox("Pilih produk DB KLIK untuk dianalisis:", my_product_list)
        
        if selected_product and not competitor_df.empty:
            with st.spinner("Mencari produk serupa di toko kompetitor..."):
                my_product_details = my_store_df[my_store_df['NAMA'] == selected_product].iloc[0]
                my_product_norm = normalize_text(my_product_details['NAMA'])
                
                competitor_df['normalized_name'] = competitor_df['NAMA'].apply(normalize_text)
                
                vectorizer = TfidfVectorizer()
                all_names = [my_product_norm] + competitor_df['normalized_name'].tolist()
                tfidf_matrix = vectorizer.fit_transform(all_names)
                
                cosine_sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:])
                
                threshold = 0.70
                similar_indices = [i for i, score in enumerate(cosine_sim[0]) if score >= threshold]
                
                matches_df = competitor_df.iloc[similar_indices].copy()
                matches_df['SKOR'] = cosine_sim[0][similar_indices] * 100
                
                st.subheader("Hasil Perbandingan:")
                st.write(f"**Produk Anda:** {my_product_details['NAMA']} - **Harga:** {format_rupiah(my_product_details['HARGA'])}")
                st.markdown("---")
                
                if not matches_df.empty:
                    all_prices_df = pd.concat([pd.DataFrame([my_product_details]), matches_df])
                    avg_price = all_prices_df['HARGA'].mean()
                    num_stores = matches_df['Toko'].nunique()
                    top_store = all_prices_df.loc[all_prices_df['OMZET'].idxmax()]
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Rata-rata Harga (Termasuk Anda)", format_rupiah(avg_price))
                    col2.metric("Ditemukan di Kompetitor", f"{num_stores} Toko")
                    col3.metric("Toko Omzet Tertinggi", top_store['Toko'], format_rupiah(top_store['OMZET']))
                    
                    matches_df['HARGA'] = matches_df['HARGA'].apply(format_rupiah)
                    matches_df['OMZET'] = matches_df['OMZET'].apply(format_rupiah)
                    st.dataframe(matches_df[['NAMA', 'Toko', 'HARGA', 'OMZET', 'SKOR']].sort_values('SKOR', ascending=False), use_container_width=True, hide_index=True)
                else: 
                    st.info("Tidak ditemukan produk yang serupa secara signifikan di toko kompetitor pada data terbaru.")
else:
    st.info("Memulai aplikasi... Harap tunggu.")
