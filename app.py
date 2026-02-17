import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="Subsidios de Vivienda Nacional - Camacol",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

CSV_GENERAL_URL = "https://raw.githubusercontent.com/JulianTorrest/Subsidio-Vivienda/refs/heads/main/Subsidios_De_Vivienda_Asignados_20260217.csv"
CSV_RURAL_URL = "https://raw.githubusercontent.com/JulianTorrest/Subsidio-Vivienda/refs/heads/main/Subsidios_de_Vivienda_Rural_Asignados_20260217.csv"
CSV_MILITARY_URL = "https://raw.githubusercontent.com/JulianTorrest/Subsidio-Vivienda/refs/heads/main/Subsidios_de_Vivienda_de_la_Caja_Promotora_de_Vivienda_Militar_y_de_Polic%C3%ADa_20260217.csv"
CSV_CMC_MCY_URL = "https://raw.githubusercontent.com/JulianTorrest/Subsidio-Vivienda/refs/heads/main/Subsidios_Mejoramientos_Programas_CMC-MCY_20260217.csv"
CSV_USED_HOUSING_URL = "https://raw.githubusercontent.com/JulianTorrest/Subsidio-Vivienda/refs/heads/main/Subsidios_de_vivienda_-_Vivienda_USADA_20260217.csv"
CSV_DATE = "20260217"
API_BASE_URL = "https://www.datos.gov.co/resource/h2yr-zfb2.json"
API_METADATA_URL = "https://www.datos.gov.co/api/views/h2yr-zfb2.json"

@st.cache_data(ttl=3600)
def get_api_last_update():
    """
    Get the last update date from the API metadata.
    Returns date in YYYYMMDD format or None if error.
    """
    try:
        response = requests.get(API_METADATA_URL, timeout=10)
        response.raise_for_status()
        metadata = response.json()
        
        if 'rowsUpdatedAt' in metadata:
            timestamp = metadata['rowsUpdatedAt']
            date_obj = datetime.fromtimestamp(timestamp)
            return date_obj.strftime("%Y%m%d")
        elif 'dataUpdatedAt' in metadata:
            timestamp = metadata['dataUpdatedAt']
            date_obj = datetime.fromtimestamp(timestamp)
            return date_obj.strftime("%Y%m%d")
    except Exception as e:
        st.warning(f"No se pudo verificar la fecha de actualización de la API: {str(e)}")
    
    return None

@st.cache_data(ttl=3600)
def fetch_data_from_api():
    """
    Fetch all data from the API with pagination handling.
    The API has a default limit of 1000 rows, so we fetch in batches of 900.
    """
    all_data = []
    offset = 0
    limit = 900
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        status_text.text('Cargando datos desde la API del Ministerio de Vivienda...')
        
        while True:
            try:
                params = {
                    "$limit": limit,
                    "$offset": offset
                }
                response = requests.get(API_BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                
                all_data.extend(data)
                offset += limit
                
                status_text.text(f'Cargados {len(all_data):,} registros...')
                
                if len(data) < limit:
                    break
                    
            except requests.exceptions.RequestException as e:
                st.error(f"Error al cargar datos de la API: {str(e)}")
                break
        
        progress_bar.progress(100)
        status_text.text(f'✅ Carga completa: {len(all_data):,} registros desde la API')
        
    finally:
        progress_bar.empty()
        status_text.empty()
    
    if all_data:
        df = pd.DataFrame(all_data)
        return process_dataframe(df, 'general')
    
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_csv_data():
    """
    Load general subsidy data from the CSV file hosted on GitHub.
    """
    try:
        df = pd.read_csv(CSV_GENERAL_URL)
        return process_dataframe(df, 'general')
    except Exception as e:
        st.error(f"Error al cargar el archivo CSV general: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_csv_rural_data():
    """
    Load rural subsidy data from the CSV file hosted on GitHub.
    """
    try:
        df = pd.read_csv(CSV_RURAL_URL)
        return process_dataframe(df, 'rural')
    except Exception as e:
        st.error(f"Error al cargar el archivo CSV rural: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner="Cargando datos militares...")
def load_csv_military_data():
    """
    Load military/police subsidy data from the CSV file hosted on GitHub.
    """
    try:
        df = pd.read_csv(CSV_MILITARY_URL)
        processed_df = process_dataframe(df, 'military')
        if processed_df.empty:
            st.error("El dataframe procesado está vacío")
        return processed_df
    except Exception as e:
        st.error(f"Error al cargar el archivo CSV militar/policía: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_csv_cmc_mcy_data():
    """
    Load CMC-MCY housing improvement subsidy data from the CSV file hosted on GitHub.
    """
    try:
        df = pd.read_csv(CSV_CMC_MCY_URL)
        return process_dataframe(df, 'cmc_mcy')
    except Exception as e:
        st.error(f"Error al cargar el archivo CSV CMC-MCY: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_csv_used_housing_data():
    """
    Load used housing subsidy data from the CSV file hosted on GitHub.
    """
    try:
        df = pd.read_csv(CSV_USED_HOUSING_URL)
        return process_dataframe(df, 'used_housing')
    except Exception as e:
        st.error(f"Error al cargar el archivo CSV Vivienda Usada: {str(e)}")
        return pd.DataFrame()

def process_dataframe(df, dataset_type='general'):
    """
    Process and clean the dataframe.
    dataset_type: 'general', 'rural', 'military', 'cmc_mcy', or 'used_housing'
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('ó', 'o').str.replace('ñ', 'n').str.replace('í', 'i').str.replace('.', '_').str.replace('-', '_')
    df.columns = df.columns.str.replace('__+', '_', regex=True)
    
    if dataset_type == 'general':
        if 'hogares' in df.columns:
            df['hogares'] = pd.to_numeric(df['hogares'].astype(str).str.replace(',', '').str.replace('$', '').str.replace('.', ''), errors='coerce')
        if 'valor_asignado' in df.columns:
            df['valor_asignado'] = pd.to_numeric(df['valor_asignado'].astype(str).str.replace(',', '').str.replace('$', '').str.replace('.', ''), errors='coerce')
        if 'ano_de_asignacion' in df.columns:
            df['a_o_de_asignaci_n'] = pd.to_numeric(df['ano_de_asignacion'].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
    elif dataset_type == 'rural':
        if 'no_sfv_asignados' in df.columns:
            df['no_sfv_asignados'] = pd.to_numeric(df['no_sfv_asignados'].astype(str).str.replace(',', '').str.replace('$', '').str.replace('.', ''), errors='coerce')
        elif 'no_sfv_asignados' in df.columns:
            df['no_sfv_asignados'] = pd.to_numeric(df['no_sfv_asignados'].astype(str).str.replace(',', '').str.replace('$', '').str.replace('.', ''), errors='coerce')
        if 'valor_asignado' in df.columns:
            df['valor_asignado'] = pd.to_numeric(df['valor_asignado'].astype(str).str.replace(',', '').str.replace('$', '').str.replace('.', ''), errors='coerce')
        if 'ano_de_asignacion' in df.columns:
            df['a_o_de_asignacion'] = pd.to_numeric(df['ano_de_asignacion'].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
    elif dataset_type == 'military':
        if 'departamento' in df.columns:
            df['departamento'] = df['departamento'].str.replace(
                'archipielago_de_san_andres_providencia_y_santa_ca',
                'archipielago_de_san_andres_providencia_y_santa_catalina',
                regex=False
            )
        
        if 'ano' in df.columns:
            df['ano'] = pd.to_numeric(df['ano'].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
        if 'trimestre' in df.columns:
            df['trimestre'] = pd.to_numeric(df['trimestre'], errors='coerce')
        
        housing_cols = ['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis']
        for col in housing_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
        
        existing_housing_cols = [col for col in housing_cols if col in df.columns]
        if len(existing_housing_cols) > 0:
            df['total_subsidios'] = df[existing_housing_cols].sum(axis=1)
        else:
            df['total_subsidios'] = 0
    elif dataset_type == 'cmc_mcy':
        if 'departamento' in df.columns:
            df['departamento'] = df['departamento'].str.strip()
            dept_mapping = {
                'ATLANTICO': 'ATLÁNTICO',
                'BOGOTA D. C.': 'BOGOTÁ, D.C.',
                'BOLIVAR': 'BOLÍVAR',
                'BOYACA': 'BOYACÁ',
                'CAQUETA': 'CAQUETÁ',
                'CORDOBA': 'CÓRDOBA',
                'GUAINIA': 'GUAINÍA',
                'QUINDIO': 'QUINDÍO'
            }
            df['departamento'] = df['departamento'].replace(dept_mapping)
        
        if 'no_sfv_asignados' in df.columns:
            df['no_sfv_asignados'] = pd.to_numeric(df['no_sfv_asignados'].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
        if 'valor_asignado' in df.columns:
            df['valor_asignado'] = pd.to_numeric(df['valor_asignado'].astype(str).str.replace(',', '').str.replace('$', '').str.replace('.', ''), errors='coerce')
        if 'ano_de_asignacion' in df.columns:
            df['ano_de_asignacion'] = pd.to_numeric(df['ano_de_asignacion'].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
    elif dataset_type == 'used_housing':
        if 'ano' in df.columns:
            df['ano'] = pd.to_numeric(df['ano'].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
        if 'trimestre' in df.columns:
            df['trimestre'] = pd.to_numeric(df['trimestre'], errors='coerce')
        
        force_cols = [
            'vis_policia_nacional', 'vis_ejercito_nacional', 'vis_fuerza_aerea_colombiana', 
            'vis_armada_nacional', 'vis_otros',
            'no_vis_policia_nacional', 'no_vis_ejercito_nacional', 'no_vis_fuerza_aerea_colombiana',
            'no_vis_armada_nacional', 'no_vis_otros'
        ]
        for col in force_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '').str.replace('.', ''), errors='coerce')
        
        vis_cols = [c for c in force_cols if c.startswith('vis_') and c in df.columns]
        no_vis_cols = [c for c in force_cols if c.startswith('no_vis_') and c in df.columns]
        
        if vis_cols:
            df['total_vis'] = df[vis_cols].sum(axis=1)
        if no_vis_cols:
            df['total_no_vis'] = df[no_vis_cols].sum(axis=1)
        if vis_cols or no_vis_cols:
            df['total_subsidios'] = df[vis_cols + no_vis_cols].sum(axis=1)
    
    return df

def load_data(force_api=False):
    """
    Main data loading function.
    Loads from CSV by default, checks API date, and loads from API if newer data is available.
    """
    if force_api:
        return fetch_data_from_api(), "API", datetime.now().strftime("%Y%m%d")
    
    api_date = get_api_last_update()
    
    if api_date and api_date > CSV_DATE:
        st.info(f"📡 Datos más recientes disponibles en la API (Fecha API: {api_date} vs CSV: {CSV_DATE}). Cargando desde la API...")
        return fetch_data_from_api(), "API", api_date
    else:
        if api_date:
            st.success(f"✅ El archivo CSV está actualizado (Fecha: {CSV_DATE}). Cargando datos locales...")
        return load_csv_data(), "CSV", CSV_DATE

def format_currency(value):
    """Format value as Colombian Pesos"""
    if pd.isna(value):
        return "N/A"
    return f"${value:,.0f}"

def format_number(value):
    """Format number with thousands separator"""
    if pd.isna(value):
        return "N/A"
    return f"{value:,.0f}"

st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f4788;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f4788;
    }
    .stMetric {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🏠 Subsidios de Vivienda Nacional</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Camacol - Análisis de Subsidios Asignados en Colombia</div>', unsafe_allow_html=True)

subsidy_type = st.radio(
    "Seleccione el tipo de subsidio:",
    ["🏘️ Subsidio General", "🌾 Subsidio Rural", "🎖️ Subsidio Militar/Policía", "🏗️ Mejoramiento CMC-MCY", "🏠 Vivienda Usada"],
    horizontal=True,
    label_visibility="collapsed"
)

st.markdown("---")

if 'force_refresh' not in st.session_state:
    st.session_state.force_refresh = False

if "🌾 Subsidio Rural" in subsidy_type:
    df_rural = load_csv_rural_data()
    df = df_rural
    data_source = "CSV Rural"
    data_date = CSV_DATE
    dataset_type = 'rural'
elif "🎖️ Subsidio Militar/Policía" in subsidy_type:
    df_military = load_csv_military_data()
    df = df_military
    data_source = "CSV Militar/Policía"
    data_date = CSV_DATE
    dataset_type = 'military'
elif "🏗️ Mejoramiento CMC-MCY" in subsidy_type:
    df_cmc_mcy = load_csv_cmc_mcy_data()
    df = df_cmc_mcy
    data_source = "CSV CMC-MCY"
    data_date = CSV_DATE
    dataset_type = 'cmc_mcy'
elif "🏠 Vivienda Usada" in subsidy_type:
    df_used = load_csv_used_housing_data()
    df = df_used
    data_source = "CSV Vivienda Usada"
    data_date = CSV_DATE
    dataset_type = 'used_housing'
else:
    df, data_source, data_date = load_data(force_api=st.session_state.force_refresh)
    dataset_type = 'general'

if st.session_state.force_refresh:
    st.session_state.force_refresh = False
    st.rerun()

if df.empty:
    st.error("No se pudieron cargar los datos. Por favor, intente nuevamente.")
    st.stop()

col_info1, col_info2, col_info3 = st.columns([2, 2, 1])

with col_info1:
    st.info(f"📊 **Registros cargados:** {len(df):,}")

with col_info2:
    formatted_date = f"{data_date[6:8]}/{data_date[4:6]}/{data_date[0:4]}"
    st.info(f"📅 **Fuente:** {data_source} | **Fecha:** {formatted_date}")

with col_info3:
    if dataset_type == 'general':
        if st.button("🔄 Actualizar desde API", help="Forzar actualización desde la API"):
            st.session_state.force_refresh = True
            st.cache_data.clear()
            st.rerun()

st.sidebar.header("🔍 Filtros")

if 'departamento' in df.columns:
    departamentos = ['Todos'] + sorted(df['departamento'].dropna().unique().tolist())
    selected_dept = st.sidebar.selectbox("Departamento", departamentos)
else:
    selected_dept = 'Todos'

if dataset_type not in ['military', 'used_housing']:
    if 'municipio' in df.columns and selected_dept != 'Todos':
        municipios_filtered = df[df['departamento'] == selected_dept]['municipio'].dropna().unique()
        municipios = ['Todos'] + sorted(municipios_filtered.tolist())
        selected_muni = st.sidebar.selectbox("Municipio", municipios)
    else:
        selected_muni = 'Todos'
    
    if 'programa' in df.columns:
        programas = ['Todos'] + sorted(df['programa'].dropna().unique().tolist())
        selected_prog = st.sidebar.selectbox("Programa", programas)
    else:
        selected_prog = 'Todos'
else:
    selected_muni = 'Todos'
    selected_prog = 'Todos'
    
    if 'trimestre' in df.columns:
        trimestres = ['Todos'] + sorted([str(int(t)) for t in df['trimestre'].dropna().unique()])
        selected_trimestre = st.sidebar.selectbox("Trimestre", trimestres)
    else:
        selected_trimestre = 'Todos'

if dataset_type in ['military', 'used_housing']:
    year_col = 'ano'
elif dataset_type in ['rural', 'cmc_mcy']:
    year_col = 'ano_de_asignacion'
else:
    year_col = 'a_o_de_asignaci_n'

if year_col in df.columns:
    years = df[year_col].dropna()
    years = years[years > 1900]
    years = sorted(years.unique())
    if len(years) > 0:
        try:
            min_year = int(min(years))
            max_year = int(max(years))
            if min_year < max_year:
                selected_years = st.sidebar.slider(
                    "Año de Asignación",
                    min_value=min_year,
                    max_value=max_year,
                    value=(min_year, max_year)
                )
            else:
                selected_years = (min_year, min_year)
        except:
            selected_years = None
    else:
        selected_years = None
else:
    selected_years = None

if dataset_type == 'general':
    if 'estado_de_postulacion' in df.columns:
        estados = ['Todos'] + sorted(df['estado_de_postulacion'].dropna().unique().tolist())
        selected_estado = st.sidebar.selectbox("Estado de Postulación", estados)
    else:
        selected_estado = 'Todos'
else:
    if 'estado' in df.columns:
        estados = ['Todos'] + sorted(df['estado'].dropna().unique().tolist())
        selected_estado = st.sidebar.selectbox("Estado", estados)
    else:
        selected_estado = 'Todos'

df_filtered = df.copy()

if selected_dept != 'Todos' and 'departamento' in df.columns:
    df_filtered = df_filtered[df_filtered['departamento'] == selected_dept]

if dataset_type not in ['military', 'used_housing']:
    if selected_muni != 'Todos' and 'municipio' in df.columns:
        df_filtered = df_filtered[df_filtered['municipio'] == selected_muni]
    
    if selected_prog != 'Todos' and 'programa' in df.columns:
        df_filtered = df_filtered[df_filtered['programa'] == selected_prog]
else:
    if selected_trimestre != 'Todos' and 'trimestre' in df.columns:
        df_filtered = df_filtered[df_filtered['trimestre'] == int(selected_trimestre)]

if selected_years and year_col in df.columns:
    df_filtered = df_filtered[
        (df_filtered[year_col] >= selected_years[0]) &
        (df_filtered[year_col] <= selected_years[1])
    ]

if dataset_type == 'general':
    if selected_estado != 'Todos' and 'estado_de_postulacion' in df.columns:
        df_filtered = df_filtered[df_filtered['estado_de_postulacion'] == selected_estado]
else:
    if selected_estado != 'Todos' and 'estado' in df.columns:
        df_filtered = df_filtered[df_filtered['estado'] == selected_estado]

# Mostrar filtros activos
active_filters = []
if selected_dept != 'Todos':
    active_filters.append(f"📍 Departamento: **{selected_dept}**")
if dataset_type not in ['military', 'used_housing']:
    if selected_muni != 'Todos':
        active_filters.append(f"🏙️ Municipio: **{selected_muni}**")
    if selected_prog != 'Todos':
        active_filters.append(f"📋 Programa: **{selected_prog}**")
else:
    if selected_trimestre != 'Todos':
        active_filters.append(f"📅 Trimestre: **{selected_trimestre}**")
if selected_years:
    active_filters.append(f"📆 Años: **{selected_years[0]} - {selected_years[1]}**")
if dataset_type == 'general' and selected_estado != 'Todos':
    active_filters.append(f"✅ Estado: **{selected_estado}**")
elif dataset_type != 'general' and selected_estado != 'Todos':
    active_filters.append(f"✅ Estado: **{selected_estado}**")

if active_filters:
    st.info("🔍 **Filtros activos:** " + " | ".join(active_filters))

st.header("📊 Indicadores Principales")

col1, col2, col3, col4 = st.columns(4)

if dataset_type == 'general':
    with col1:
        total_hogares = df_filtered['hogares'].sum() if 'hogares' in df_filtered.columns else 0
        st.metric("Total Hogares Beneficiados", format_number(total_hogares))
    
    with col2:
        total_valor = df_filtered['valor_asignado'].sum() if 'valor_asignado' in df_filtered.columns else 0
        st.metric("Valor Total Asignado", format_currency(total_valor))
    
    with col3:
        total_registros = len(df_filtered)
        st.metric("Registros", format_number(total_registros))
    
    with col4:
        if 'departamento' in df_filtered.columns:
            total_deptos = df_filtered['departamento'].nunique()
            st.metric("Departamentos", format_number(total_deptos))
elif dataset_type in ['rural', 'cmc_mcy']:
    with col1:
        total_sfv = df_filtered['no_sfv_asignados'].sum() if 'no_sfv_asignados' in df_filtered.columns else 0
        st.metric("Total SFV Asignados", format_number(total_sfv))
    
    with col2:
        total_valor = df_filtered['valor_asignado'].sum() if 'valor_asignado' in df_filtered.columns else 0
        st.metric("Valor Total Asignado", format_currency(total_valor))
    
    with col3:
        total_registros = len(df_filtered)
        st.metric("Registros", format_number(total_registros))
    
    with col4:
        if 'departamento' in df_filtered.columns:
            total_deptos = df_filtered['departamento'].nunique()
            st.metric("Departamentos", format_number(total_deptos))
elif dataset_type == 'military':
    with col1:
        total_subsidios = df_filtered['total_subsidios'].sum() if 'total_subsidios' in df_filtered.columns else 0
        st.metric("Total Subsidios", format_number(total_subsidios))
    
    with col2:
        total_vis = df_filtered[['nueva_vis', 'usada_vis']].sum().sum() if all(c in df_filtered.columns for c in ['nueva_vis', 'usada_vis']) else 0
        st.metric("Total VIS", format_number(total_vis))
    
    with col3:
        total_no_vis = df_filtered[['nueva_no_vis', 'usada_no_vis']].sum().sum() if all(c in df_filtered.columns for c in ['nueva_no_vis', 'usada_no_vis']) else 0
        st.metric("Total No VIS", format_number(total_no_vis))
    
    with col4:
        if 'departamento' in df_filtered.columns:
            total_deptos = df_filtered['departamento'].nunique()
            st.metric("Departamentos", format_number(total_deptos))
else:
    with col1:
        total_subsidios = df_filtered['total_subsidios'].sum() if 'total_subsidios' in df_filtered.columns else 0
        st.metric("Total Subsidios", format_number(total_subsidios))
    
    with col2:
        total_vis = df_filtered['total_vis'].sum() if 'total_vis' in df_filtered.columns else 0
        st.metric("Total VIS", format_number(total_vis))
    
    with col3:
        total_no_vis = df_filtered['total_no_vis'].sum() if 'total_no_vis' in df_filtered.columns else 0
        st.metric("Total No VIS", format_number(total_no_vis))
    
    with col4:
        if 'departamento' in df_filtered.columns:
            total_deptos = df_filtered['departamento'].nunique()
            st.metric("Departamentos", format_number(total_deptos))

st.markdown("---")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Por Departamento", "📅 Por Año", "🏘️ Por Programa", "� Analítica Avanzada", "� Datos Detallados"])

with tab1:
    st.subheader("Subsidios por Departamento")
    
    if dataset_type in ['military', 'used_housing']:
        beneficiary_col = 'total_subsidios'
        beneficiary_label = 'Total Subsidios'
    elif dataset_type in ['rural', 'cmc_mcy']:
        beneficiary_col = 'no_sfv_asignados'
        beneficiary_label = 'SFV Asignados'
    else:
        beneficiary_col = 'hogares'
        beneficiary_label = 'Hogares'
    
    if 'departamento' in df_filtered.columns and beneficiary_col in df_filtered.columns:
        if dataset_type in ['military', 'used_housing']:
            dept_data = df_filtered.groupby('departamento').agg({
                beneficiary_col: 'sum'
            }).reset_index().sort_values(beneficiary_col, ascending=False).head(20)
        else:
            dept_data = df_filtered.groupby('departamento').agg({
                beneficiary_col: 'sum',
                'valor_asignado': 'sum'
            }).reset_index().sort_values(beneficiary_col, ascending=False).head(20)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_dept_beneficiaries = px.bar(
                dept_data,
                x=beneficiary_col,
                y='departamento',
                orientation='h',
                title=f'Top 20 Departamentos por {beneficiary_label}',
                labels={beneficiary_col: beneficiary_label, 'departamento': 'Departamento'},
                color=beneficiary_col,
                color_continuous_scale='Blues'
            )
            fig_dept_beneficiaries.update_layout(height=600)
            st.plotly_chart(fig_dept_beneficiaries, use_container_width=True)
        
        with col2:
            if dataset_type == 'military':
                vis_data = df_filtered.groupby('departamento')[['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis']].sum().reset_index()
                vis_data = vis_data.head(20)
                fig_housing_types = px.bar(
                    vis_data,
                    x=['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis'],
                    y='departamento',
                    orientation='h',
                    title='Top 20 Departamentos por Tipo de Vivienda',
                    labels={'value': 'Cantidad', 'departamento': 'Departamento', 'variable': 'Tipo'},
                    barmode='stack'
                )
                fig_housing_types.update_layout(height=600)
                st.plotly_chart(fig_housing_types, use_container_width=True)
            elif dataset_type == 'used_housing':
                vis_data = df_filtered.groupby('departamento')[['total_vis', 'total_no_vis']].sum().reset_index()
                vis_data = vis_data.head(20)
                fig_vis_comparison = px.bar(
                    vis_data,
                    x=['total_vis', 'total_no_vis'],
                    y='departamento',
                    orientation='h',
                    title='Top 20 Departamentos: VIS vs No VIS',
                    labels={'value': 'Cantidad', 'departamento': 'Departamento', 'variable': 'Tipo'},
                    barmode='group',
                    color_discrete_map={'total_vis': '#1f4788', 'total_no_vis': '#28a745'}
                )
                fig_vis_comparison.update_layout(height=600)
                st.plotly_chart(fig_vis_comparison, use_container_width=True)
            else:
                fig_dept_valor = px.bar(
                    dept_data,
                    x='valor_asignado',
                    y='departamento',
                    orientation='h',
                    title='Top 20 Departamentos por Valor Asignado',
                    labels={'valor_asignado': 'Valor Asignado (COP)', 'departamento': 'Departamento'},
                    color='valor_asignado',
                    color_continuous_scale='Greens'
                )
                fig_dept_valor.update_layout(height=600)
                st.plotly_chart(fig_dept_valor, use_container_width=True)

with tab2:
    st.subheader("Evolución Temporal de Subsidios")
    
    if dataset_type in ['military', 'used_housing']:
        year_col = 'ano'
        beneficiary_col = 'total_subsidios'
        beneficiary_label = 'Total Subsidios'
    elif dataset_type in ['rural', 'cmc_mcy']:
        year_col = 'ano_de_asignacion'
        beneficiary_col = 'no_sfv_asignados'
        beneficiary_label = 'SFV Asignados'
    else:
        year_col = 'a_o_de_asignaci_n'
        beneficiary_col = 'hogares'
        beneficiary_label = 'Hogares'
    
    if year_col in df_filtered.columns:
        if dataset_type in ['military', 'used_housing']:
            year_data = df_filtered.groupby(year_col).agg({
                beneficiary_col: 'sum'
            }).reset_index().sort_values(year_col)
        else:
            year_data = df_filtered.groupby(year_col).agg({
                beneficiary_col: 'sum',
                'valor_asignado': 'sum'
            }).reset_index().sort_values(year_col)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_year_beneficiaries = px.line(
                year_data,
                x=year_col,
                y=beneficiary_col,
                title=f'{beneficiary_label} por Año',
                labels={year_col: 'Año', beneficiary_col: beneficiary_label},
                markers=True
            )
            fig_year_beneficiaries.update_traces(line_color='#1f4788', line_width=3)
            st.plotly_chart(fig_year_beneficiaries, use_container_width=True)
        
        with col2:
            if dataset_type == 'military':
                housing_types_year = df_filtered.groupby(year_col)[['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis']].sum().reset_index()
                fig_housing_year = px.line(
                    housing_types_year,
                    x=year_col,
                    y=['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis'],
                    title='Tipos de Vivienda por Año',
                    labels={year_col: 'Año', 'value': 'Cantidad', 'variable': 'Tipo'},
                    markers=True
                )
                st.plotly_chart(fig_housing_year, use_container_width=True)
            else:
                fig_year_valor = px.line(
                    year_data,
                    x=year_col,
                    y='valor_asignado',
                    title='Valor Asignado por Año',
                    labels={year_col: 'Año', 'valor_asignado': 'Valor Asignado (COP)'},
                    markers=True
                )
                fig_year_valor.update_traces(line_color='#28a745', line_width=3)
                st.plotly_chart(fig_year_valor, use_container_width=True)

with tab3:
    st.subheader("Análisis por Programa")
    
    if dataset_type == 'military':
        st.info("📊 El dataset Militar/Policía no contiene información por programa. Mostrando análisis por tipo de vivienda.")
        
        housing_summary = df_filtered[['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis']].sum()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_housing_pie = px.pie(
                values=housing_summary.values,
                names=['Nueva VIS', 'Nueva No VIS', 'Usada VIS', 'Usada No VIS'],
                title='Distribución por Tipo de Vivienda',
                hole=0.4
            )
            st.plotly_chart(fig_housing_pie, use_container_width=True)
        
        with col2:
            fig_housing_bar = px.bar(
                x=housing_summary.values,
                y=housing_summary.index,
                orientation='h',
                title='Subsidios por Tipo de Vivienda',
                labels={'x': 'Cantidad', 'y': 'Tipo'},
                color=housing_summary.values,
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig_housing_bar, use_container_width=True)
    elif dataset_type == 'used_housing':
        st.info("📊 El dataset Vivienda Usada no contiene información por programa. Mostrando análisis por fuerza militar.")
        
        force_cols = {
            'vis_policia_nacional': 'Policía Nacional',
            'vis_ejercito_nacional': 'Ejército Nacional',
            'vis_fuerza_aerea_colombiana': 'Fuerza Aérea',
            'vis_armada_nacional': 'Armada Nacional',
            'vis_otros': 'Otros',
            'no_vis_policia_nacional': 'Policía Nacional (No VIS)',
            'no_vis_ejercito_nacional': 'Ejército Nacional (No VIS)',
            'no_vis_fuerza_aerea_colombiana': 'Fuerza Aérea (No VIS)',
            'no_vis_armada_nacional': 'Armada Nacional (No VIS)',
            'no_vis_otros': 'Otros (No VIS)'
        }
        
        force_data = {}
        for col, label in force_cols.items():
            if col in df_filtered.columns:
                force_data[label] = df_filtered[col].sum()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_force_pie = px.pie(
                values=list(force_data.values()),
                names=list(force_data.keys()),
                title='Distribución por Fuerza Militar',
                hole=0.4
            )
            st.plotly_chart(fig_force_pie, use_container_width=True)
        
        with col2:
            fig_force_bar = px.bar(
                x=list(force_data.values()),
                y=list(force_data.keys()),
                orientation='h',
                title='Subsidios por Fuerza Militar',
                labels={'x': 'Cantidad', 'y': 'Fuerza'},
                color=list(force_data.values()),
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig_force_bar, use_container_width=True)
    elif 'programa' in df_filtered.columns:
        if dataset_type in ['rural', 'cmc_mcy']:
            beneficiary_col = 'no_sfv_asignados'
            beneficiary_label = 'SFV Asignados'
        else:
            beneficiary_col = 'hogares'
            beneficiary_label = 'Hogares'
        
        prog_data = df_filtered.groupby('programa').agg({
            beneficiary_col: 'sum',
            'valor_asignado': 'sum'
        }).reset_index().sort_values(beneficiary_col, ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_prog_pie = px.pie(
                prog_data,
                values=beneficiary_col,
                names='programa',
                title=f'Distribución de {beneficiary_label} por Programa',
                hole=0.4
            )
            st.plotly_chart(fig_prog_pie, use_container_width=True)
        
        with col2:
            fig_prog_bar = px.bar(
                prog_data,
                x='programa',
                y='valor_asignado',
                title='Valor Asignado por Programa',
                labels={'programa': 'Programa', 'valor_asignado': 'Valor Asignado (COP)'},
                color='valor_asignado',
                color_continuous_scale='Viridis'
            )
            fig_prog_bar.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_prog_bar, use_container_width=True)

with tab4:
    st.subheader("📊 Analítica Avanzada")
    
    if dataset_type in ['military', 'used_housing']:
        beneficiary_col = 'total_subsidios'
        beneficiary_label = 'Total Subsidios'
        year_col = 'ano'
    elif dataset_type in ['rural', 'cmc_mcy']:
        beneficiary_col = 'no_sfv_asignados'
        beneficiary_label = 'SFV Asignados'
        year_col = 'ano_de_asignacion'
    else:
        beneficiary_col = 'hogares'
        beneficiary_label = 'Hogares'
        year_col = 'a_o_de_asignaci_n'
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🎯 Concentración Geográfica")
        if 'departamento' in df_filtered.columns and beneficiary_col in df_filtered.columns:
            top_5_dept = df_filtered.groupby('departamento')[beneficiary_col].sum().nlargest(5)
            total_beneficiaries = df_filtered[beneficiary_col].sum()
            concentration = (top_5_dept.sum() / total_beneficiaries * 100) if total_beneficiaries > 0 else 0
            
            st.metric("Concentración Top 5 Departamentos", f"{concentration:.1f}%")
            
            fig_concentration = px.bar(
                x=top_5_dept.values,
                y=top_5_dept.index,
                orientation='h',
                title='Top 5 Departamentos',
                labels={'x': beneficiary_label, 'y': 'Departamento'},
                color=top_5_dept.values,
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig_concentration, use_container_width=True)
    
    with col2:
        if dataset_type == 'military':
            st.markdown("### 🏘️ Distribución VIS vs No VIS")
            if all(col in df_filtered.columns for col in ['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis']):
                total_vis = df_filtered[['nueva_vis', 'usada_vis']].sum().sum()
                total_no_vis = df_filtered[['nueva_no_vis', 'usada_no_vis']].sum().sum()
                total = total_vis + total_no_vis
                
                if total > 0:
                    vis_pct = (total_vis / total * 100)
                    st.metric("% VIS", f"{vis_pct:.1f}%")
                    
                    fig_vis_comparison = px.pie(
                        values=[total_vis, total_no_vis],
                        names=['VIS', 'No VIS'],
                        title='Distribución VIS vs No VIS',
                        color_discrete_sequence=['#1f4788', '#28a745']
                    )
                    st.plotly_chart(fig_vis_comparison, use_container_width=True)
        else:
            st.markdown("### 💰 Valor Promedio por Beneficiario")
            if beneficiary_col in df_filtered.columns and 'valor_asignado' in df_filtered.columns:
                total_valor = df_filtered['valor_asignado'].sum()
                total_beneficiaries = df_filtered[beneficiary_col].sum()
                avg_value = total_valor / total_beneficiaries if total_beneficiaries > 0 else 0
                
                st.metric("Valor Promedio", format_currency(avg_value))
                
                if 'departamento' in df_filtered.columns:
                    dept_avg = df_filtered.groupby('departamento').apply(
                        lambda x: x['valor_asignado'].sum() / x[beneficiary_col].sum() if x[beneficiary_col].sum() > 0 else 0
                    ).nlargest(10)
                    
                    fig_avg = px.bar(
                        x=dept_avg.values,
                        y=dept_avg.index,
                        orientation='h',
                        title='Top 10 Departamentos por Valor Promedio',
                        labels={'x': 'Valor Promedio (COP)', 'y': 'Departamento'},
                        color=dept_avg.values,
                        color_continuous_scale='Greens'
                    )
                    st.plotly_chart(fig_avg, use_container_width=True)
    
    st.markdown("---")
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown("### 📈 Tendencia de Crecimiento")
        if year_col in df_filtered.columns and beneficiary_col in df_filtered.columns:
            yearly_trend = df_filtered.groupby(year_col)[beneficiary_col].sum().sort_index()
            
            if len(yearly_trend) > 1:
                growth_rate = ((yearly_trend.iloc[-1] - yearly_trend.iloc[0]) / yearly_trend.iloc[0] * 100) if yearly_trend.iloc[0] > 0 else 0
                st.metric("Crecimiento Total", f"{growth_rate:.1f}%")
                
                fig_trend = px.area(
                    x=yearly_trend.index,
                    y=yearly_trend.values,
                    title='Tendencia Anual',
                    labels={'x': 'Año', 'y': beneficiary_label}
                )
                fig_trend.update_traces(fillcolor='rgba(31, 71, 136, 0.3)', line_color='#1f4788')
                st.plotly_chart(fig_trend, use_container_width=True)
    
    with col4:
        if dataset_type == 'military':
            st.markdown("### � Nueva vs Usada")
            if all(col in df_filtered.columns for col in ['nueva_vis', 'nueva_no_vis', 'usada_vis', 'usada_no_vis']):
                total_nueva = df_filtered[['nueva_vis', 'nueva_no_vis']].sum().sum()
                total_usada = df_filtered[['usada_vis', 'usada_no_vis']].sum().sum()
                total = total_nueva + total_usada
                
                if total > 0:
                    nueva_pct = (total_nueva / total * 100)
                    st.metric("% Nueva", f"{nueva_pct:.1f}%")
                    
                    fig_nueva_usada = px.pie(
                        values=[total_nueva, total_usada],
                        names=['Nueva', 'Usada'],
                        title='Distribución Nueva vs Usada',
                        color_discrete_sequence=['#ff6b6b', '#4ecdc4']
                    )
                    st.plotly_chart(fig_nueva_usada, use_container_width=True)
        else:
            st.markdown("### �️ Distribución por Programa")
            if 'programa' in df_filtered.columns and 'valor_asignado' in df_filtered.columns:
                prog_summary = df_filtered.groupby('programa').agg({
                    beneficiary_col: 'sum',
                    'valor_asignado': 'sum'
                }).reset_index()
                
                prog_summary['avg_value'] = prog_summary['valor_asignado'] / prog_summary[beneficiary_col]
                prog_summary = prog_summary.sort_values('avg_value', ascending=False)
                
                fig_prog_scatter = px.scatter(
                    prog_summary,
                    x=beneficiary_col,
                    y='valor_asignado',
                    size='avg_value',
                    color='programa',
                    title='Programas: Beneficiarios vs Valor Total',
                    labels={
                        beneficiary_col: beneficiary_label,
                        'valor_asignado': 'Valor Total (COP)',
                        'avg_value': 'Valor Promedio'
                    },
                    hover_data=['avg_value']
                )
                st.plotly_chart(fig_prog_scatter, use_container_width=True)
    
    st.markdown("---")
    
    if dataset_type == 'military':
        st.markdown("### � Análisis por Trimestre")
        if 'trimestre' in df_filtered.columns and 'total_subsidios' in df_filtered.columns:
            trimestre_data = df_filtered.groupby('trimestre')['total_subsidios'].sum().reset_index()
            
            fig_trimestre = px.bar(
                trimestre_data,
                x='trimestre',
                y='total_subsidios',
                title='Subsidios por Trimestre',
                labels={'trimestre': 'Trimestre', 'total_subsidios': 'Total Subsidios'},
                color='total_subsidios',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_trimestre, use_container_width=True)
    else:
        st.markdown("### ��️ Mapa de Calor: Municipios")
        if 'municipio' in df_filtered.columns and beneficiary_col in df_filtered.columns:
            muni_data = df_filtered.groupby('municipio').agg({
                beneficiary_col: 'sum',
                'valor_asignado': 'sum'
            }).reset_index().sort_values(beneficiary_col, ascending=False).head(30)
            
            fig_heatmap = px.density_heatmap(
                df_filtered.head(1000),
                x='departamento',
                y='municipio',
                z=beneficiary_col,
                title='Distribución de Subsidios por Departamento y Municipio (Top 1000 registros)',
                color_continuous_scale='YlOrRd'
            )
            fig_heatmap.update_layout(height=500)
            st.plotly_chart(fig_heatmap, use_container_width=True)

with tab5:
    st.subheader("Tabla de Datos Detallados")
    
    if dataset_type == 'general':
        format_dict = {
            'hogares': '{:,.0f}',
            'valor_asignado': '${:,.0f}',
            'a_o_de_asignaci_n': '{:.0f}'
        } if all(col in df_filtered.columns for col in ['hogares', 'valor_asignado', 'a_o_de_asignaci_n']) else {}
    elif dataset_type in ['rural', 'cmc_mcy']:
        format_dict = {
            'no_sfv_asignados': '{:,.0f}',
            'valor_asignado': '${:,.0f}',
            'ano_de_asignacion': '{:.0f}'
        } if all(col in df_filtered.columns for col in ['no_sfv_asignados', 'valor_asignado', 'ano_de_asignacion']) else {}
    elif dataset_type == 'military':
        format_dict = {
            'ano': '{:.0f}',
            'trimestre': '{:.0f}',
            'nueva_vis': '{:,.0f}',
            'nueva_no_vis': '{:,.0f}',
            'usada_vis': '{:,.0f}',
            'usada_no_vis': '{:,.0f}',
            'total_subsidios': '{:,.0f}'
        } if all(col in df_filtered.columns for col in ['ano', 'trimestre']) else {}
    else:
        format_dict = {
            'ano': '{:.0f}',
            'trimestre': '{:.0f}',
            'total_vis': '{:,.0f}',
            'total_no_vis': '{:,.0f}',
            'total_subsidios': '{:,.0f}'
        } if all(col in df_filtered.columns for col in ['ano', 'trimestre']) else {}

    if format_dict and len(df_filtered) > 0:
        try:
            st.dataframe(
                df_filtered.style.format(format_dict, na_rep='-'),
                use_container_width=True,
                height=500
            )
        except:
            st.dataframe(
                df_filtered,
                use_container_width=True,
                height=500
            )
    else:
        st.dataframe(
            df_filtered,
            use_container_width=True,
            height=500
        )
    
    if dataset_type == 'military':
        file_prefix = 'subsidios_vivienda_militar_policia'
    elif dataset_type == 'rural':
        file_prefix = 'subsidios_vivienda_rural'
    elif dataset_type == 'cmc_mcy':
        file_prefix = 'subsidios_mejoramiento_cmc_mcy'
    elif dataset_type == 'used_housing':
        file_prefix = 'subsidios_vivienda_usada'
    else:
        file_prefix = 'subsidios_vivienda_general'
    
    st.download_button(
        label="📥 Descargar datos filtrados (CSV)",
        data=df_filtered.to_csv(index=False).encode('utf-8'),
        file_name=f'{file_prefix}_{datetime.now().strftime("%Y%m%d")}.csv',
        mime='text/csv'
    )

st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p><strong>Camacol</strong> - Cámara Colombiana de la Construcción</p>
        <p>Fuente: Ministerio de Vivienda, Ciudad y Territorio - Datos Abiertos Colombia</p>
        <p>Última actualización: {}</p>
    </div>
""".format(datetime.now().strftime("%d/%m/%Y %H:%M")), unsafe_allow_html=True)
