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

@st.cache_data(ttl=3600)
def fetch_all_data():
    """
    Fetch all data from the API with pagination handling.
    The API has a default limit of 1000 rows, so we fetch in batches of 900.
    """
    base_url = "https://www.datos.gov.co/resource/h2yr-zfb2.json"
    all_data = []
    offset = 0
    limit = 900
    
    with st.spinner('Cargando datos del Ministerio de Vivienda...'):
        while True:
            try:
                params = {
                    "$limit": limit,
                    "$offset": offset
                }
                response = requests.get(base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                
                all_data.extend(data)
                offset += limit
                
                if len(data) < limit:
                    break
                    
            except requests.exceptions.RequestException as e:
                st.error(f"Error al cargar datos: {str(e)}")
                break
    
    if all_data:
        df = pd.DataFrame(all_data)
        
        if 'hogares' in df.columns:
            df['hogares'] = pd.to_numeric(df['hogares'], errors='coerce')
        if 'valor_asignado' in df.columns:
            df['valor_asignado'] = pd.to_numeric(df['valor_asignado'], errors='coerce')
        if 'a_o_de_asignaci_n' in df.columns:
            df['a_o_de_asignaci_n'] = pd.to_numeric(df['a_o_de_asignaci_n'], errors='coerce')
        
        return df
    
    return pd.DataFrame()

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

df = fetch_all_data()

if df.empty:
    st.error("No se pudieron cargar los datos. Por favor, intente nuevamente.")
    st.stop()

st.success(f"✅ Datos cargados exitosamente: {len(df):,} registros")

st.sidebar.header("🔍 Filtros")

if 'departamento' in df.columns:
    departamentos = ['Todos'] + sorted(df['departamento'].dropna().unique().tolist())
    selected_dept = st.sidebar.selectbox("Departamento", departamentos)
else:
    selected_dept = 'Todos'

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

if 'a_o_de_asignaci_n' in df.columns:
    years = sorted(df['a_o_de_asignaci_n'].dropna().unique())
    if len(years) > 0:
        selected_years = st.sidebar.slider(
            "Año de Asignación",
            min_value=int(min(years)),
            max_value=int(max(years)),
            value=(int(min(years)), int(max(years)))
        )
    else:
        selected_years = None
else:
    selected_years = None

if 'estado_de_postulaci_n' in df.columns:
    estados = ['Todos'] + sorted(df['estado_de_postulaci_n'].dropna().unique().tolist())
    selected_estado = st.sidebar.selectbox("Estado de Postulación", estados)
else:
    selected_estado = 'Todos'

df_filtered = df.copy()

if selected_dept != 'Todos' and 'departamento' in df.columns:
    df_filtered = df_filtered[df_filtered['departamento'] == selected_dept]

if selected_muni != 'Todos' and 'municipio' in df.columns:
    df_filtered = df_filtered[df_filtered['municipio'] == selected_muni]

if selected_prog != 'Todos' and 'programa' in df.columns:
    df_filtered = df_filtered[df_filtered['programa'] == selected_prog]

if selected_years and 'a_o_de_asignaci_n' in df.columns:
    df_filtered = df_filtered[
        (df_filtered['a_o_de_asignaci_n'] >= selected_years[0]) &
        (df_filtered['a_o_de_asignaci_n'] <= selected_years[1])
    ]

if selected_estado != 'Todos' and 'estado_de_postulaci_n' in df.columns:
    df_filtered = df_filtered[df_filtered['estado_de_postulaci_n'] == selected_estado]

st.header("📊 Indicadores Principales")

col1, col2, col3, col4 = st.columns(4)

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

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Por Departamento", "📅 Por Año", "🏘️ Por Programa", "📋 Datos Detallados"])

with tab1:
    st.subheader("Subsidios por Departamento")
    
    if 'departamento' in df_filtered.columns and 'hogares' in df_filtered.columns:
        dept_data = df_filtered.groupby('departamento').agg({
            'hogares': 'sum',
            'valor_asignado': 'sum'
        }).reset_index().sort_values('hogares', ascending=False).head(20)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_dept_hogares = px.bar(
                dept_data,
                x='hogares',
                y='departamento',
                orientation='h',
                title='Top 20 Departamentos por Hogares Beneficiados',
                labels={'hogares': 'Hogares', 'departamento': 'Departamento'},
                color='hogares',
                color_continuous_scale='Blues'
            )
            fig_dept_hogares.update_layout(height=600)
            st.plotly_chart(fig_dept_hogares, use_container_width=True)
        
        with col2:
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
    
    if 'a_o_de_asignaci_n' in df_filtered.columns:
        year_data = df_filtered.groupby('a_o_de_asignaci_n').agg({
            'hogares': 'sum',
            'valor_asignado': 'sum'
        }).reset_index().sort_values('a_o_de_asignaci_n')
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_year_hogares = px.line(
                year_data,
                x='a_o_de_asignaci_n',
                y='hogares',
                title='Hogares Beneficiados por Año',
                labels={'a_o_de_asignaci_n': 'Año', 'hogares': 'Hogares'},
                markers=True
            )
            fig_year_hogares.update_traces(line_color='#1f4788', line_width=3)
            st.plotly_chart(fig_year_hogares, use_container_width=True)
        
        with col2:
            fig_year_valor = px.line(
                year_data,
                x='a_o_de_asignaci_n',
                y='valor_asignado',
                title='Valor Asignado por Año',
                labels={'a_o_de_asignaci_n': 'Año', 'valor_asignado': 'Valor Asignado (COP)'},
                markers=True
            )
            fig_year_valor.update_traces(line_color='#28a745', line_width=3)
            st.plotly_chart(fig_year_valor, use_container_width=True)

with tab3:
    st.subheader("Análisis por Programa")
    
    if 'programa' in df_filtered.columns:
        prog_data = df_filtered.groupby('programa').agg({
            'hogares': 'sum',
            'valor_asignado': 'sum'
        }).reset_index().sort_values('hogares', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_prog_pie = px.pie(
                prog_data,
                values='hogares',
                names='programa',
                title='Distribución de Hogares por Programa',
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
            fig_prog_bar.update_xaxis(tickangle=-45)
            st.plotly_chart(fig_prog_bar, use_container_width=True)

with tab4:
    st.subheader("Tabla de Datos Detallados")
    
    st.dataframe(
        df_filtered.style.format({
            'hogares': '{:,.0f}',
            'valor_asignado': '${:,.0f}',
            'a_o_de_asignaci_n': '{:.0f}'
        } if all(col in df_filtered.columns for col in ['hogares', 'valor_asignado', 'a_o_de_asignaci_n']) else {}),
        use_container_width=True,
        height=500
    )
    
    st.download_button(
        label="📥 Descargar datos filtrados (CSV)",
        data=df_filtered.to_csv(index=False).encode('utf-8'),
        file_name=f'subsidios_vivienda_{datetime.now().strftime("%Y%m%d")}.csv',
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

