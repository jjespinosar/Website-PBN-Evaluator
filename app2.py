import streamlit as st
import pandas as pd
import io
import base64
from datetime import datetime
import math
import warnings
warnings.filterwarnings('ignore')

# =========================================================
# CONFIGURACIÓN Y FUNCIONES BASE (del Script Original)
# =========================================================

BATCH_LIMIT = 100 
DISPLAY_MAX_ROWS = 500
WHITELIST_DOMAINS = [
    'kommo.com', 'amocrm.com', 'hubspot.com', 'salesforce.com',
    'zoho.com', 'microsoft.com', 'google.com', 'facebook.com',
    'linkedin.com', 'twitter.com', 'instagram.com', 'zapier.com', 
    'notion.so', 'notion.com', 'slack.com', 'asana.com',
    'trello.com', 'calendly.com', 'airtable.com', 'medium.com',
    'wix.com', 'wordpress.com', 'github.com', 'gitlab.com', 
    'stripe.com', 'paypal.com', 'tableau.com', 'intercom.com', 
    'canva.com'
]

# --- LÓGICA DE ANÁLISIS ---
# NOTA: Esta función DEBE ser reemplazada con la lógica completa
# de tu script original de Colab (chequeos, scoring, etc.).
def analizar_datos(df):
    """
    Función que simula la lógica del script de Colab (Máximo 100 dominios).
    """
    if df.empty:
        return pd.DataFrame()
    
    # ⚠️ APLICANDO LÍMITE ORIGINAL DE 100 DOMINIOS
    if len(df) > BATCH_LIMIT:
        st.warning(f"⚠️ ATENCIÓN: El análisis está LIMITADO a los primeros {BATCH_LIMIT} dominios, según la configuración del script original.")
        df = df.head(BATCH_LIMIT)
    
    st.info(f"Analizando {len(df)} filas.")

    # ----------------------------------------------------
    # SIMULACIÓN DE RESULTADOS (REEMPLAZAR CON TU LÓGICA REAL)
    # ----------------------------------------------------
    if 'Dominio' not in df.columns:
        df['Dominio'] = df.iloc[:, 0] # Usar la primera columna como dominio si no existe
        
    df['Evaluacion_PBN'] = 'TRUST ALTO'
    df.loc[df.index % 5 == 0, 'Evaluacion_PBN'] = 'RIESGO ALTO'
    df.loc[df.index % 3 == 0, 'Evaluacion_PBN'] = 'RIESGO MODERADO'
    df['Detalles_Evaluacion'] = 'Lógica de análisis PBN a implementar'
    
    return df.head(DISPLAY_MAX_ROWS)

# --- Funciones de Descarga ---
@st.cache_data
def get_table_download_link(df, file_format):
    """Genera un enlace de descarga para el DataFrame."""
    output = io.BytesIO()
    
    if file_format == 'Excel':
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Resultados_PBN_Eval')
        b64 = base64.b64encode(output.getvalue()).decode()
        return f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="evaluacion_pbn_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx">📥 Descargar Excel (.xlsx)</a>'
    
    elif file_format == 'CSV':
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        return f'<a href="data:file/csv;base64,{b64}" download="evaluacion_pbn_{datetime.now().strftime("%Y%m%d_%H%M")}.csv">📥 Descargar CSV (.csv)</a>'

# =========================================================
# INTERFAZ DE STREAMLIT (IMITANDO FLUJO DE COLAB)
# =========================================================

# Inicializa el estado para el control de flujo
if 'df_original_app2' not in st.session_state:
    st.session_state.df_original_app2 = None
if 'analysis_run_app2' not in st.session_state:
    st.session_state.analysis_run_app2 = False


def main_app2():
    st.set_page_config(layout="wide", page_title="Website Evaluation + PBN Tool")
    
    st.markdown("<h2>🌐 Website Evaluation Tool + Detección PBN</h2>", unsafe_allow_html=True)
    st.markdown("---")

    st.subheader("Paso 1: Cargar Archivo")
    
    # st.file_uploader reemplaza a files.upload()
    uploaded_file = st.file_uploader(
        "Sube tu archivo de dominios para la evaluación (Excel o CSV)", 
        type=['xlsx', 'csv'], 
        key='uploaded_file_app2' # Key específico para este proyecto
    )

    df_original = None
    
    if uploaded_file is not None:
        try:
            uploaded_file.seek(0)
            if uploaded_file.name.endswith('.xlsx'):
                df_original = pd.read_excel(uploaded_file)
            else: 
                df_original = pd.read_csv(uploaded_file)
            
            st.session_state.df_original_app2 = df_original
            st.success(f"Archivo cargado con éxito: {len(df_original)} filas. Listo para evaluar.")

        except Exception as e:
            st.error(f"Error al cargar o leer el archivo. ¿Es el formato correcto? Error: {e}")
            st.session_state.df_original_app2 = None
            st.session_state.analysis_run_app2 = False
    
    # --- BOTONES DE ACCIÓN (Reemplazan ipywidgets) ---
    st.subheader("Paso 2: Evaluar y Reiniciar")
    
    # Crear dos columnas para alinear los botones
    col_evaluar, col_reiniciar = st.columns([1, 1])
    
    with col_evaluar:
        # El botón de 'cargar_y_evaluar'
        eval_button = st.button(
            "📂 Evaluar Archivo", 
            type="primary",
            disabled=(st.session_state.df_original_app2 is None), # Desactivado si no hay archivo
            key='eval_button_app2'
        )
        
    with col_reiniciar:
        # El botón de 'reiniciar'
        reset_button = st.button(
            "🔄 Reiniciar", 
            type="secondary",
            key='reset_button_app2'
        )
        
    # LÓGICA DE CONTROL DE FLUJO
    if eval_button:
        # Al pulsar 'Evaluar', se establece el estado a True
        st.session_state.analysis_run_app2 = True
    
    if reset_button:
        # Al pulsar 'Reiniciar', se resetea el estado y se recarga la página
        st.session_state.df_original_app2 = None
        st.session_state.analysis_run_app2 = False
        st.rerun() # Fuerza el reinicio completo

    # 3. Mostrar Resultados solo si se pulsó 'Evaluar'
    if st.session_state.analysis_run_app2 and st.session_state.df_original_app2 is not None:
        st.markdown("---")
        st.header("✅ Resultados de la Evaluación PBN")

        with st.spinner('Ejecutando la lógica de análisis...'):
            df_resultados = analizar_datos(st.session_state.df_original_app2.copy())
        
        if not df_resultados.empty:
            riesgo_alto = len(df_resultados[df_resultados['Evaluacion_PBN'] == 'RIESGO ALTO'])
            trust_alto = len(df_resultados[df_resultados['Evaluacion_PBN'] == 'TRUST ALTO'])
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Dominios Analizados", len(df_resultados))
            col2.metric("Riesgo ALTO", riesgo_alto)
            col3.metric("TRUST ALTO", trust_alto)
            
            st.dataframe(df_resultados, use_container_width=True, height=350)

            st.markdown("---")
            st.subheader("Paso 3: Descargar Resultados")
            
            # Botones de descarga (reemplazan los botones de ipywidgets)
            col_dl_excel, col_dl_csv = st.columns(2)
            
            with col_dl_excel:
                 st.markdown(get_table_download_link(df_resultados, 'Excel'), unsafe_allow_html=True)
            with col_dl_csv:
                 st.markdown(get_table_download_link(df_resultados, 'CSV'), unsafe_allow_html=True)


if __name__ == '__main__':
    main_app2()
