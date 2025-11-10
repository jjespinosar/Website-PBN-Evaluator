import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import math
import warnings
warnings.filterwarnings('ignore')

# =========================================================
# CONFIGURACIÓN Y FUNCIONES DEL ANÁLISIS (PORTADO DE COLAB)
# =========================================================

# --- CONFIGURACIÓN Y CONSTANTES ---
BASE_CONFIG = {
    'alto_riesgo': 8,
    'riesgo_moderado': 5,
    'riesgo_bajo': 3,
    'max_puntos': 17, # Máximo total de penalización
}

MARCAS_VERIFICADAS = [
    'zendesk', 'hubspot', 'salesforce', 'shopify', 'wordpress', 'microsoft',
    'google', 'facebook', 'linkedin', 'twitter', 'instagram', 'youtube',
    'amazon', 'apple', 'adobe', 'mailchimp', 'semrush', 'ahrefs', 'moz',
    'github', 'stackoverflow', 'wikipedia', 'reddit', 'quora', 'medium', 'notion', 'slack', 'trello', 'asana', 'canva'
]
# --- FUNCIONES DE AJUSTE DINÁMICO ---
def ajustar_configuracion(level):
    """Ajusta los umbrales y pesos según el nivel de flexibilidad (1-10)."""
    
    # Factor de flexibilidad: 0.0 (Nivel 1, Máx. Estricto) a 1.0 (Nivel 10, Máx. Flexible)
    flexible_factor = (level - 1) / 9.0 
    
    config = BASE_CONFIG.copy()

    config['umbrales'] = {
        # Link Building
        # Rango (15 -> 3)
        'min_refdomains_analysis': int(15 - (12 * flexible_factor)),
        # Rango (10 -> 25)
        'spam_density_threshold': 10 + (15 * flexible_factor),
        # Rango (25 -> 50)
        'high_density_threshold': 25 + (25 * flexible_factor),
        # Rango (0.10 -> 0.25)
        'low_ip_diversity': 0.10 + (0.15 * flexible_factor),
        # Rango (0.05 -> 0.13)
        'very_low_ip_diversity': 0.05 + (0.08 * flexible_factor),
        # Rango (0.94 -> 0.98)
        'excessive_dofollow': 0.94 + (0.04 * flexible_factor),
        # Rango (0.98 -> 0.99)
        'very_excessive_dofollow': 0.98 + (0.01 * flexible_factor),
        # Rango (0.05 -> 0.20)
        'min_brand_anchor_ratio': 0.05 + (0.15 * flexible_factor),

        # Edad y Crecimiento
        # Rango (8 -> 20)
        'suspicious_growth_monthly': 8 + (12 * flexible_factor),
        # Rango (15 -> 40)
        'very_suspicious_growth': 15 + (25 * flexible_factor),
        # Rango (0.5 -> 1.5)
        'young_domain_high_dr': 0.5 + (1.0 * flexible_factor),
        # Rango (0.2 -> 0.7)
        'very_young_domain': 0.2 + (0.5 * flexible_factor),

        # Calidad y Autoridad
        # Rango (0.20 -> 0.35)
        'dr_traffic_discrepancy': 0.20 + (0.15 * flexible_factor),
        # Rango (0.3 -> 1.0)
        'low_traffic_per_keyword': 0.3 + (0.7 * flexible_factor),
        # Rango (1 -> 3)
        'spam_pattern_count': int(1 + (2 * flexible_factor)), 
    }

    config['positive_signals'] = {
        # Rango (2000 -> 7000)
        'high_quality_traffic': int(2000 + (5000 * flexible_factor)),
        # Rango (0.4 -> 0.7)
        'diverse_ip_profile': 0.4 + (0.3 * flexible_factor),
        # Rango (1 -> 4)
        'established_domain_age': 1 + (3 * flexible_factor),
        # Rango (0.15 -> 0.35)
        'branded_anchors_ratio': 0.15 + (0.20 * flexible_factor),
        # Rango (0.005 -> 0.030)
        'authority_tlds_presence': 0.005 + (0.025 * flexible_factor)
    }

    config['factores'] = {
        # Penalización por Densidad: Rango (4 -> 2)
        'densidad_links': int(4 - (2 * flexible_factor)), 
        # Penalización por IP: Rango (4 -> 2)
        'diversidad_ips': int(4 - (2 * flexible_factor)),
        # Penalización por Calidad: Rango (3 -> 1)
        'calidad_dominios': int(3 - (2 * flexible_factor)), 
        # Penalización por Edad: Rango (3 -> 1)
        'edad_dominio': int(3 - (2 * flexible_factor)), 
        # Penalización por Anchor: Rango (3 -> 1)
        'anchor_text': int(3 - (2 * flexible_factor)), 
        # Penalización por Crecimiento: Rango (3 -> 1)
        'perfil_crecimiento': int(3 - (2 * flexible_factor)),
        # Penalización por Autoridad: Rango (3 -> 1)
        'autoridad_trafico': int(3 - (2 * flexible_factor)), 
        # Penalización por TLDs: Rango (2 -> 1)
        'tlds_autoridad': int(2 - (1 * flexible_factor)) 
    }

    config['patrones_spam'] = [
        'review', 'best', 'top', 'buy', 'cheap', 'discount', 'offer',
        'blog', 'news', 'hub', 'center', 'network', 'express', 'online',
        'guide', 'tips', 'advice', 'solution', 'service', 'shop', 'store'
    ]
    
    config['marcas_verificadas'] = MARCAS_VERIFICADAS
    return config

# --- FUNCIONES AUXILIARES DE LIMPIEZA Y DATOS ---

def find_col(cols, candidates):
    """Busca una columna siendo tolerante a mayúsculas/minúsculas y espacios."""
    lc = {c.lower().strip(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    for cand in candidates:
        for k, v in lc.items():
            if cand.lower() in k:
                return v 
    return None

def calcular_edad_dominio_meses(age_value):
    """Convierte la edad/fecha de creación a meses, manteniendo la lógica original."""
    if pd.isna(age_value) or age_value == 0:
        return 0.0
    try:
        x = float(age_value)
        current_year = pd.Timestamp.now().year
        if 0 < x < 50:
            return x * 12.0
        elif 1970 <= x <= current_year + 1:
            return max(0, current_year - x) * 12.0
        else:
            return x
    except ValueError:
        try:
            date = pd.to_datetime(age_value, errors='coerce')
            if pd.notna(date):
                age_days = (pd.Timestamp.now() - date).days
                return max(0, age_days / 30.437) # Meses
        except Exception:
            pass
    return 0.0

def es_marca_verificada(dominio, config):
    """Verifica si el dominio es una marca conocida."""
    dominio_clean = str(dominio).lower().replace('www.', '').split('.')[0]
    for marca in config['marcas_verificadas']:
        if marca in dominio_clean:
            return True
    return False

# --- CHEQUEOS DE PENALIZACIÓN MODULARES (11 PUNTOS) ---

def _check_density(data, config, es_marca):
    """1. Densidad de Backlinks (Backlinks/RefDomains)"""
    refdomains, backlinks = data['refdomains_all'], data['backlinks_all']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if backlinks > 0 and refdomains > 0:
        densidad = backlinks / refdomains
        factor_marca = 0.7 if es_marca else 1.0
        umbral_densidad = umbral['spam_density_threshold'] * factor_marca
        umbral_alta = umbral['high_density_threshold'] * factor_marca
        if densidad > umbral_alta:
            puntos += config['factores']['densidad_links']
            factores.append(f"Densidad ALTA ({densidad:.1f} links/dominio)")
        elif densidad > umbral_densidad:
            puntos += max(1, config['factores']['densidad_links'] - 1)
            factores.append(f"Densidad moderada ({densidad:.1f} links/dominio)")
    return puntos, factores

def _check_diversity(data, config, es_marca):
    """2. Diversidad de IPs/Subredes"""
    refip_div = data['RefIP_Diversidad']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if refip_div > 0:
        factor_marca = 1.2 if es_marca else 1.0
        umbral_baja = umbral['low_ip_diversity'] * factor_marca
        umbral_muy_baja = umbral['very_low_ip_diversity'] * factor_marca
        if refip_div < umbral_muy_baja:
            puntos += config['factores']['diversidad_ips']
            factores.append(f"IPs MUY BAJA diversidad ({refip_div:.3f})")
        elif refip_div < umbral_baja:
            puntos += max(1, config['factores']['diversidad_ips'] - 1)
            factores.append(f"IPs baja diversidad ({refip_div:.3f})")
    return puntos, factores

def _check_growth(data, config, es_marca):
    """3. Crecimiento de Dominios (Dominios/Mes)"""
    domains_per_month = data['domains_per_month']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if not math.isinf(domains_per_month) and domains_per_month > 0:
        factor_crecimiento = 1.3 if es_marca else 1.0
        if domains_per_month > (umbral['very_suspicious_growth'] * factor_crecimiento):
            puntos += config['factores']['perfil_crecimiento']
            factores.append(f"Crecimiento MUY SOSPECHOSO ({domains_per_month:.1f} dom/mes)")
        elif domains_per_month > (umbral['suspicious_growth_monthly'] * factor_crecimiento):
            puntos += max(1, config['factores']['perfil_crecimiento'] - 1)
            factores.append(f"Crecimiento sospechoso ({domains_per_month:.1f} dom/mes)")
    return puntos, factores

def _check_traffic_vs_authority(data, config, es_marca):
    """4. Discrepancia Autoridad (DR/DA) vs Tráfico"""
    dr, organic_traffic = data['dr'], data['organic_traffic']
    puntos, factores = 0, []
    if dr > 45 and organic_traffic > 0:
        traffic_threshold = max(300, dr * 8)
        threshold_marca = traffic_threshold * 1.5 if es_marca else traffic_threshold
        if organic_traffic < threshold_marca:
            puntos += config['factores']['autoridad_trafico']
            factores.append(f"DR alto ({dr}) vs tráfico bajo ({organic_traffic})")
    return puntos, factores

def _check_dofollow_ratio(data, config):
    """5. Ratio Dofollow (Demasiados links Dofollow)"""
    pct_bl_followed, refdomains = data['Pct_Backlinks_Followed'], data['refdomains_all']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if pct_bl_followed >= 0:
        if pct_bl_followed > umbral['very_excessive_dofollow'] and refdomains > 25:
            puntos += config['factores']['anchor_text']
            factores.append(f"Dofollow CASI TOTAL ({pct_bl_followed*100:.1f}%)")
        elif pct_bl_followed > umbral['excessive_dofollow'] and refdomains > 15:
            puntos += max(1, config['factores']['anchor_text'] - 1)
            factores.append(f"Dofollow muy alto ({pct_bl_followed*100:.1f}%)")
    return puntos, factores

def _check_age_vs_authority(data, config, es_marca):
    """6. Edad del Dominio vs Autoridad Rápida"""
    domain_age_years, dr = data['domain_age_months'] / 12.0, data['dr']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if domain_age_years > 0:
        factor_edad = 0.8 if es_marca else 1.0
        if domain_age_years < (umbral['very_young_domain'] * factor_edad) and dr > 45:
            puntos += config['factores']['edad_dominio']
            factores.append(f"Dominio MUY NUEVO ({domain_age_years:.1f}a) con DR alto ({dr})")
        elif domain_age_years < (umbral['young_domain_high_dr'] * factor_edad) and dr > 55:
            puntos += max(1, config['factores']['edad_dominio'] - 1)
            factores.append(f"Dominio nuevo ({domain_age_years:.1f}a) con DR alto ({dr})")
    return puntos, factores

def _check_ur_vs_dr(data, config):
    """7. Discrepancia URL Rating (UR) vs Domain Rating (DR)"""
    ur, dr = data['url_rating'], data['dr']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if ur > 0 and dr > 0:
        ur_dr_ratio = ur / dr
        if ur_dr_ratio < umbral['dr_traffic_discrepancy'] and dr > 35:
            puntos += 1
            factores.append(f"UR muy bajo vs DR ({ur_dr_ratio:.2f})")
    return puntos, factores

def _check_keywords_vs_traffic(data, config):
    """8. Keywords orgánicas vs Tráfico real (Baja visibilidad)"""
    organic_keywords, organic_traffic = data['organic_keywords'], data['organic_traffic']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if organic_keywords > 80:
        traffic_per_kw = organic_traffic / organic_keywords
        if traffic_per_kw < umbral['low_traffic_per_keyword'] and organic_keywords > 150:
            puntos += 1
            factores.append(f"Keywords altas ({organic_keywords}) vs tráfico bajo ({traffic_per_kw:.2f})")
    return puntos, factores

def _check_authority_tlds(data, config):
    """9. TLDs de Autoridad (Falta de .edu/.gov)"""
    pct_authority_tlds, refdomains = data['pct_authority_tlds'], data['refdomains_all']
    puntos, factores = 0, []
    if pct_authority_tlds == 0 and refdomains > 40:
        puntos += config['factores']['tlds_autoridad']
        factores.append("CERO TLDs de autoridad")
    return puntos, factores

def _check_brand_anchors(data, config, es_marca):
    """10. Anchors de Marca Bajos (Sobre-optimización)"""
    pct_brand_anchors, refdomains = data['pct_brand_anchors'], data['refdomains_all']
    puntos, factores = 0, []
    umbral = config['umbrales']
    if pct_brand_anchors >= 0:
        umbral_marca = umbral['min_brand_anchor_ratio'] * 1.5 if es_marca else umbral['min_brand_anchor_ratio']
        if pct_brand_anchors < umbral_marca and refdomains > 25:
            puntos += config['factores']['anchor_text']
            factores.append(f"Anchors marca BAJOS ({pct_brand_anchors*100:.1f}%)")
    return puntos, factores

def _check_domain_patterns(data, config):
    """11. Patrones de spam en el dominio"""
    dominio = data['target']
    puntos, factores = 0, []
    umbral = config['umbrales']
    pattern_count = sum(1 for pat in config['patrones_spam'] if re.search(r'\b'+re.escape(pat)+r'\b', dominio))
    if pattern_count >= umbral['spam_pattern_count']:
        puntos += 1
        factores.append(f"Patrones spam ({pattern_count}) en dominio")
    return puntos, factores

def _calculate_trust_signals(data, config, es_marca):
    """Calcula las señales de confianza que reducen el riesgo."""
    trust_signals = 0
    factores = []
    pos_config = config['positive_signals']

    if data['organic_traffic'] >= pos_config['high_quality_traffic']:
        trust_signals += 2
        factores.append("✓ Tráfico orgánico ALTO")
    if data['RefIP_Diversidad'] >= pos_config['diverse_ip_profile']:
        trust_signals += 2
        factores.append("✓ IPs DIVERSAS")
    if (data['domain_age_months'] / 12.0) >= pos_config['established_domain_age']:
        trust_signals += 1
        factores.append("✓ Dominio ESTABLECIDO")
    if data['pct_brand_anchors'] >= pos_config['branded_anchors_ratio']:
        trust_signals += 1
        factores.append("✓ Anchors marca ADECUADOS")
    if data['pct_authority_tlds'] >= pos_config['authority_tlds_presence']:
        trust_signals += 1
        factores.append("✓ TLDs autoridad PRESENTES")
    if es_marca:
        trust_signals += 2
        factores.append("✓ MARCA VERIFICADA (confianza extra)")
    return trust_signals, factores

# --- MAPPING PRINCIPAL DE DATOS (preparar_datos_spam) ---

def preparar_datos_spam(df):
    """
    Estandariza los nombres de las columnas y limpia datos para el análisis.
    """
    mapping = {
        'target': ['Target', 'target', 'domain', 'url', 'Domain', 'domain_name', 'Dominio_URL'],
        'dr': ['Domain Rating', 'DR', 'domain rating', 'domain_rating', 'Domain Authority', 'DA'],
        'refdomains_all': ['Ref. domains / All', 'Referring domains', 'ref domains', 'Referring Domains', 'refdomains'],
        'backlinks_all': ['Backlinks / All', 'Backlinks', 'backlinks', 'backlink'],
        'ref_ips': ['Ref. IPs / IPs', 'Ref IPs', 'Referring IPs', 'ref_ips'],
        'ref_subnets': ['Ref. IPs / Subnets', 'subnets', 'Subnets', 'ref_subnets'],
        'backlinks_followed': ['Ref. domains / Followed', 'Backlinks / Followed', 'Followed Backlinks', 'backlinks_followed'],
        'domain_age': ['Domain Age', 'Age', 'Created', 'Creation Date', 'domain_age', 'creation_year'],
        'pct_authority_tlds': ['Authority TLDs', 'EduGov Links', 'Educational Links', 'pct_authority_tlds'],
        'pct_brand_anchors': ['Brand Anchors', 'Branded Anchors', 'Anchor Brand', 'pct_brand_anchors'],
        'organic_traffic': ['Organic / Traffic', 'Organic Traffic', 'Traffic', 'Organic search', 'organic_traffic'],
        'organic_keywords': ['Organic / Total Keywords', 'Keywords', 'organic_keywords', 'organic_kw'],
        'url_rating': ['URL Rating', 'UR', 'url_rating']
    }

    df_p = df.copy()
    cols = list(df_p.columns)
    rename_map = {}
    target_col_name = None

    # Mapeo de columnas
    for new_col, candidates in mapping.items():
        found = find_col(cols, candidates)
        if found:
            rename_map[found] = new_col
            if new_col == 'target':
                target_col_name = found

    df_p = df_p.rename(columns=rename_map)

    # Añadir columnas faltantes con valor 0
    for col in mapping.keys():
        if col not in df_p.columns:
            df_p[col] = 0

    # Conversión de Tipos de Datos (limpieza)
    for c in df_p.columns:
        if c == 'target':
            continue
        try:
            df_p[c] = df_p[c].astype(str).str.replace(',', '', regex=False)
            df_p[c] = df_p[c].str.replace('%', '', regex=False)
            df_p[c] = df_p[c].replace(['N/A', '-', '', 'nan', 'inf'], '0')
            if c != 'domain_age':
                df_p[c] = pd.to_numeric(df_p[c], errors='coerce').fillna(0)
        except:
            df_p[c] = 0

    # CÁLCULOS DERIVADOS
    df_p['domain_age_months'] = df_p['domain_age'].apply(calcular_edad_dominio_meses).fillna(0)

    for pct in ['pct_authority_tlds', 'pct_brand_anchors']:
        if pct in df_p.columns:
            df_p[pct] = df_p[pct].clip(0,100)/100.0

    df_p['Pct_Backlinks_Followed'] = np.where(
        df_p['backlinks_all']>0,
        df_p['backlinks_followed']/df_p['backlinks_all'], 0.0
    )

    df_p['RefIP_Diversidad'] = np.where(
        df_p['ref_ips']>0,
        df_p['ref_subnets']/df_p['ref_ips'], 0.0
    )

    df_p['domains_per_month'] = df_p.apply(
        lambda r: r['refdomains_all']/r['domain_age_months']
        if r['domain_age_months']>0 else float('inf'), axis=1
    )

    return df_p, target_col_name

def calcular_spam_score_moz(domain_data, config):
    """Función principal de cálculo de riesgo."""
    puntos = 0
    factores = []
    metricas_usadas = 0

    refdomains = float(domain_data.get('refdomains_all', 0))
    dominio = str(domain_data.get('target', '')).lower()
    umbral = config['umbrales']
    es_marca = es_marca_verificada(dominio, config)

    # 1. CHECK INICIAL: DATOS INSUFICIENTES
    if refdomains < umbral['min_refdomains_analysis']:
        return {
            'puntos_riesgo': 0, 'spam_score_100': 0, 'nivel_riesgo': "✅ INSUFICIENTES DATOS",
            'accion_recomendada': "NO ANALIZADO", 'spam_factores': "Muy pocos dominios de referencia",
            'metricas_usadas': 0, 'confianza_analisis': 0.1, 'trust_signals': 0, 'es_marca_verificada': es_marca
        }

    # 2. CÁLCULO DE PENALIDADES (Modularizado)
    checks = [
        _check_density, _check_diversity, _check_growth, _check_traffic_vs_authority,
        _check_dofollow_ratio, _check_age_vs_authority, _check_ur_vs_dr,
        _check_keywords_vs_traffic, _check_authority_tlds, _check_brand_anchors,
        _check_domain_patterns
    ]

    for check_func in checks:
        try:
            if check_func in [_check_density, _check_diversity, _check_growth, _check_traffic_vs_authority, _check_age_vs_authority, _check_brand_anchors]:
                p, f = check_func(domain_data, config, es_marca)
            else:
                p, f = check_func(domain_data, config)
            puntos += p
            factores.extend(f)
            metricas_usadas += 1
        except Exception:
            pass

    # 3. CÁLCULO DE RECOMPENSAS
    trust_signals, trust_factores = _calculate_trust_signals(domain_data, config, es_marca)
    factores.extend(trust_factores)

    # 4. AJUSTE FINAL
    puntos = max(0, puntos - min(trust_signals, 6))
    puntos = min(puntos, config['max_puntos'])

    confianza = min(metricas_usadas / float(len(checks)), 1.0)
    spam_score_100 = int(round((puntos / float(config['max_puntos'])) * 100)) if config['max_puntos']>0 else 0

    # CLASIFICACIÓN
    if puntos >= config['alto_riesgo']:
        nivel, accion = "🔴 ALTO SPAM SCORE", "EVITAR - Alto riesgo"
    elif puntos >= config['riesgo_moderado']:
        nivel, accion = "🟠 SPAM SCORE MODERADO", "REVISIÓN RECOMENDADA"
    elif puntos >= config['riesgo_bajo']:
        nivel, accion = "🟡 SPAM SCORE BAJO", "PRECAUCIÓN"
    else:
        nivel, accion = "✅ SPAM SCORE MÍNIMO", "CONFIABLE"

    return {
        'puntos_riesgo': puntos, 
        'spam_score_100': spam_score_100, 
        'nivel_riesgo': nivel,
        'accion_recomendada': accion, 
        'spam_factores': ".\n".join(factores) if factores else "Perfil limpio - Sin penalizaciones detectadas.",
        'confianza_analisis': round(confianza, 2),
        'trust_signals': trust_signals, 
        'es_marca_verificada': es_marca
    }

def analizar_spam_vectorizado(df_prepared, config):
    """Aplica la lógica de análisis fila por fila."""
    resultados = []
    
    for _, row in df_prepared.iterrows():
        dom_data = row.to_dict()
        res = calcular_spam_score_moz(dom_data, config)
        
        out = res
        out['target_merge_key'] = dom_data.get('target', '')
        resultados.append(out)

    return pd.DataFrame(resultados)

# --- FUNCIÓN DE DESCARGA ---
@st.cache_data
def convert_df_to_excel(df):
    """Convierte el DataFrame a un objeto BytesIO de Excel para la descarga."""
    df_export = df.rename(columns={
        'puntos_riesgo': 'Spam Score (Puntos Riesgo)',
        'spam_score_100': 'Spam Score (%)',
        'nivel_riesgo': 'Nivel de Riesgo',
        'accion_recomendada': 'Accion Recomendada',
        'spam_factores': 'Factores de Penalizacion/Bonificacion',
        'confianza_analisis': 'Confianza Analisis (%)',
        'trust_signals': 'Bonificacion Trust',
        'es_marca_verificada': 'Es Marca Verificada'
    })

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Resultados_SPAM_PBN')
    processed_data = output.getvalue()
    return processed_data


# =========================================================
# INTERFAZ DE STREAMLIT (CON FLUJO MEJORADO)
# =========================================================

# Inicializa el estado para saber si se ha ejecutado el análisis
if 'analysis_run' not in st.session_state:
    st.session_state.analysis_run = False
    st.session_state.df_resultados = None
    st.session_state.target_col_name = None

def main():
    # El st.set_page_config debe ser la primera llamada de Streamlit
    st.set_page_config(layout="wide", page_title="Streamlit Spam/PBN Analyzer")
    
    st.title("🌐 Analizador de SPAM y Riesgo PBN")
    st.markdown("Herramienta potenciada por tu sistema de análisis híbrido (Streamlit Edition).")
    
    # --- Columna de Configuración (Sidebar) ---
    with st.sidebar:
        st.header("⚙️ Configuración de Análisis")
        
        # SLIDER DE FLEXIBILIDAD
        flexibility_level = st.slider(
            "Nivel de Flexibilidad del Detector (1 a 10)",
            min_value=1, 
            max_value=10, 
            value=st.session_state.get('flexibility_slider_value', 6), 
            step=1,
            help="1: Máximo estricto (fácil clasificar como SPAM). 10: Máxima flexibilidad.",
            key='flexibility_slider_value'
        )
        
        CONFIG = ajustar_configuracion(flexibility_level)
        
        st.markdown(f"**Factor de Ajuste (0.0 a 1.0):** `{(flexibility_level - 1) / 9.0:.2f}`")
        st.markdown(f"**Umbral Alto Riesgo:** Puntuación >= **{CONFIG['alto_riesgo']}**")
        st.markdown(f"<hr>", unsafe_allow_html=True)
        
        st.header("📂 Cargar Datos")
        uploaded_file = st.file_uploader(
            "Sube un archivo **Excel (.xlsx)**", 
            type=['xlsx'], 
            help="El análisis requiere múltiples métricas.",
            key='uploaded_file_key'
        )

    # --- Área Principal (Control de Flujo) ---
    
    # 1. Ejecutar al inicio o al subir un archivo (preparación)
    if uploaded_file is not None:
        try:
            uploaded_file.seek(0)
            df_original = pd.read_excel(uploaded_file)
            df_original.columns = df_original.columns.astype(str).str.strip() 
            df_original = df_original.reset_index(drop=True)
            
            st.success(f"Archivo **'{uploaded_file.name}'** cargado con {len(df_original)} filas. **Ahora pulsa EJECUTAR.**")
            
            df_prepared, target_col_name = preparar_datos_spam(df_original.copy())

            if not target_col_name:
                st.error("❌ ERROR: No se pudo identificar la columna de dominio/URL.")
                return

            # --- BOTONES DE ACCIÓN (IMITA BOTONES PRINCIPALES DE COLAB) ---
            st.subheader("Paso 2: Ejecutar y Reiniciar")
            col_exec, col_reset = st.columns([1, 1])

            with col_exec:
                # El botón principal de ejecución
                if st.button("🚀 EJECUTAR ANÁLISIS SPAM/PBN", type="primary"):
                    st.session_state.analysis_run = True
                    # Al ejecutar, guardamos los datos necesarios en la sesión
                    st.session_state.df_prepared = df_prepared
                    st.session_state.df_original = df_original
                    st.session_state.target_col_name = target_col_name
                    st.rerun() # Fuerza una nueva ejecución para mostrar resultados

            with col_reset:
                # El botón de reinicio (como el clear_output de Colab)
                if st.button("🔄 REINICIAR / NUEVO ANÁLISIS", type="secondary"):
                    st.session_state.analysis_run = False
                    st.session_state.df_resultados = None
                    st.session_state.uploaded_file_key = None
                    st.rerun() # Reinicia la aplicación

            # 2. Mostrar Resultados solo si el estado es 'analysis_run'
            if st.session_state.analysis_run:
                st.markdown("---")
                
                # --- Ejecución y Fusión ---
                with st.spinner('Procesando lógica de penalización...'):
                    # Usamos los datos guardados en la sesión
                    df_analysis_results = analizar_spam_vectorizado(st.session_state.df_prepared, CONFIG)

                df_prepared_temp = st.session_state.df_prepared.copy()
                df_prepared_temp['target_merge_key_prep'] = df_prepared_temp['target']

                df_results_merge = pd.merge(
                    st.session_state.df_original.rename(columns={st.session_state.target_col_name: 'target_merge_key_prep_orig'}),
                    df_analysis_results,
                    left_on='target_merge_key_prep_orig',
                    right_on='target_merge_key',
                    how='left'
                )
                
                df_resultados = df_results_merge.rename(columns={'target_merge_key_prep_orig': st.session_state.target_col_name})
                df_resultados = df_resultados.drop(columns=['target_merge_key'], errors='ignore')
                st.session_state.df_resultados = df_resultados # Guardar resultados finales

                # 3. Mostrar la Tabla y Métricas
                st.header("✅ Resultados del Análisis")
                
                total_dominios = len(df_resultados)
                alto_riesgo = len(df_resultados[df_resultados['spam_score_100'] >= 70])
                moderado_riesgo = len(df_resultados[(df_resultados['spam_score_100'] >= 40) & (df_resultados['spam_score_100'] < 70)])
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Analizado", total_dominios)
                col2.metric("Alto Riesgo (>=70%)", alto_riesgo, delta=f"{(alto_riesgo/total_dominios*100):.1f}% del total" if total_dominios > 0 else "0.0%")
                col3.metric("Moderado Riesgo (40-69%)", moderado_riesgo, delta=f"{(moderado_riesgo/total_dominios*100):.1f}% del total" if total_dominios > 0 else "0.0%")
                
                columnas_finales = [
                    st.session_state.target_col_name,
                    'Domain Rating', 
                    'Organic / Traffic', 
                    'Ref. domains / All',
                    'spam_score_100', 
                    'nivel_riesgo', 
                    'accion_recomendada',
                    'spam_factores'
                ]
                
                valid_cols = [c for c in columnas_finales if c in df_resultados.columns]
                df_display = df_resultados[valid_cols].sort_values('spam_score_100', ascending=False, na_position='last').rename(columns={'spam_factores': 'Factores_Penalizacion', 'spam_score_100': 'Spam Score (%)', 'nivel_riesgo': 'Nivel Riesgo'})

                try:
                    df_styled = df_display.style.background_gradient(
                        subset=['Spam Score (%)'],
                        cmap='Reds',
                        low=0.1,
                        high=0.8
                    ).format({'Spam Score (%)': '{:d}%'})
                    st.dataframe(df_styled, use_container_width=True, height=500)
                except Exception as style_error:
                    st.warning(f"⚠️ Error al aplicar el estilo de color. Mostrando tabla sin estilo. Detalle: {style_error}")
                    st.dataframe(df_display, use_container_width=True, height=500)

                # 4. Botón de Descarga (Descarga los resultados de la sesión)
                excel_data = convert_df_to_excel(df_resultados)
                
                st.markdown("---")
                st.download_button(
                    label="📥 DESCARGAR RESULTADOS (Excel)",
                    data=excel_data,
                    file_name=f'Informe_SPAM_PBN_{pd.Timestamp.now().strftime("%Y%m%d_%H%M")}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    key='download_excel'
                )


        except Exception as e:
            st.error(f"Ocurrió un error al procesar el archivo. Detalle del Error: {e}")
            st.session_state.analysis_run = False # Resetear estado si hay fallo

    else:
        # Estado inicial
        st.info("⬆️ Sube tu archivo de dominios en formato Excel (.xlsx) en el panel de la izquierda para comenzar el análisis.")
        st.session_state.analysis_run = False

if __name__ == '__main__':
    main()
