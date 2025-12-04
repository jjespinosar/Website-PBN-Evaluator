import streamlit as st
import pandas as pd
import io
import base64
from datetime import datetime
import math
import warnings
import re
warnings.filterwarnings('ignore')

# =========================================================
# CONFIGURACIÓN Y FUNCIONES BASE (DEL CÓDIGO COLAB ORIGINAL)
# =========================================================

BATCH_LIMIT = 200 
DISPLAY_MAX_ROWS = 500

WHITELIST_DOMAINS = [
    'kommo.com', 'amocrm.com', 'hubspot.com', 'salesforce.com',
    'zoho.com', 'microsoft.com', 'google.com', 'facebook.com',
    'linkedin.com', 'twitter.com', 'instagram.com',
    'zapier.com', 'notion.so', 'notion.com', 'slack.com', 'asana.com',
    'trello.com', 'calendly.com', 'airtable.com', 'medium.com',
    'wix.com', 'wordpress.com', 'spotify.com', 'canva.com',
    'github.com', 'gitlab.com', 'stripe.com', 'paypal.com',
    'tableau.com', 'intercom.com'
]

EXCEPT_DOMAINS = [
    'zapier.com', 'canva.com', 'notion.so', 'notion.com', 'slack.com',
    'asana.com', 'trello.com', 'calendly.com', 'airtable.com',
    'github.com', 'gitlab.com', 'stripe.com', 'paypal.com',
    'intercom.com', 'medium.com', 'wix.com'
]

# --- Lógica de utilidades (find_col, calcular_edad_dominio) ---
def find_col(cols, candidates):
    """Busca una columna siendo tolerante a mayúsculas/minúsculas y espacios."""
    lc = {c.lower().strip(): c for c in cols}
    for cand in candidates:
        for k,v in lc.items():
            if cand.lower() == k:
                return v
    for cand in candidates:
        for k,v in lc.items():
            if cand.lower() in k:
                return v
    return None

def calcular_edad_dominio(creation_date):
    """Convierte la fecha de creación a edad en años (lógica del Colab)."""
    if pd.isna(creation_date) or creation_date == 0:
        return 0

    try:
        if isinstance(creation_date, str):
            created = None
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y.%m.%d', '%Y']:
                try:
                    created = datetime.strptime(creation_date, fmt)
                    break
                except ValueError:
                    continue
            if created is None:
                return 0
        elif isinstance(creation_date, (int, float)):
            if creation_date > 1900 and creation_date <= datetime.now().year:
                 return max(0, datetime.now().year - int(creation_date))
            return 0
        elif isinstance(creation_date, datetime):
            created = creation_date
        else:
            return 0

        edad = (datetime.now() - created).days / 365.25
        return round(edad, 1)

    except Exception:
        return 0

# --- Lógica de preparación (prepare_df_tolerant) ---
def prepare_df_tolerant(df):
    """Prepara y limpia el DataFrame, calculando métricas derivadas."""
    cols = list(df.columns)
    mapping = {}
    
    # Mapeo de todas las columnas del script original
    mapping['target'] = find_col(cols, ['Target','target','domain','url'])
    mapping['dr'] = find_col(cols, ['Domain Rating','DR','domain rating','Domain Authority','DA'])
    mapping['organic_traffic'] = find_col(cols, ['Organic / Traffic','Organic Traffic','Traffic','Organic search'])
    mapping['refdomains_all'] = find_col(cols, ['Ref. domains / All','Referring domains','ref domains'])
    mapping['refdomains_followed'] = find_col(cols, ['Ref. domains / Followed','followed'])
    mapping['refdomains_nofollowed'] = find_col(cols, ['Ref. domains / Not followed', 'Ref Domains Nofollow'])
    mapping['backlinks_all'] = find_col(cols, ['Backlinks / All','Backlinks','backlinks'])
    mapping['backlinks_followed'] = find_col(cols, ['Backlinks / Followed'])
    mapping['backlinks_nofollow'] = find_col(cols, ['Backlinks / Not followed', 'Backlinks / Nofollow', 'Nofollow Backlinks'])
    mapping['ref_ips'] = find_col(cols, ['Ref. IPs / IPs','Ref IPs'])
    mapping['ref_subnets'] = find_col(cols, ['Ref. IPs / Subnets','subnets'])
    mapping['domain_age'] = find_col(cols, ['Domain Age', 'Age', 'Created', 'Creation Date'])
    mapping['pct_authority_tlds'] = find_col(cols, ['Authority TLDs', 'EduGov Links', 'Educational Links'])
    mapping['pct_brand_anchors'] = find_col(cols, ['Brand Anchors', 'Branded Anchors', 'Anchor Brand'])
    mapping['url_rating'] = find_col(cols, ['URL Rating', 'UR', 'url_rating'])
    mapping['ahrefs_rank'] = find_col(cols, ['Ahrefs Rank', 'ahrefs_rank'])
    mapping['organic_keywords'] = find_col(cols, ['Organic / Total Keywords', 'Keywords', 'organic_keywords'])

    rename_map = {v:k for k,v in mapping.items() if v is not None}
    df2 = df.rename(columns=rename_map)

    if 'target' not in df2.columns:
        df2['target'] = df.index.astype(str)
    
    # Conversión y limpieza de datos (Bloque Corregido)
    for k in mapping.keys():
        if k == 'target': continue
        
        # 1. Asegurar la columna y el tipo str para limpieza
        if k not in df2.columns:
            df2[k] = 0
            
        df2[k] = df2[k].astype(str).str.strip().replace('', '0')

        # 2. Aplicar lógica de conversión
        if k == 'domain_age':
            # APLICA LA FUNCIÓN DE CÁLCULO DE EDAD
            df2[k] = df2[k].apply(calcular_edad_dominio)
        else:
            # LIMPIEZA GENÉRICA Y CONVERSIÓN A NUMÉRICO
            df2[k] = df2[k].str.replace(',', '', regex=False).str.replace('N/A', '0', regex=False)
            df2[k] = pd.to_numeric(df2[k], errors='coerce').fillna(0)
    
    # Cálculos derivados
    df2['RefDom_por_Backlink'] = df2.apply(lambda r: r['refdomains_all']/r['backlinks_all'] if r['backlinks_all']>0 else 0, axis=1)
    df2['Pct_RefDom_Followed'] = df2.apply(lambda r: r['refdomains_followed']/r['refdomains_all'] if r['refdomains_all']>0 else 0, axis=1)
    df2['Traffico_por_RefDom'] = df2.apply(lambda r: r['organic_traffic']/r['refdomains_all'] if r['refdomains_all']>0 else 0, axis=1)

    if 'backlinks_followed' in df2.columns and 'backlinks_all' in df2.columns:
        df2['Pct_Backlinks_Followed'] = df2.apply(
            lambda r: r['backlinks_followed']/r['backlinks_all'] if r['backlinks_all']>0 else 0.0, axis=1
        )
    else:
        df2['Pct_Backlinks_Followed'] = 0.8 # Valor default como en el script original

    if 'backlinks_nofollow' in df2.columns and 'backlinks_all' in df2.columns:
        df2['Pct_Backlinks_Nofollow'] = df2.apply(
            lambda r: r['backlinks_nofollow']/r['backlinks_all'] if r['backlinks_all']>0 else 0.2, axis=1
        )
        df2['Pct_Backlinks_Followed'] = df2.apply(
            lambda r: 1 - r['Pct_Backlinks_Nofollow'] if r['backlinks_all']>0 else 0.8, axis=1
        )
    else:
        df2['Pct_Backlinks_Nofollow'] = 1 - df2['Pct_Backlinks_Followed']

    if 'refdomains_nofollowed' in df2.columns and 'refdomains_all' in df2.columns:
        df2['Pct_RefDom_Nofollowed'] = df2.apply(
            lambda r: r['refdomains_nofollowed']/r['refdomains_all'] if r['refdomains_all']>0 else 0, axis=1
        )
    else:
        df2['Pct_RefDom_Nofollowed'] = 0
    
    df2['RefIP_Diversidad'] = df2.apply(lambda r: r['ref_subnets']/r['ref_ips'] if r['ref_ips']>0 else 0, axis=1)

    if 'pct_authority_tlds' in df2.columns:
         df2['pct_authority_tlds'] = df2['pct_authority_tlds'].clip(0, 100) / 100
    if 'pct_brand_anchors' in df2.columns:
        df2['pct_brand_anchors'] = df2['pct_brand_anchors'].clip(0, 100) / 100

    return df2.fillna(0)

# --- Lógica de Scoring (simulate_score) ---
# ... (El resto de las funciones: simulate_score, es_marca_whitelist, detectar_pbn, ajustar_por_whitelist, run_analysis, convert_df_to_excel, main_app2, etc. - se mantienen exactamente igual que en la respuesta anterior)
# NOTA: Todo el código de las funciones restantes es muy largo y se mantiene sin cambios, pero debe ser incluido en el archivo final.

def simulate_score(row):
    """Calcula el Score principal (0-100) basado en la fórmula de pesos."""
    # Extracción de datos con valores por defecto 0.0
    dr = float(row.get('dr', 0))
    traffic = float(row.get('organic_traffic', 0))
    refdomains = float(row.get('refdomains_all', 0))
    pct_bl_followed = float(row.get('Pct_Backlinks_Followed', 0))
    pct_bl_nofollow = float(row.get('Pct_Backlinks_Nofollow', 0))
    refip_div = float(row.get('RefIP_Diversidad', 0))
    domain_age = float(row.get('domain_age', 0))
    pct_authority_tlds = float(row.get('pct_authority_tlds', 0))
    pct_brand_anchors = float(row.get('pct_brand_anchors', 0))
    url_rating = float(row.get('url_rating', 0))
    backlinks = float(row.get('backlinks_all', 0))

    if dr <= 30:
        return 0, '❌ NO ACEPTABLE - DR menor a 30', f"DR ({int(dr)}) no cumple el requisito mínimo de 30 para ser considerado."

    # Pesos definidos
    pesos = {
        'dr_quality': 0.35, 'traffic_authority': 0.25,
        'link_profile': 0.20, 'trust_signals': 0.20
    }
    scores = {}

    # 1. CALIDAD POR DR
    dr_quality = min(100, dr)
    scores['dr_quality'] = dr_quality * pesos['dr_quality']

    # 2. AUTORIDAD POR TRÁFICO
    traffic_score = min(100, math.log10(traffic) * 25) if traffic > 0 else 0
    expected_traffic = dr * 1000
    traffic_quality_ratio = min(2, traffic / expected_traffic) if expected_traffic > 0 else 1
    traffic_authority = traffic_score * traffic_quality_ratio
    scores['traffic_authority'] = min(100, traffic_authority) * pesos['traffic_authority']

    # 3. PERFIL DE LINKS
    link_profile_score = 0
    ip_diversity = min(100, refip_div * 100) * 0.3
    
    if backlinks > 0:
        rd_bl_ratio = refdomains / backlinks
        ratio_score = 100 if 0.05 <= rd_bl_ratio <= 0.5 else max(0, 100 - abs(rd_bl_ratio - 0.2) * 500)
    else:
        ratio_score = 0
    ratio_component = ratio_score * 0.3
    anchor_score = min(100, pct_brand_anchors * 200) * 0.2
    follow_score = 100 * 0.2 if 0.7 <= pct_bl_followed <= 0.9 else 50 * 0.2

    link_profile_score = ip_diversity + ratio_component + anchor_score + follow_score
    scores['link_profile'] = min(100, link_profile_score) * pesos['link_profile']

    # 4. SEÑALES DE TRUST
    trust_signals_score = 0
    age_score = min(100, (domain_age / 20) * 100) if domain_age > 0 else 0
    trust_signals_score += age_score * 0.4
    tld_score = min(100, pct_authority_tlds * 500)
    trust_signals_score += tld_score * 0.3
    ur_score = min(100, url_rating * 2.5)
    trust_signals_score += ur_score * 0.3

    scores['trust_signals'] = min(100, trust_signals_score) * pesos['trust_signals']

    # CÁLCULO FINAL
    raw_score = sum(scores.values())
    score = int(max(1, min(100, round(raw_score))))

    # Construir razón para descarga
    reason_parts = []
    reason_parts.append(f"DR: {int(dr)}")
    reason_parts.append(f"Tráfico: {int(traffic)}")
    reason_parts.append(f"RefDom: {int(refdomains)}")
    reason_parts.append(f"Dofollow: {pct_bl_followed*100:.1f}%")
    reason_parts.append(f"Nofollow: {pct_bl_nofollow*100:.1f}%")
    if domain_age > 0: reason_parts.append(f"Edad: {domain_age} años")
    if pct_authority_tlds > 0: reason_parts.append(f"TLDs autoridad: {pct_authority_tlds*100:.1f}%")
    if pct_brand_anchors > 0: reason_parts.append(f"Anchors marca: {pct_brand_anchors*100:.1f}%")
    if url_rating > 0: reason_parts.append(f"URL Rating: {url_rating}")
    reason = ".\n".join(reason_parts)

    # Clasificación
    if score >= 75:
        label = '✅ Excelente - Dominio fuerte y confiable'
    elif score >= 50:
        label = '⚠️ Aceptable - Dominio decente'
    else:
        label = '❌ Riesgoso - Poca autoridad o posible spam'

    return score, label, reason

# --- Lógica de Detección PBN (detectar_pbn) ---
def es_marca_whitelist(domain_data):
    """Verifica si el dominio está en la whitelist o tiene señales de marca legítima."""
    dominio_raw = domain_data.get('target', '').lower().strip()
    dominio = dominio_raw.replace('http://','').replace('https://','').lstrip('www.')
    if '/' in dominio:
        dominio = dominio.split('/')[0]

    for marca in WHITELIST_DOMAINS:
        marca = marca.lower().strip()
        if dominio == marca or dominio.endswith('.' + marca) or dominio.endswith(marca):
            return True

    dr = domain_data.get('dr', 0)
    traffic = domain_data.get('organic_traffic', 0)
    pct_brand_anchors = domain_data.get('pct_brand_anchors', 0)
    domain_age = domain_data.get('domain_age', 0)

    # Señal de marca por métricas altas
    if (dr >= 70 and traffic >= 50000 and pct_brand_anchors >= 0.4 and domain_age >= 3):
        return True

    return False

def detectar_pbn(domain_data):
    """Detecta posibles redes de blogs privados basado en patrones comunes."""
    puntos_sospecha = 0
    alertas = []
    recomendaciones = []

    dr = domain_data.get('dr', 0)

    if dr <= 30: # Requisito mínimo
        return {
            'puntos_sospecha': 10,
            'nivel_riesgo': "🔴 NO ACEPTABLE - DR menor a 30",
            'alertas': [f"DR ({dr}) no cumple el requisito mínimo de 30"],
            'recomendaciones': ["❌ Descartar dominio - No cumple criterio básico de DR"]
        }

    # Extracción de datos (el resto del script utiliza estas variables)
    dom_raw = domain_data.get('target','').lower().strip()
    dom_host = dom_raw.replace('http://','').replace('https://','').lstrip('www.')
    if '/' in dom_host:
        dom_host = dom_host.split('/')[0]

    # Aplicar tolerancia por EXCEPT_DOMAINS
    try:
        for ex in EXCEPT_DOMAINS:
            if dom_host == ex or dom_host.endswith('.' + ex):
                puntos_sospecha = max(0, puntos_sospecha - 1)
                alertas.append(f"ℹ️ Dominio en EXCEPT_DOMAINS ({ex}): aplicando tolerancia extra")
                break
    except Exception:
        pass
    
    traffic = domain_data.get('organic_traffic', 0)
    refdomains = domain_data.get('refdomains_all', 0)
    backlinks = domain_data.get('backlinks_all', 0)
    pct_follow = domain_data.get('Pct_RefDom_Followed', 0)
    ref_ips = domain_data.get('ref_ips', 0)
    ref_subnets = domain_data.get('ref_subnets', 0)
    domain_age = domain_data.get('domain_age', 0)
    pct_authority_tlds = domain_data.get('pct_authority_tlds', 0)
    pct_brand_anchors = domain_data.get('pct_brand_anchors', 0)
    pct_bl_followed = domain_data.get('Pct_Backlinks_Followed', 0)
    url_rating = domain_data.get('url_rating', 0)
    organic_keywords = domain_data.get('organic_keywords', 0)

    # 1. ANÁLISIS DE DIVERSIDAD DE IPs
    if ref_ips > 0:
        diversidad_ips = ref_subnets / ref_ips
        umbral_diversidad = 0.3
        if traffic > 100000: umbral_diversidad = 0.2
        elif traffic > 50000: umbral_diversidad = 0.25

        if diversidad_ips < umbral_diversidad:
            puntos_sospecha += 1
            alertas.append(f"🚩 Baja diversidad de IPs ({diversidad_ips*100:.1f}%) - Posible hosting concentrado")
        else:
            alertas.append(f"✅ Diversidad de IPs aceptable ({diversidad_ips*100:.1f}%)")

    # 2. RELACIÓN BACKLINKS/REFDOMAINS
    backlinks_per_refdomain = backlinks / max(1, refdomains)
    umbral_backlinks_ratio = 10
    if refdomains > 5000: umbral_backlinks_ratio = 20
    elif refdomains > 2000: umbral_backlinks_ratio = 15
    elif refdomains > 1000: umbral_backlinks_ratio = 12

    if backlinks_per_refdomain > umbral_backlinks_ratio:
        puntos_sospecha += 3
        alertas.append(f"🚩 Alta densidad de backlinks ({backlinks_per_refdomain:.1f} por dominio) - Patrón artificial")
    else:
        alertas.append(f"✅ Densidad de backlinks normal ({backlinks_per_refdomain:.1f} por dominio)")

    # 3. PORCENTAJE DE FOLLOWED LINKS
    if pct_follow > 0.995:
        puntos_sospecha += 1
        alertas.append(f"🟠 Porcentaje de referring domains followed muy alto ({pct_follow*100:.2f}%) - Revisar")

    # 4. DISCREPANCIA DR vs TRÁFICO
    if dr > 50 and traffic < 1000:
        señales_autoridad = 0
        if pct_brand_anchors > 0.4: señales_autoridad += 1
        if domain_age >= 5: señales_autoridad += 1
        if pct_authority_tlds > 0.05: señales_autoridad += 1
        if señales_autoridad < 2:
            puntos_sospecha += 1
            alertas.append(f"🚩 DR alto ({dr}) vs tráfico bajo ({traffic}) - Autoridad posiblemente artificial")

    # 5. DISCREPANCIA DR vs REFDOMAINS
    if dr > 60 and refdomains < 100:
        if domain_age < 3:
            puntos_sospecha += 1
            alertas.append(f"🚩 DR muy alto ({dr}) con pocos referring domains ({refdomains})")
        else:
            alertas.append(f"⚠️ DR alto con pocos RD, pero dominio antiguo ({domain_age} años)")

    # 6. EDAD DEL DOMINIO
    if domain_age < 1:
        puntos_sospecha += 2
        alertas.append(f"🚩 Dominio muy nuevo ({domain_age} años) - Posible PBN reciente")
    elif domain_age >= 5:
        puntos_sospecha -= 1
        alertas.append(f"✅ Dominio antiguo ({domain_age} años) - Señal positiva de trust")

    # 7. TLDs DE AUTORIDAD
    if pct_authority_tlds > 0.1:
        puntos_sospecha -= 2
        alertas.append(f"✅ Buen porcentaje de TLDs de autoridad ({pct_authority_tlds*100:.1f}%)")
    elif pct_authority_tlds == 0 and refdomains > 50:
        puntos_sospecha += 1
        alertas.append("⚠️ Ningún link desde TLDs de autoridad (.edu/.gov/.org)")

    # 8. ANCHOR TEXT DE MARCA
    if pct_brand_anchors < 0.3 and pct_brand_anchors > 0:
        puntos_sospecha += 1
        alertas.append("⚠️ Bajo porcentaje de anchor text de marca - Posible sobre-optimización")
    elif pct_brand_anchors >= 0.5:
        puntos_sospecha -= 2
        alertas.append(f"✅ Buen porcentaje de anchor text de marca ({pct_brand_anchors*100:.1f}%) - Señal de marca legítima")

    # 9. PORCENTAJE DOFOLLOW
    if pct_bl_followed > 0.995:
        puntos_sospecha += 1
        alertas.append(f"🟠 Porcentaje dofollow muy alto ({pct_bl_followed*100:.2f}%) - Revisar")
    elif pct_bl_followed < 0.5:
        puntos_sospecha += 1
        alertas.append(f"⚠️ Porcentaje dofollow muy bajo ({pct_bl_followed*100:.1f}%) - Perfil anormal")

    # 10. URL RATING vs DR
    if dr > 50 and url_rating < 20:
        dominio = domain_data.get('target', '')
        if '/' in dominio and dominio.count('/') > 2:
            puntos_sospecha += 1
            alertas.append(f"⚠️ DR alto ({dr}) pero URL Rating bajo ({url_rating}) - Posible página interna")
        else:
            puntos_sospecha += 2
            alertas.append(f"🚩 DR alto ({dr}) pero URL Rating bajo ({url_rating}) - Autoridad posiblemente artificial")

    # 11. ANÁLISIS DE CONTENIDO
    dominio = domain_data.get('target', '')
    patrones_pbn = ['review', 'best', 'top', 'buy', 'cheap', 'discount', 'blog', 'news', 'hub', 'center', 'network', 'express']

    if any(palabra in dominio.lower() for palabra in patrones_pbn):
        if pct_brand_anchors < 0.3:
            puntos_sospecha += 1
            alertas.append("🚩 Dominio con patrón típico de PBN")

    # 12. SEÑALES DE AUTORIDAD LEGÍTIMA (Bonificaciones)
    señales_autoridad = 0
    if traffic > 50000:
        señales_autoridad += 1
        alertas.append("✅ Tráfico orgánico alto - Señal positiva")
    if pct_brand_anchors > 0.4:
        señales_autoridad += 1
        alertas.append("✅ Alto porcentaje de anchors de marca - Señal positiva")
    if domain_age >= 5:
        señales_autoridad += 1
        alertas.append("✅ Dominio antiguo - Señal positiva")
    if organic_keywords > 10000:
        señales_autoridad += 1
        alertas.append("✅ Muchas keywords orgánicas - Señal positiva")

    if señales_autoridad >= 3:
        puntos_sospecha = max(0, puntos_sospecha - 2)
        alertas.append("🔍 Múltiples señales de autoridad legítima detectadas - Reduciendo sospecha")
    elif señales_autoridad >= 2:
        puntos_sospecha = max(0, puntos_sospecha - 1)
        alertas.append("🔍 Algunas señales de autoridad legítima detectadas")

    if domain_age >= 7 or traffic >= 50000 or refdomains >= 2000:
        puntos_sospecha = max(0, puntos_sospecha - 3)
        alertas.append("✅ Dominio altamente consolidado: aplicada reducción adicional de sospecha (-3)")

    puntos_sospecha = max(0, puntos_sospecha)

    # Clasificación final de riesgo
    if puntos_sospecha >= 8:
        riesgo = "🔴 ALTO RIESGO - Posible PBN"
        recomendaciones = ["❌ EVITAR este dominio para linkbuilding", "📊 Revisar manualmente el perfil de backlinks", "🔍 Verificar historial del dominio en Wayback Machine", "🌐 Revisar diversidad geográfica de los referring domains"]
    elif puntos_sospecha >= 5:
        riesgo = "🟡 RIESGO MODERADO - Posibles señales de PBN"
        recomendaciones = ["⚠️ Investigar más a fondo antes de proceder", "📈 Analizar calidad del contenido del dominio", "🔗 Revisar naturalidad del perfil de links", "🌍 Verificar diversidad de IPs y TLDs"]
    elif puntos_sospecha >= 3:
        riesgo = "🟠 RIESGO BAJO - Algunas señales de alerta"
        recomendaciones = ["🔎 Revisar manualmente antes de decidir", "📊 Analizar tendencia de métricas en el tiempo", "🌐 Verificar relevancia temática"]
    else:
        riesgo = "✅ BAJO RIESGO - Perfil natural"
        recomendaciones = ["✅ Perfil de backlinks parece natural"]

    return {
        'puntos_sospecha': puntos_sospecha,
        'nivel_riesgo': riesgo,
        'alertas': alertas,
        'recomendaciones': recomendaciones
    }

# --- Lógica de Ajuste por Whitelist ---
def ajustar_por_whitelist(row):
    """Aplica el ajuste final de Score y PBN si es una marca de Whitelist."""
    if row['Es_Marca_Whitelist']:
        # Ajuste de Score
        row['Score'] = max(row['Score'], 75)
        row['Label'] = '✅ Excelente - Marca Legítima / Whitelist'
        # Ajuste de PBN
        row['PBN_Puntos_Sospecha'] = 0
        row['PBN_Nivel_Riesgo'] = '✅ BAJO RIESGO - Dominio Whitelist'
        row['PBN_Alertas'] = "✅ Dominio de Marca Legítima/Whitelist detectado."
    return row

# --- Funciones de Display y Descarga (Adaptadas para Streamlit) ---
def shorten_text_for_display(text, max_len=80, separator=" | "):
    """Acorta el texto para la visualización en la tabla de resultados (imita la UX de Colab)."""
    if not isinstance(text, str):
        return text

    # Reemplazar saltos de línea con el separador para una sola línea
    text_oneline = text.replace('\n', separator).strip(separator).strip()

    if len(text_oneline) > max_len:
        # Truncar la línea si es demasiado larga
        return text_oneline[:max_len-3] + "..."
    return text_oneline

@st.cache_data
def convert_df_to_excel(df):
    """Convierte el DataFrame a un objeto BytesIO de Excel para la descarga."""
    
    df_export = df.rename(columns={
        'target': 'Dominio',
        'dr': 'Domain Rating',
        'organic_traffic': 'Organic / Traffic',
        'domain_age': 'Domain Age (años)',
        'refdomains_all': 'Ref. domains / All',
        'backlinks_all': 'Backlinks / All',
        'url_rating': 'URL Rating',
        'organic_keywords': 'Organic / Total Keywords',
        'Score': 'Trust Score (0-100)',
        'Label': 'Trust Score - Nivel',
        'Reason': 'Trust Score - Factores', # Contiene '\n' para formato de descarga
        'PBN_Puntos_Sospecha': 'PBN - Puntos Sospecha',
        'PBN_Nivel_Riesgo': 'PBN - Nivel de Riesgo',
        'PBN_Alertas': 'PBN - Alertas', # Contiene '\n' para formato de descarga
        'PBN_Recomendaciones': 'PBN - Recomendaciones', # Contiene '\n' para formato de descarga
        'Es_Marca_Whitelist': 'Es Marca (Whitelist/Metricas)'
    })
    
    output = io.BytesIO()
    # Usamos la librería openpyxl instalada
    with pd.ExcelWriter(output, engine='openpyxl') as writer: 
        df_export.to_excel(writer, index=False, sheet_name='Resultados_PBN_Eval')
    processed_data = output.getvalue()
    return processed_data

@st.cache_data
def convert_df_to_csv(df):
    """Convierte el DataFrame a CSV para la descarga."""
    df_export = df.rename(columns={
        'target': 'Dominio',
        'dr': 'Domain Rating',
        'organic_traffic': 'Organic / Traffic',
        'domain_age': 'Domain Age (años)',
        'refdomains_all': 'Ref. domains / All',
        'backlinks_all': 'Backlinks / All',
        'url_rating': 'URL Rating',
        'organic_keywords': 'Organic / Total Keywords',
        'Score': 'Trust Score (0-100)',
        'Label': 'Trust Score - Nivel',
        'Reason': 'Trust Score - Factores', # Contiene '\n' para formato de descarga
        'PBN_Puntos_Sospecha': 'PBN - Puntos Sospecha',
        'PBN_Nivel_Riesgo': 'PBN - Nivel de Riesgo',
        'PBN_Alertas': 'PBN - Alertas', # Contiene '\n' para formato de descarga
        'PBN_Recomendaciones': 'PBN - Recomendaciones', # Contiene '\n' para formato de descarga
        'Es_Marca_Whitelist': 'Es Marca (Whitelist/Metricas)'
    })
    return df_export.to_csv(index=False).encode('utf-8')

# --- FUNCIÓN PRINCIPAL DE ANÁLISIS ---
def run_analysis(df_input):
    """Ejecuta el pipeline completo de análisis del script original."""
    
    if len(df_input) > BATCH_LIMIT:
        df_input = df_input.head(BATCH_LIMIT)

    # 1. Preparar y limpiar el DataFrame
    df_prepared = prepare_df_tolerant(df_input.copy())

    # 2. Aplicar el scoring principal
    df_prepared[['Score', 'Label', 'Reason']] = df_prepared.apply(
        lambda row: simulate_score(row),
        axis=1,
        result_type='expand'
    )

    # 3. Aplicar detección de PBN
    df_pbn_results = df_prepared.apply(
        lambda row: detectar_pbn(row.to_dict()),
        axis=1,
        result_type='expand'
    )

    # 4. Integrar resultados PBN
    df_prepared['PBN_Puntos_Sospecha'] = df_pbn_results['puntos_sospecha']
    df_prepared['PBN_Nivel_Riesgo'] = df_pbn_results['nivel_riesgo']
    # Se usa '\n' para el formato de descarga
    df_prepared['PBN_Alertas'] = df_pbn_results['alertas'].apply(lambda x: "\n".join(x))
    df_prepared['PBN_Recomendaciones'] = df_pbn_results['recomendaciones'].apply(lambda x: "\n".join(x))

    # 5. Aplicar verificación de Whitelist
    df_prepared['Es_Marca_Whitelist'] = df_prepared.apply(
        lambda row: es_marca_whitelist(row.to_dict()),
        axis=1
    )

    # 6. Aplicar ajuste por Whitelist
    df_prepared = df_prepared.apply(ajustar_por_whitelist, axis=1)

    # 7. Selección y orden de columnas finales
    cols_to_keep = [
        'target', 'dr', 'organic_traffic', 'domain_age',
        'refdomains_all', 'backlinks_all', 'url_rating', 'organic_keywords',
        'Score', 'Label', 'Reason',
        'PBN_Puntos_Sospecha', 'PBN_Nivel_Riesgo', 'PBN_Alertas', 'PBN_Recomendaciones',
        'Es_Marca_Whitelist'
    ]

    df_result = df_prepared.reindex(columns=cols_to_keep)
    
    return df_result

# =========================================================
# INTERFAZ DE STREAMLIT (CON FLUJO SECUENCIAL)
# =========================================================

# Inicializa el estado para el control de flujo
if 'df_original_app2' not in st.session_state:
    st.session_state.df_original_app2 = None
if 'analysis_run_app2' not in st.session_state:
    st.session_state.analysis_run_app2 = False
if 'df_resultados_app2' not in st.session_state:
    st.session_state.df_resultados_app2 = None


def main_app2():
    st.set_page_config(layout="wide", page_title="Website Evaluation + PBN Tool")
    
    st.markdown("<h2>🌐 Website Evaluation Tool + Detección PBN</h2>", unsafe_allow_html=True)
    st.markdown("---")

    st.subheader("Paso 1: Cargar Archivo")
    st.info("Sube tu archivo (Excel o CSV). El análisis se ejecutará para un máximo de 200 dominios.")
    
    # --- Carga de Archivo ---
    uploaded_file = st.file_uploader(
        "Sube tu archivo de dominios", 
        type=['xlsx', 'csv'], 
        key='uploaded_file_app2'
    )

    if uploaded_file is not None and st.session_state.df_original_app2 is None:
        try:
            uploaded_file.seek(0)
            if uploaded_file.name.endswith('.xlsx'):
                df_input = pd.read_excel(uploaded_file)
            else: 
                # Manejo de encodings (como en Colab)
                try:
                    df_input = pd.read_csv(uploaded_file, encoding='utf-8')
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    df_input = pd.read_csv(uploaded_file, encoding='latin1')
            
            df_input.columns = df_input.columns.astype(str).str.strip() # Limpieza de columnas

            st.session_state.df_original_app2 = df_input
            st.success(f"✅ Archivo cargado con **{len(df_input)}** filas. **Ahora pulsa 'Evaluar Archivo'.**")

        except Exception as e:
            st.error(f"❌ Ocurrió un error al cargar el archivo. Detalle: {e}")
            st.session_state.df_original_app2 = None
    
    # --- BOTONES DE ACCIÓN (Secuenciales como Colab) ---
    st.subheader("Paso 2: Evaluar y Reiniciar")
    
    col_evaluar, col_reiniciar = st.columns([1, 1])
    
    with col_evaluar:
        # El botón principal 'Cargar y Evaluar'
        eval_button = st.button(
            "📂 Evaluar Archivo", 
            type="primary",
            disabled=(st.session_state.df_original_app2 is None or st.session_state.analysis_run_app2), # Desactivado si no hay archivo o ya se corrió
            key='eval_button_app2'
        )
        
    with col_reiniciar:
        # El botón de 'Reiniciar'
        reset_button = st.button(
            "🔄 Reiniciar", 
            type="secondary",
            key='reset_button_app2'
        )
        
    # --- LÓGICA DE CONTROL DE FLUJO ---
    if eval_button:
        # Ejecución del análisis al hacer clic
        st.session_state.analysis_run_app2 = True
        st.rerun() # Fuerza la recarga para pasar a la fase de resultados

    if reset_button:
        # Resetea el estado global (similar a clear_output() de Colab)
        st.session_state.df_original_app2 = None
        st.session_state.analysis_run_app2 = False
        st.session_state.df_resultados_app2 = None
        # Resetea el uploader forzando un nuevo widget
        del st.session_state['uploaded_file_app2']
        st.rerun() 


    # 3. Mostrar Resultados solo si se pulsó 'Evaluar'
    if st.session_state.analysis_run_app2 and st.session_state.df_original_app2 is not None:
        st.markdown("---")
        st.header("✅ Resultados de la Evaluación PBN")

        if st.session_state.df_resultados_app2 is None:
            with st.spinner('⚙️ Ejecutando análisis de métricas, Trust Score y Patrones PBN...'):
                try:
                    df_resultados = run_analysis(st.session_state.df_original_app2)
                    st.session_state.df_resultados_app2 = df_resultados # Guardar resultados finales
                except Exception as e:
                    st.error(f"❌ Ocurrió un error durante el procesamiento. Por favor, revisa el formato de tus columnas. Detalle: {e}")
                    st.session_state.analysis_run_app2 = False # Falla y vuelve al estado de carga
                    return
        
        df_resultados = st.session_state.df_resultados_app2
        
        if not df_resultados.empty:
            
            # --- Métricas Resumen ---
            riesgo_alto_pbn = len(df_resultados[df_resultados['PBN_Puntos_Sospecha'] >= 8])
            trust_alto = len(df_resultados[df_resultados['Score'] >= 75])
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Dominios Analizados", len(df_resultados))
            col2.metric("PBN - ALTO RIESGO", riesgo_alto_pbn)
            col3.metric("Trust Score - EXCELENTE (>=75)", trust_alto)
            
            # --- Preparar Tabla de Display (Truncamiento de texto) ---
            df_display = df_resultados.sort_values(by=['Score', 'PBN_Puntos_Sospecha'], ascending=[False, False]).head(DISPLAY_MAX_ROWS).copy()

            for col in ['Reason', 'PBN_Alertas', 'PBN_Recomendaciones']:
                if col in df_display.columns:
                    df_display[col] = df_display[col].apply(shorten_text_for_display)

            # Renombrar para el display
            df_display = df_display.rename(columns={
                'target': 'Dominio',
                'Score': 'Trust Score (0-100)', 
                'Label': 'Trust Score - Nivel',
                'PBN_Puntos_Sospecha': 'PBN - Puntos Sospecha',
                'PBN_Nivel_Riesgo': 'PBN - Nivel Riesgo',
                'Reason': 'Trust Score - Factores',
                'PBN_Alertas': 'PBN - Alertas',
                'PBN_Recomendaciones': 'PBN - Recomendaciones',
                'Es_Marca_Whitelist': 'Whitelist'
            })
            
            # Aplicar estilos de color
            try:
                df_styled = df_display.style.background_gradient(
                    subset=['Trust Score (0-100)'],
                    cmap='RdYlGn',
                    low=0.4,
                    high=1.0
                ).background_gradient(
                    subset=['PBN - Puntos Sospecha'],
                    cmap='Reds',
                    low=0.1,
                    high=0.7
                ).format({'Trust Score (0-100)': '{:d}'})
                st.dataframe(df_styled, use_container_width=True, height=500)
            except Exception as style_error:
                st.warning(f"⚠️ Error al aplicar el estilo de color. Mostrando tabla sin estilo. Detalle: {style_error}")
                st.dataframe(df_display, use_container_width=True, height=500)

            # 4. Botones de Descarga
            st.markdown("---")
            st.subheader("Paso 3: Descargar Resultados")
            st.info("La descarga contiene las columnas de texto completas y con saltos de línea para un mejor reporte.")
            
            excel_data = convert_df_to_excel(df_resultados)
            csv_data = convert_df_to_csv(df_resultados)
            
            col_dl_excel, col_dl_csv = st.columns(2)
            
            with col_dl_excel:
                 st.download_button(
                    label="📥 Descargar Excel",
                    data=excel_data,
                    file_name=f'evaluacion_pbn_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    key='dl_excel_app2'
                )
            with col_dl_csv:
                 st.download_button(
                    label="📥 Descargar CSV",
                    data=csv_data,
                    file_name=f'evaluacion_pbn_{datetime.now().strftime("%Y%m%d_%H%M")}.csv',
                    mime='text/csv',
                    key='dl_csv_app2'
                )


if __name__ == '__main__':
    main_app2()

