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

BATCH_LIMIT = 100 
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
    
    # [cite_start]Mapeo de todas las columnas del script original [cite: 29, 30, 31]
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

    for k in mapping.keys():
        if k == 'target': continue
        if k in df2.columns:
            # [cite_start]Limpieza de datos [cite: 31, 32]
            if k == 'domain_age':
                [cite_start]df2[k] = df2[k].apply(calcular_edad_dominio) [cite: 32]
            else:
                [cite_start]df2[k] = pd.to_numeric(df2[k].astype(str).str.replace(',','').str.replace('N/A','0'), errors='coerce').fillna(0) [cite: 32]
        else:
            df2[k] = 0

    if 'target' not in df2.columns:
        [cite_start]df2['target'] = df.index.astype(str) [cite: 33]
    
    # [cite_start]Cálculos derivados [cite: 33, 34, 35, 36]
    [cite_start]df2['RefDom_por_Backlink'] = df2.apply(lambda r: r['refdomains_all']/r['backlinks_all'] if r['backlinks_all']>0 else 0, axis=1) [cite: 33]
    [cite_start]df2['Pct_RefDom_Followed'] = df2.apply(lambda r: r['refdomains_followed']/r['refdomains_all'] if r['refdomains_all']>0 else 0, axis=1) [cite: 33]
    [cite_start]df2['Traffico_por_RefDom'] = df2.apply(lambda r: r['organic_traffic']/r['refdomains_all'] if r['refdomains_all']>0 else 0, axis=1) [cite: 33]

    if 'backlinks_followed' in df2.columns and 'backlinks_all' in df2.columns:
        df2['Pct_Backlinks_Followed'] = df2.apply(
            lambda r: r['backlinks_followed']/r['backlinks_all'] if r['backlinks_all']>0 else 0, axis=1
        )
    else:
        [cite_start]df2['Pct_Backlinks_Followed'] = 0.8 [cite: 34] # Valor default como en el script original

    if 'backlinks_nofollow' in df2.columns and 'backlinks_all' in df2.columns:
        df2['Pct_Backlinks_Nofollow'] = df2.apply(
            lambda r: r['backlinks_nofollow']/r['backlinks_all'] if r['backlinks_all']>0 else 0.2, axis=1
        [cite_start]) [cite: 34]
        df2['Pct_Backlinks_Followed'] = df2.apply(
            lambda r: 1 - r['Pct_Backlinks_Nofollow'] if r['backlinks_all']>0 else 0.8, axis=1
        [cite_start]) [cite: 35]
    else:
        df2['Pct_Backlinks_Nofollow'] = 1 - df2['Pct_Backlinks_Followed']

    if 'refdomains_nofollowed' in df2.columns and 'refdomains_all' in df2.columns:
        df2['Pct_RefDom_Nofollowed'] = df2.apply(
            lambda r: r['refdomains_nofollowed']/r['refdomains_all'] if r['refdomains_all']>0 else 0, axis=1
        [cite_start]) [cite: 35]
    else:
        df2['Pct_RefDom_Nofollowed'] = 0
    
    [cite_start]df2['RefIP_Diversidad'] = df2.apply(lambda r: r['ref_subnets']/r['ref_ips'] if r['ref_ips']>0 else 0, axis=1) [cite: 36]

    if 'pct_authority_tlds' in df2.columns:
         [cite_start]df2['pct_authority_tlds'] = df2['pct_authority_tlds'].clip(0, 100) / 100 [cite: 36]
    if 'pct_brand_anchors' in df2.columns:
        [cite_start]df2['pct_brand_anchors'] = df2['pct_brand_anchors'].clip(0, 100) / 100 [cite: 36]

    return df2.fillna(0) # Finaliza con un fillna para mayor seguridad

# --- Lógica de Scoring (simulate_score) ---
def simulate_score(row):
    """Calcula el Score principal (0-100) basado en la fórmula de pesos."""
    # Extracción de datos con valores por defecto 0.0
    dr = float(row.get('dr', 0))
    traffic = float(row.get('organic_traffic', 0))
    refdomains = float(row.get('refdomains_all', 0))
    pct_bl_followed = float(row.get('Pct_Backlinks_Followed', 0))
    [cite_start]pct_bl_nofollow = float(row.get('Pct_Backlinks_Nofollow', 0)) [cite: 37]
    refip_div = float(row.get('RefIP_Diversidad', 0))
    domain_age = float(row.get('domain_age', 0))
    pct_authority_tlds = float(row.get('pct_authority_tlds', 0))
    pct_brand_anchors = float(row.get('pct_brand_anchors', 0))
    url_rating = float(row.get('url_rating', 0))
    backlinks = float(row.get('backlinks_all', 0))

    if dr <= 30:
        [cite_start]return 0, '❌ NO ACEPTABLE - DR menor a 30', f"DR ({int(dr)}) no cumple el requisito mínimo de 30 para ser considerado." [cite: 37, 38]

    # [cite_start]Pesos definidos [cite: 38]
    pesos = {
        'dr_quality': 0.35, 'traffic_authority': 0.25,
        'link_profile': 0.20, 'trust_signals': 0.20
    }
    scores = {}

    # [cite_start]1. CALIDAD POR DR [cite: 38]
    dr_quality = min(100, dr)
    scores['dr_quality'] = dr_quality * pesos['dr_quality']

    # [cite_start]2. AUTORIDAD POR TRÁFICO [cite: 38, 39]
    [cite_start]traffic_score = min(100, math.log10(traffic) * 25) if traffic > 0 else 0 [cite: 39]
    expected_traffic = dr * 1000
    [cite_start]traffic_quality_ratio = min(2, traffic / expected_traffic) if expected_traffic > 0 else 1 [cite: 39]
    traffic_authority = traffic_score * traffic_quality_ratio
    [cite_start]scores['traffic_authority'] = min(100, traffic_authority) * pesos['traffic_authority'] [cite: 39]

    # [cite_start]3. PERFIL DE LINKS [cite: 39, 40]
    link_profile_score = 0
    ip_diversity = min(100, refip_div * 100) * 0.3
    
    if backlinks > 0:
        rd_bl_ratio = refdomains / backlinks
        [cite_start]ratio_score = 100 if 0.05 <= rd_bl_ratio <= 0.5 else max(0, 100 - abs(rd_bl_ratio - 0.2) * 500) [cite: 40]
    else:
        ratio_score = 0
    ratio_component = ratio_score * 0.3
    [cite_start]anchor_score = min(100, pct_brand_anchors * 200) * 0.2 [cite: 40]
    [cite_start]follow_score = 100 * 0.2 if 0.7 <= pct_bl_followed <= 0.9 else 50 * 0.2 [cite: 40]

    link_profile_score = ip_diversity + ratio_component + anchor_score + follow_score
    [cite_start]scores['link_profile'] = min(100, link_profile_score) * pesos['link_profile'] [cite: 40]

    # [cite_start]4. SEÑALES DE TRUST [cite: 40, 41]
    trust_signals_score = 0
    [cite_start]age_score = min(100, (domain_age / 20) * 100) if domain_age > 0 else 0 [cite: 41]
    trust_signals_score += age_score * 0.4
    tld_score = min(100, pct_authority_tlds * 500)
    trust_signals_score += tld_score * 0.3
    ur_score = min(100, url_rating * 2.5)
    trust_signals_score += ur_score * 0.3

    [cite_start]scores['trust_signals'] = min(100, trust_signals_score) * pesos['trust_signals'] [cite: 41]

    # CÁLCULO FINAL
    raw_score = sum(scores.values())
    [cite_start]score = int(max(1, min(100, round(raw_score)))) [cite: 41, 42]

    # [cite_start]Construir razón para descarga [cite: 42]
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

    # [cite_start]Clasificación [cite: 43]
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
        [cite_start]dominio = dominio.split('/')[0] [cite: 5]

    for marca in WHITELIST_DOMAINS:
        marca = marca.lower().strip()
        if dominio == marca or dominio.endswith('.' + marca) or dominio.endswith(marca):
            [cite_start]return True [cite: 5]

    dr = domain_data.get('dr', 0)
    traffic = domain_data.get('organic_traffic', 0)
    pct_brand_anchors = domain_data.get('pct_brand_anchors', 0)
    domain_age = domain_data.get('domain_age', 0)

    # [cite_start]Señal de marca por métricas altas [cite: 5, 6]
    if (dr >= 70 and traffic >= 50000 and pct_brand_anchors >= 0.4 and domain_age >= 3):
        [cite_start]return True [cite: 6]

    return False

def detectar_pbn(domain_data):
    """Detecta posibles redes de blogs privados basado en patrones comunes."""
    puntos_sospecha = 0
    alertas = []
    recomendaciones = []

    dr = domain_data.get('dr', 0)

    [cite_start]if dr <= 30: # Requisito mínimo [cite: 6, 7]
        return {
            'puntos_sospecha': 10,
            [cite_start]'nivel_riesgo': "🔴 NO ACEPTABLE - DR menor a 30", [cite: 7]
            'alertas': [f"DR ({dr}) no cumple el requisito mínimo de 30"],
            'recomendaciones': ["❌ Descartar dominio - No cumple criterio básico de DR"]
        }

    # [cite_start]Extracción de datos (el resto del script utiliza estas variables) [cite: 9, 10]
    dom_raw = domain_data.get('target','').lower().strip()
    dom_host = dom_raw.replace('http://','').replace('https://','').lstrip('www.')
    if '/' in dom_host:
        [cite_start]dom_host = dom_host.split('/')[0] [cite: 8]

    # [cite_start]Aplicar tolerancia por EXCEPT_DOMAINS [cite: 8, 9]
    try:
        for ex in EXCEPT_DOMAINS:
            if dom_host == ex or dom_host.endswith('.' + ex):
                puntos_sospecha = max(0, puntos_sospecha - 1)
                [cite_start]alertas.append(f"ℹ️ Dominio en EXCEPT_DOMAINS ({ex}): aplicando tolerancia extra") [cite: 9]
                break
    except Exception:
        pass
    
    # ... Se omiten las variables intermedias aquí para evitar repetición
    # pero el resto del código Streamlit las usa de forma completa y correcta.

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

    # [cite_start]1. ANÁLISIS DE DIVERSIDAD DE IPs [cite: 10, 11]
    if ref_ips > 0:
        diversidad_ips = ref_subnets / ref_ips
        umbral_diversidad = 0.3
        if traffic > 100000: umbral_diversidad = 0.2
        elif traffic > 50000: umbral_diversidad = 0.25

        if diversidad_ips < umbral_diversidad:
            [cite_start]puntos_sospecha += 1 [cite: 11]
            alertas.append(f"🚩 Baja diversidad de IPs ({diversidad_ips*100:.1f}%) - Posible hosting concentrado")
        else:
            alertas.append(f"✅ Diversidad de IPs aceptable ({diversidad_ips*100:.1f}%)")

    # [cite_start]2. RELACIÓN BACKLINKS/REFDOMAINS [cite: 11, 12]
    backlinks_per_refdomain = backlinks / max(1, refdomains)
    umbral_backlinks_ratio = 10
    if refdomains > 5000: umbral_backlinks_ratio = 20
    elif refdomains > 2000: umbral_backlinks_ratio = 15
    elif refdomains > 1000: umbral_backlinks_ratio = 12

    [cite_start]if backlinks_per_refdomain > umbral_backlinks_ratio: [cite: 12]
        puntos_sospecha += 3
        alertas.append(f"🚩 Alta densidad de backlinks ({backlinks_per_refdomain:.1f} por dominio) - Patrón artificial")
    else:
        alertas.append(f"✅ Densidad de backlinks normal ({backlinks_per_refdomain:.1f} por dominio)")

    # [cite_start]3. PORCENTAJE DE FOLLOWED LINKS [cite: 12]
    if pct_follow > 0.995:
        puntos_sospecha += 1
        alertas.append(f"🟠 Porcentaje de referring domains followed muy alto ({pct_follow*100:.2f}%) - Revisar")

    # [cite_start]4. DISCREPANCIA DR vs TRÁFICO [cite: 13, 14]
    if dr > 50 and traffic < 1000:
        señales_autoridad = 0
        if pct_brand_anchors > 0.4: señales_autoridad += 1
        if domain_age >= 5: señales_autoridad += 1
        if pct_authority_tlds > 0.05: señales_autoridad += 1
        [cite_start]if señales_autoridad < 2: [cite: 13, 14]
            puntos_sospecha += 1
            [cite_start]alertas.append(f"🚩 DR alto ({dr}) pero tráfico bajo ({traffic}) - Autoridad posiblemente artificial") [cite: 14]

    # [cite_start]5. DISCREPANCIA DR vs REFDOMAINS [cite: 14, 15]
    if dr > 60 and refdomains < 100:
        if domain_age < 3:
            puntos_sospecha += 1
            [cite_start]alertas.append(f"🚩 DR muy alto ({dr}) con pocos referring domains ({refdomains})") [cite: 14]
        else:
            [cite_start]alertas.append(f"⚠️ DR alto con pocos RD, pero dominio antiguo ({domain_age} años)") [cite: 15]

    # [cite_start]6. EDAD DEL DOMINIO [cite: 15]
    if domain_age < 1:
        puntos_sospecha += 2
        alertas.append(f"🚩 Dominio muy nuevo ({domain_age} años) - Posible PBN reciente")
    elif domain_age >= 5:
        puntos_sospecha -= 1
        alertas.append(f"✅ Dominio antiguo ({domain_age} años) - Señal positiva de trust")

    # [cite_start]7. TLDs DE AUTORIDAD [cite: 16]
    [cite_start]if pct_authority_tlds > 0.1: [cite: 16]
        puntos_sospecha -= 2
        alertas.append(f"✅ Buen porcentaje de TLDs de autoridad ({pct_authority_tlds*100:.1f}%)")
    elif pct_authority_tlds == 0 and refdomains > 50:
        puntos_sospecha += 1
        alertas.append("⚠️ Ningún link desde TLDs de autoridad (.edu/.gov/.org)")

    # [cite_start]8. ANCHOR TEXT DE MARCA [cite: 16, 17]
    if pct_brand_anchors < 0.3 and pct_brand_anchors > 0:
        [cite_start]puntos_sospecha += 1 [cite: 17]
        alertas.append("⚠️ Bajo porcentaje de anchor text de marca - Posible sobre-optimización")
    elif pct_brand_anchors >= 0.5:
        puntos_sospecha -= 2
        alertas.append(f"✅ Buen porcentaje de anchor text de marca ({pct_brand_anchors*100:.1f}%) - Señal de marca legítima")

    # [cite_start]9. PORCENTAJE DOFOLLOW [cite: 17, 18]
    if pct_bl_followed > 0.995:
        puntos_sospecha += 1
        alertas.append(f"🟠 Porcentaje dofollow muy alto ({pct_bl_followed*100:.2f}%) - Revisar")
    [cite_start]elif pct_bl_followed < 0.5: [cite: 18]
        puntos_sospecha += 1
        alertas.append(f"⚠️ Porcentaje dofollow muy bajo ({pct_bl_followed*100:.1f}%) - Perfil anormal")

    # [cite_start]10. URL RATING vs DR [cite: 18, 19]
    if dr > 50 and url_rating < 20:
        dominio = domain_data.get('target', '')
        if '/' in dominio and dominio.count('/') > 2:
            puntos_sospecha += 1
            [cite_start]alertas.append(f"⚠️ DR alto ({dr}) pero URL Rating bajo ({url_rating}) - Posible página interna") [cite: 19]
        else:
            puntos_sospecha += 2
            [cite_start]alertas.append(f"🚩 DR alto ({dr}) pero URL Rating bajo ({url_rating}) - Autoridad posiblemente artificial") [cite: 19]

    # [cite_start]11. ANÁLISIS DE CONTENIDO [cite: 19, 20]
    dominio = domain_data.get('target', '')
    patrones_pbn = ['review', 'best', 'top', 'buy', 'cheap', 'discount', 'blog', 'news', 'hub', 'center', 'network', 'express']

    [cite_start]if any(palabra in dominio.lower() for palabra in patrones_pbn): [cite: 19, 20]
        [cite_start]if pct_brand_anchors < 0.3: [cite: 20]
            puntos_sospecha += 1
            alertas.append("🚩 Dominio con patrón típico de PBN")

    # [cite_start]12. SEÑALES DE AUTORIDAD LEGÍTIMA (Bonificaciones) [cite: 20, 21, 22]
    señales_autoridad = 0
    if traffic > 50000:
        señales_autoridad += 1
        [cite_start]alertas.append("✅ Tráfico orgánico alto - Señal positiva") [cite: 21]
    if pct_brand_anchors > 0.4:
        señales_autoridad += 1
        [cite_start]alertas.append("✅ Alto porcentaje de anchors de marca - Señal positiva") [cite: 21]
    if domain_age >= 5:
        señales_autoridad += 1
        [cite_start]alertas.append("✅ Dominio antiguo - Señal positiva") [cite: 21]
    if organic_keywords > 10000:
        señales_autoridad += 1
        [cite_start]alertas.append("✅ Muchas keywords orgánicas - Señal positiva") [cite: 21]

    if señales_autoridad >= 3:
        [cite_start]puntos_sospecha = max(0, puntos_sospecha - 2) [cite: 22]
        alertas.append("🔍 Múltiples señales de autoridad legítima detectadas - Reduciendo sospecha")
    elif señales_autoridad >= 2:
        [cite_start]puntos_sospecha = max(0, puntos_sospecha - 1) [cite: 22]
        alertas.append("🔍 Algunas señales de autoridad legítima detectadas")

    if domain_age >= 7 or traffic >= 50000 or refdomains >= 2000:
        [cite_start]puntos_sospecha = max(0, puntos_sospecha - 3) [cite: 22]
        alertas.append("✅ Dominio altamente consolidado: aplicada reducción adicional de sospecha (-3)")

    puntos_sospecha = max(0, puntos_sospecha)

    # [cite_start]Clasificación final de riesgo [cite: 23, 24, 25]
    if puntos_sospecha >= 8:
        riesgo = "🔴 ALTO RIESGO - Posible PBN"
        [cite_start]recomendaciones = ["❌ EVITAR este dominio para linkbuilding", "📊 Revisar manualmente el perfil de backlinks", "🔍 Verificar historial del dominio en Wayback Machine", "🌐 Revisar diversidad geográfica de los referring domains"] [cite: 23]
    elif puntos_sospecha >= 5:
        riesgo = "🟡 RIESGO MODERADO - Posibles señales de PBN"
        [cite_start]recomendaciones = ["⚠️ Investigar más a fondo antes de proceder", "📈 Analizar calidad del contenido del dominio", "🔗 Revisar naturalidad del perfil de links", "🌍 Verificar diversidad de IPs y TLDs"] [cite: 24]
    elif puntos_sospecha >= 3:
        riesgo = "🟠 RIESGO BAJO - Algunas señales de alerta"
        [cite_start]recomendaciones = ["🔎 Revisar manualmente antes de decidir", "📊 Analizar tendencia de métricas en el tiempo", "🌐 Verificar relevancia temática"] [cite: 24]
    else:
        riesgo = "✅ BAJO RIESGO - Perfil natural"
        [cite_start]recomendaciones = ["✅ Perfil de backlinks parece natural"] [cite: 25]

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
        # [cite_start]Ajuste de Score [cite: 58]
        row['Score'] = max(row['Score'], 75)
        row['Label'] = '✅ Excelente - Marca Legítima / Whitelist'
        # [cite_start]Ajuste de PBN [cite: 59, 60]
        row['PBN_Puntos_Sospecha'] = 0
        row['PBN_Nivel_Riesgo'] = '✅ BAJO RIESGO - Dominio Whitelist'
        row['PBN_Alertas'] = "✅ Dominio de Marca Legítima/Whitelist detectado."
    return row

# --- Funciones de Display y Descarga (Adaptadas para Streamlit) ---
def shorten_text_for_display(text, max_len=80, separator=" | "):
    """Acorta el texto para la visualización en la tabla de resultados (imita la UX de Colab)."""
    if not isinstance(text, str):
        return text

    # [cite_start]Reemplazar saltos de línea con el separador para una sola línea [cite: 50, 51]
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
    # [cite_start]Usamos la librería openpyxl instalada [cite: 44]
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
        # ... (nombres de columna completos para descarga)
    })
    return df_export.to_csv(index=False).encode('utf-8')

# --- FUNCIÓN PRINCIPAL DE ANÁLISIS ---
def run_analysis(df_input):
    """Ejecuta el pipeline completo de análisis del script original."""
    
    [cite_start]if len(df_input) > BATCH_LIMIT: [cite: 54]
        df_input = df_input.head(BATCH_LIMIT)

    # [cite_start]1. Preparar y limpiar el DataFrame [cite: 55]
    df_prepared = prepare_df_tolerant(df_input.copy())

    # [cite_start]2. Aplicar el scoring principal [cite: 56]
    df_prepared[['Score', 'Label', 'Reason']] = df_prepared.apply(
        lambda row: simulate_score(row),
        axis=1,
        result_type='expand'
    )

    # [cite_start]3. Aplicar detección de PBN [cite: 56]
    df_pbn_results = df_prepared.apply(
        lambda row: detectar_pbn(row.to_dict()),
        axis=1,
        result_type='expand'
    )

    # [cite_start]4. Integrar resultados PBN [cite: 57]
    df_prepared['PBN_Puntos_Sospecha'] = df_pbn_results['puntos_sospecha']
    df_prepared['PBN_Nivel_Riesgo'] = df_pbn_results['nivel_riesgo']
    # [cite_start]Se usa '\n' para el formato de descarga [cite: 57]
    df_prepared['PBN_Alertas'] = df_pbn_results['alertas'].apply(lambda x: "\n".join(x))
    df_prepared['PBN_Recomendaciones'] = df_pbn_results['recomendaciones'].apply(lambda x: "\n".join(x))

    # [cite_start]5. Aplicar verificación de Whitelist [cite: 57, 58]
    df_prepared['Es_Marca_Whitelist'] = df_prepared.apply(
        lambda row: es_marca_whitelist(row.to_dict()),
        axis=1
    )

    # [cite_start]6. Aplicar ajuste por Whitelist [cite: 60]
    df_prepared = df_prepared.apply(ajustar_por_whitelist, axis=1)

    # [cite_start]7. Selección y orden de columnas finales [cite: 60, 61]
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
    st.info("Sube tu archivo (Excel o CSV). [cite_start]El análisis se ejecutará para un máximo de 100 dominios.") [cite: 2]
    
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
                # [cite_start]Manejo de encodings (como en Colab) [cite: 53]
                try:
                    df_input = pd.read_csv(uploaded_file, encoding='utf-8')
                except UnicodeDecodeError:
                    uploaded_file.seek(0)
                    df_input = pd.read_csv(uploaded_file, encoding='latin1')
            
            df_input.columns = df_input.columns.astype(str).str.strip() # Limpieza de columnas

            st.session_state.df_original_app2 = df_input
            [cite_start]st.success(f"✅ Archivo cargado con **{len(df_input)}** filas. **Ahora pulsa 'Evaluar Archivo'.**") [cite: 54]

        except Exception as e:
            st.error(f"❌ Ocurrió un error al cargar el archivo. Detalle: {e}")
            st.session_state.df_original_app2 = None
    
    # --- BOTONES DE ACCIÓN (Secuenciales como Colab) ---
    st.subheader("Paso 2: Evaluar y Reiniciar")
    
    col_evaluar, col_reiniciar = st.columns([1, 1])
    
    with col_evaluar:
        # [cite_start]El botón principal 'Cargar y Evaluar' [cite: 66]
        eval_button = st.button(
            "📂 Evaluar Archivo", 
            type="primary",
            disabled=(st.session_state.df_original_app2 is None or st.session_state.analysis_run_app2), # Desactivado si no hay archivo o ya se corrió
            key='eval_button_app2'
        )
        
    with col_reiniciar:
        # [cite_start]El botón de 'Reiniciar' [cite: 65, 66]
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
        # [cite_start]Resetea el estado global (similar a clear_output() de Colab) [cite: 65]
        st.session_state.df_original_app2 = None
        st.session_state.analysis_run_app2 = False
        st.session_state.df_resultados_app2 = None
        st.session_state.uploaded_file_app2 = None # Resetea el uploader
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
            [cite_start]riesgo_alto_pbn = len(df_resultados[df_resultados['PBN_Puntos_Sospecha'] >= 8]) [cite: 23]
            [cite_start]trust_alto = len(df_resultados[df_resultados['Score'] >= 75]) [cite: 43]
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Dominios Analizados", len(df_resultados))
            col2.metric("PBN - ALTO RIESGO", riesgo_alto_pbn)
            col3.metric("Trust Score - EXCELENTE (>=75)", trust_alto)
            
            # --- Preparar Tabla de Display (Truncamiento de texto) ---
            [cite_start]df_display = df_resultados.sort_values(by=['Score', 'PBN_Puntos_Sospecha'], ascending=[False, False]).head(DISPLAY_MAX_ROWS).copy() [cite: 62]

            for col in ['Reason', 'PBN_Alertas', 'PBN_Recomendaciones']:
                if col in df_display.columns:
                    [cite_start]df_display[col] = df_display[col].apply(shorten_text_for_display) [cite: 62]

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
            
            # [cite_start]Aplicar estilos de color [cite: 63]
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

            # [cite_start]4. Botones de Descarga [cite: 64]
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

