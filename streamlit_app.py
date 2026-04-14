import os, io, re, json, asyncio
from typing import Optional
import numpy as np
import streamlit as st
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment, PatternFill
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from groq import AsyncGroq, Groq as SyncGroq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

# --- UI Configuration ---
st.set_page_config(page_title="Decathlon Product Lookup", page_icon="⚡", layout="wide")
st.markdown("""
<style>
h1 { color: #0082C3; }
.tag { display:inline-block; background:#0082C3; color:white; border-radius:4px; padding:2px 8px; font-size:12px; margin:2px; }
.ai-badge { display:inline-block; background:linear-gradient(90deg,#f55036,#ff8c00); color:white; border-radius:12px; padding:2px 10px; font-size:11px; font-weight:700; margin-left:6px; }
.kw-badge { display:inline-block; background:#0082C3; color:white; border-radius:12px; padding:2px 10px; font-size:11px; font-weight:700; margin-left:6px; }
</style>
""", unsafe_allow_html=True)

st.title("Decathlon Product Lookup")

# --- Constants ---
IMAGE_COLS = ["OG_image"] + [f"picture_{i}" for i in range(1, 11)]
TEMPLATE_PATH = "product-creation-template.xlsx"
DECA_CAT_PATH = "deca_cat.xlsx"
MASTER_PATH = "Decathlon_Working_File_Split.csv"
SIZES_PATH = "sizes.txt"

MASTER_TO_TEMPLATE = {
    "product_name": "Name",
    "designed_for": "Description",
    "sku_num_sku_r3": "SellerSKU",
    "brand_name": "Brand",
    "bar_code": "GTIN_Barcode",
    "color": "color",
    "model_label": "model",
    "OG_image": "MainImage",
    "picture_1": "Image2",
    "picture_2": "Image3",
    "picture_3": "Image4",
    "picture_4": "Image5",
    "picture_5": "Image6",
    "picture_6": "Image7",
    "picture_7": "Image8",
}

# --- Logic: UK Size Extraction ---
_UK_SIZE_PATTERNS = [
    re.compile(r'\bUK\s*(\d{1,2}(?:\.\d)?)\b', re.IGNORECASE),
    re.compile(r'\bUK\s*(\d{1,2}(?:\.\d)?)\s*[-–]\s*\d{1,2}', re.IGNORECASE),
]

def extract_uk_size(raw: str) -> Optional[str]:
    if not raw: return None
    cleaned = re.sub(r'"+', '', raw).strip()
    for pat in _UK_SIZE_PATTERNS:
        m = pat.search(cleaned)
        if m: return f"UK {m.group(1)}"
    return None

def parse_valid_sizes(path: str) -> list:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        return []

# --- Logic: Category & Brand Matchers ---
def _clean(val) -> str:
    if pd.isna(val) or str(val).strip() in ("", "-", "nan"): return ""
    return str(val).strip()

def _format_gtin(val) -> str:
    raw = str(val).strip()
    if not raw or raw.lower() in ("nan", ""): return ""
    try: return str(int(float(raw)))
    except: return raw

def match_brand(raw: str, df_brands: pd.DataFrame) -> str:
    if not raw or pd.isna(raw): return ""
    needle = str(raw).strip().lower()
    for _, brow in df_brands.iterrows():
        if brow["brand_name_lower"] in needle or needle in brow["brand_name_lower"]:
            return brow["brand_entry"]
    return str(raw).strip()

# --- Variation Logic ---
def get_variation(row, is_fashion, valid_sizes, size_override) -> str:
    if not is_fashion:
        raw = re.sub(r'"+', '', str(row.get("variation", ""))).strip()
        return raw if raw.lower() not in ("", "nan", "none") else "..."
    
    if size_override: return size_override
    raw = re.sub(r'"+', '', str(row.get("size", ""))).strip()
    
    if valid_sizes:
        if raw in valid_sizes: return raw
        uk = extract_uk_size(raw)
        if uk in valid_sizes: return uk
        for s in valid_sizes:
            if s.lower() in raw.lower(): return s
            
    return raw if raw.lower() not in ("", "nan", "none") else "..."

# --- Rule-Based Short Description ---
GENDER_MAP = {"MEN'S": "Men", "WOMEN'S": "Women", "BOYS'": "Boys", "GIRLS'": "Girls", "UNISEX": "Unisex"}

def rule_based_short_desc(row: pd.Series) -> str:
    brand = _clean(row.get("brand_name", "")).title()
    ptype = _clean(row.get("type", "")).title()
    g_raw = _clean(row.get("channable_gender", "")).split("|")[0].strip().upper()
    gender = GENDER_MAP.get(g_raw, g_raw.title())
    color = _clean(row.get("color", "")).split("|")[0].strip().title()
    
    bullets = [f"{brand} {ptype} for {gender}", f"Design: {color}", "High-quality sports gear"]
    items = "".join(f"<li>{b}</li>" for b in bullets)
    return f"<ul>{items}</ul>"

# --- TEMPLATE BUILDER (Optimized for Single Sheet + Red Shading) ---
def build_template(
    results_df, df_cat, df_brands,
    ai_categories,
    short_descs,
    is_fashion: bool = True,
    valid_sizes: Optional[list] = None,
    size_override: Optional[str] = None,
) -> bytes:
    # 1. Load original headers to ensure compatibility
    try:
        orig_wb = load_workbook(TEMPLATE_PATH, read_only=True)
        headers = [cell.value for cell in orig_wb["Upload Template"][1]]
        orig_wb.close()
    except:
        headers = list(MASTER_TO_TEMPLATE.values()) + ["ParentSKU", "PrimaryCategory", "AdditionalCategory", "variation", "price", "color_family", "short_description", "product_weight", "package_content"]

    # 2. Initialize fresh Workbook (much faster)
    wb = Workbook()
    ws = wb.active
    ws.title = "Upload Template"
    ws.append(headers)
    
    header_map = {name: i+1 for i, name in enumerate(headers)}
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    data_font = Font(name="Calibri", size=11)

    # Pre-calculate lookups
    model_to_first_sku = results_df.groupby("model_code")["sku_num_sku_r3"].first().to_dict()
    exp_to_fullpath = dict(zip(df_cat["export_category"], df_cat["Category Path"])) if df_cat is not None else {}

    for i, (_, src_row) in enumerate(results_df.iterrows()):
        row_idx = i + 2
        row_data = {}

        # Basic Mapping
        for m_col, t_col in MASTER_TO_TEMPLATE.items():
            row_data[t_col] = _clean(src_row.get(m_col))

        # Specialized Fields
        row_data["ParentSKU"] = model_to_first_sku.get(str(src_row.get("model_code", "")))
        row_data["GTIN_Barcode"] = _format_gtin(src_row.get("bar_code"))
        row_data["price"] = "100000"
        
        # Categories
        p_code, s_code = ai_categories[i] if ai_categories else ("", "")
        row_data["PrimaryCategory"] = exp_to_fullpath.get(p_code, p_code)
        row_data["AdditionalCategory"] = exp_to_fullpath.get(s_code, s_code)

        # Variation & Weight
        var_val = get_variation(src_row, is_fashion, valid_sizes, size_override)
        row_data["variation"] = var_val
        row_data["short_description"] = short_descs[i] if short_descs else ""
        
        bw = _clean(src_row.get("business_weight"))
        row_data["product_weight"] = re.sub(r'\s*kg\s*$', '', bw, flags=re.IGNORECASE)

        # Write to Sheet
        for t_col, val in row_data.items():
            if t_col in header_map:
                cell = ws.cell(row=row_idx, column=header_map[t_col], value=val)
                cell.font = data_font
                
                # SHADING LOGIC: If fashion and size is not in valid_sizes, shade red
                if t_col == "variation" and is_fashion:
                    if valid_sizes and var_val not in valid_sizes:
                        cell.fill = red_fill
                    elif var_val == "...":
                        cell.fill = red_fill

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# --- AI & Data Loading Helpers (Condensed) ---
@st.cache_data
def load_reference_data(file_bytes):
    wb = io.BytesIO(file_bytes)
    df_cat = pd.read_excel(wb, sheet_name="category", dtype=str)
    df_cat["Category Path"] = df_cat["Category Path"].fillna("")
    df_brands = pd.read_excel(wb, sheet_name="brands", dtype=str)
    df_brands.columns = ["brand_entry"]
    df_brands["brand_name_lower"] = df_brands["brand_entry"].str.split("-").str[-1].str.lower().str.strip()
    return df_cat, df_brands

@st.cache_data
def load_master(file_bytes, is_csv):
    return pd.read_csv(io.BytesIO(file_bytes), dtype=str) if is_csv else pd.read_excel(io.BytesIO(file_bytes), dtype=str)

# =============================================================================
# MAIN APP FLOW
# =============================================================================

with st.sidebar:
    st.header("Settings")
    uploaded_master = st.file_uploader("Master File", type=["xlsx", "csv"])
    product_type = st.radio("Type", ["Fashion", "Other"])
    is_fashion = product_type == "Fashion"
    
    uploaded_sizes = st.file_uploader("sizes.txt", type=["txt"])
    valid_sizes = []
    if uploaded_sizes:
        valid_sizes = [l.strip() for l in uploaded_sizes.read().decode().splitlines() if l.strip()]
    
    size_override = None
    if is_fashion and valid_sizes:
        size_choice = st.selectbox("Global Size Override", ["(None)"] + valid_sizes)
        if size_choice != "(None)": size_override = size_choice

# Data Initialization
df_cat, df_brands = None, None
try:
    ref_bytes = open(DECA_CAT_PATH, "rb").read()
    df_cat, df_brands = load_reference_data(ref_bytes)
except: st.error("Missing deca_cat.xlsx")

if uploaded_master:
    df_master = load_master(uploaded_master.read(), uploaded_master.name.endswith(".csv"))
    
    # Simple Search
    query = st.text_area("Enter SKUs (one per line)")
    if query:
        skus = [s.strip() for s in query.splitlines() if s.strip()]
        results = df_master[df_master["sku_num_sku_r3"].isin(skus)].copy()
        
        if not results.empty:
            st.success(f"Found {len(results)} matches.")
            
            # Generate temporary data for preview
            short_descs = [rule_based_short_desc(r) for _, r in results.iterrows()]
            
            if st.button("Generate Template"):
                tpl = build_template(results, df_cat, df_brands, None, short_descs, is_fashion, valid_sizes, size_override)
                st.download_button("Download Upload Template", tpl, "upload_template.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                st.info("💡 Fashion sizes not found in sizes.txt are highlighted RED in the Excel file.")
