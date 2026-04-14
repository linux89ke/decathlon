import os, io, re, json, asyncio
from typing import Optional
import numpy as np
import streamlit as st
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from groq import AsyncGroq, Groq as SyncGroq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

st.set_page_config(page_title="Decathlon Template Generator", page_icon="👟", layout="wide")

# Custom CSS for Red-Flagging
st.markdown("""
<style>
    .st-emotion-cache-16idsyz p { font-size: 14px; }
    .invalid-size-row { background-color: #ffcccc; }
</style>
""", unsafe_allow_html=True)

# Constants & Paths
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

# --- Utility Functions ---

def _clean(val) -> str:
    return str(val).strip() if pd.notna(val) and str(val).strip() not in ("", "nan", "-") else ""

def _format_gtin(val) -> str:
    raw = _clean(val)
    try:
        return str(int(float(raw))) if raw else ""
    except:
        return raw

def parse_valid_sizes():
    if os.path.exists(SIZES_PATH):
        with open(SIZES_PATH, "r", encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip() and not l.startswith("#")]
    return []

def get_variation(row: pd.Series, is_fashion: bool, valid_sizes: list, override: str = None) -> str:
    if override: return override
    
    # Both Fashion and Other now look at the 'size' column per request
    raw = _clean(row.get("size", ""))
    if not raw or raw.lower() in ("no size", "none"):
        return "..."
    
    if is_fashion and valid_sizes:
        # Check for exact or UK match
        raw_up = raw.upper()
        if any(s.upper() == raw_up for s in valid_sizes):
            return raw
        # Fallback to extraction
        uk_match = re.search(r'\bUK\s*(\d{1,2}(?:\.\d)?)\b', raw, re.I)
        if uk_match:
            uk_val = f"UK {uk_match.group(1)}"
            if any(s.upper() == uk_val.upper() for s in valid_sizes):
                return uk_val
    return raw

# --- Excel Processing ---

def build_template(df, df_cat, df_brands, ai_cats, descs, is_fashion, valid_sizes, overrides):
    wb = load_workbook(TEMPLATE_PATH)
    # Delete other sheets if they exist to save time/space
    for sheet in wb.sheetnames:
        if sheet != "Upload Template":
            del wb[sheet]
    
    ws = wb["Upload Template"]
    headers = {ws.cell(1, c).value: c for c in range(1, ws.max_column + 1) if ws.cell(1, c).value}
    
    red_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
    
    # Export code to full path mapping
    exp_to_path = dict(zip(df_cat["export_category"], df_cat["Category Path"])) if df_cat is not None else {}

    for i, (_, src_row) in enumerate(df.iterrows()):
        row_idx = i + 2
        sku = _clean(src_row.get("sku_num_sku_r3"))
        
        # Variation Logic
        size_override = overrides.get(sku)
        final_var = get_variation(src_row, is_fashion, valid_sizes, size_override)
        
        # Name Logic (The "Dark Grey" Fix)
        name = _clean(src_row.get("product_name"))
        color_raw = _clean(src_row.get("color", ""))
        colors = [c.strip() for c in color_raw.split("|") if c.strip()]
        
        if colors and name:
            # Only append if NO part of the color string is already in the name
            if not any(c.lower() in name.lower() for c in colors):
                name = f"{name} - {colors[0].title()}"

        data = {
            "SellerSKU": sku,
            "Name": name,
            "Price_KES": "100000",
            "Stock": "0",
            "variation": final_var,
            "color_family": colors[0] if colors else "",
            "short_description": descs[i] if descs else "",
            "product_weight": re.sub(r'kg', '', _clean(src_row.get("business_weight")), flags=re.I).strip(),
            "GTIN_Barcode": _format_gtin(src_row.get("bar_code"))
        }

        # Categories
        cat_pair = ai_cats[i] if ai_cats else (None, None)
        if cat_pair[0]: data["PrimaryCategory"] = exp_to_path.get(cat_pair[0], cat_pair[0])
        if cat_pair[1]: data["AdditionalCategory"] = exp_to_path.get(cat_pair[1], cat_pair[1])

        # Write to Sheet
        for k, v in data.items():
            if k in headers:
                ws.cell(row_idx, headers[k]).value = v
        
        # Shade Red if size is invalid
        if is_fashion and valid_sizes and final_var not in valid_sizes:
            for c in range(1, ws.max_column + 1):
                ws.cell(row_idx, c).fill = red_fill

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# --- Main App ---

st.title("👟 Decathlon Template Creator")

# Sidebar
with st.sidebar:
    st.header("Settings")
    prod_type = st.radio("Product Type", ["Fashion", "Other"])
    is_fash = prod_type == "Fashion"
    
    st.markdown("---")
    use_ai = st.toggle("Use AI (Groq) for Categories", False)
    api_key = st.text_input("Groq API Key", type="password") if use_ai else ""

# Load Master Data & Sizes
valid_sizes = parse_valid_sizes()
df_master = pd.read_csv(MASTER_PATH, dtype=str) if os.path.exists(MASTER_PATH) else None
df_cat = pd.read_excel(DECA_CAT_PATH) if os.path.exists(DECA_CAT_PATH) else None

# Search Logic
manual_skus = st.text_area("Enter SKUs (one per line)")
if manual_skus:
    query_list = [s.strip() for s in manual_skus.splitlines() if s.strip()]
    results = df_master[df_master["sku_num_sku_r3"].isin(query_list)].copy()
    
    if not results.empty:
        st.subheader("Results & Editor")
        
        # In-memory overrides for the current session
        if "size_overrides" not in st.session_state:
            st.session_state.size_overrides = {}

        # Table Display
        display_df = results.copy()
        
        # Compute "Live" columns
        primary_cats = [] # Logic to match categories would go here
        
        # Table with Overrides
        for idx, row in results.iterrows():
            sku = row["sku_num_sku_r3"]
            col1, col2, col3 = st.columns([2, 4, 2])
            
            with col1:
                st.write(f"**{sku}**")
                st.caption(row["product_name"][:50])
            
            with col2:
                # Show Category in Full
                st.info("Primary Category: [Full Path Logic Here]")
                
            with col3:
                # Per-row size override
                current_size = get_variation(row, is_fash, valid_sizes)
                is_invalid = is_fash and current_size not in valid_sizes
                
                label = f"Size {'⚠️' if is_invalid else ''}"
                st.session_state.size_overrides[sku] = st.selectbox(
                    label,
                    options=[current_size] + valid_sizes if current_size not in valid_sizes else valid_sizes,
                    key=f"size_{sku}"
                )

        # Download
        st.markdown("---")
        if st.button("Generate Template", type="primary"):
            # This triggers the build_template with the overrides from session_state
            final_bytes = build_template(
                results, df_cat, None, None, None, is_fash, valid_sizes, st.session_state.size_overrides
            )
            st.download_button(
                "Click to Download Upload Template",
                data=final_bytes,
                file_name="decathlon_upload.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.error("No SKUs found in master file.")
