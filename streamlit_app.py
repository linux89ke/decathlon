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

# Setup
st.set_page_config(page_title="Decathlon Template Generator", page_icon="👟", layout="wide")
st.markdown("""
<style>
h1 { color: #0082C3; }
.invalid-row { background-color: #ffcccc !important; padding: 10px; border-radius: 5px; margin-bottom: 5px; }
</style>
""", unsafe_allow_html=True)

# Constants 
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
}

# --- HELPERS ---

def _clean(val) -> str:
    if pd.isna(val) or str(val).strip() in ("", "-", "nan"):
        return ""
    return str(val).strip()

def parse_valid_sizes():
    if os.path.exists(SIZES_PATH):
        with open(SIZES_PATH, "r", encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip() and not l.startswith("#")]
    return []

def get_variation(row: pd.Series, is_fashion: bool, valid_sizes: list, override: str = None) -> str:
    if override and override != "(auto)": return override
    
    raw = re.sub(r'"+', '', _clean(row.get("size", ""))).strip().rstrip(".")
    if not raw or raw.lower() in ("no size", "none"):
        return "..."
    
    if is_fashion and valid_sizes:
        if any(s.lower() == raw.lower() for s in valid_sizes):
            return next(s for s in valid_sizes if s.lower() == raw.lower())
        uk_match = re.search(r'\bUK\s*(\d{1,2}(?:\.\d)?)\b', raw, re.I)
        if uk_match:
            uk_val = f"UK {uk_match.group(1)}"
            if any(s.lower() == uk_val.lower() for s in valid_sizes):
                return next(s for s in valid_sizes if s.lower() == uk_val.lower())
    return raw

# --- EXCEL BUILDER ---

def build_template(results_df, df_cat, is_fashion, valid_sizes, sku_overrides, ai_categories=None):
    wb = load_workbook(TEMPLATE_PATH)
    # Only output the Upload Template sheet to save time
    for sheet in wb.sheetnames:
        if sheet != "Upload Template":
            del wb[sheet]
    
    ws = wb["Upload Template"]
    header_map = {ws.cell(1, c).value: c for c in range(1, ws.max_column + 1) if ws.cell(1, c).value}
    red_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
    exp_to_fullpath = dict(zip(df_cat["export_category"], df_cat["Category Path"])) if df_cat is not None else {}

    for i, (_, src_row) in enumerate(results_df.iterrows()):
        row_idx = i + 2
        sku = _clean(src_row.get("sku_num_sku_r3"))
        
        # Name logic: Fix "Dark Grey - Graphite Grey" bug
        name = _clean(src_row.get("product_name"))
        color_raw = _clean(src_row.get("color", ""))
        color_parts = [c.strip() for c in color_raw.split("|") if c.strip()]
        if color_parts and name:
            if not any(c.lower() in name.lower() for c in color_parts):
                name = f"{name} - {color_parts[0].title()}"

        var_val = get_variation(src_row, is_fashion, valid_sizes, sku_overrides.get(sku))

        row_data = {
            "SellerSKU": sku,
            "Name": name,
            "Price_KES": "100000",
            "Stock": "0",
            "variation": var_val,
            "GTIN_Barcode": _clean(src_row.get("bar_code"))
        }

        if ai_categories:
            prim_code = ai_categories[i][0]
            row_data["PrimaryCategory"] = exp_to_fullpath.get(prim_code, prim_code)

        for tmpl_col, value in row_data.items():
            if tmpl_col in header_map:
                cell = ws.cell(row=row_idx, column=header_map[tmpl_col])
                cell.value = value
                # Shade Red if size is invalid
                if is_fashion and valid_sizes and var_val != "..." and var_val not in valid_sizes:
                    cell.fill = red_fill

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# --- MAIN UI ---

valid_sizes = parse_valid_sizes()
df_master = pd.read_csv(MASTER_PATH, dtype=str) if os.path.exists(MASTER_PATH) else None
df_cat = pd.read_excel(DECA_CAT_PATH) if os.path.exists(DECA_CAT_PATH) else None

with st.sidebar:
    st.header("Settings")
    prod_type = st.radio("Product Type", ["Fashion", "Other"])
    is_fash = prod_type == "Fashion"
    st.info(f"Loaded {len(valid_sizes)} valid sizes from local project folder.")

manual_skus = st.text_area("Enter SKUs (one per line)")
if manual_skus:
    skus = [s.strip() for s in manual_skus.splitlines() if s.strip()]
    results = df_master[df_master["sku_num_sku_r3"].isin(skus)].copy() if df_master is not None else pd.DataFrame()

    if not results.empty:
        if "sku_overrides" not in st.session_state:
            st.session_state.sku_overrides = {}

        st.subheader(f"Found {len(results)} SKUs")
        for idx, row in results.iterrows():
            sku = row["sku_num_sku_r3"]
            current_var = get_variation(row, is_fash, valid_sizes, st.session_state.sku_overrides.get(sku))
            is_invalid = is_fash and current_var != "..." and current_var not in valid_sizes
            
            # Front end "Red Shade" if missing/wrong size
            row_div = '<div class="invalid-row">' if is_invalid else '<div>'
            st.markdown(row_div, unsafe_allow_html=True)
            
            c1, c2, c3 = st.columns([3, 4, 2])
            with c1:
                st.write(f"**{sku}**")
                st.caption(row.get("product_name", ""))
            with c2:
                # Show Primary Category in Full
                st.write("**Primary Category:**")
                st.write("Hiking / Footwear / Shoes") # Placeholder for mapping logic
            with c3:
                if is_fash:
                    st.session_state.sku_overrides[sku] = st.selectbox(
                        f"Size for {sku}", ["(auto)"] + valid_sizes, key=f"size_{sku}"
                    )
                else:
                    st.write(f"Size: {current_var}")
            st.markdown('</div>', unsafe_allow_html=True)
            st.divider()

        if st.button("Generate Upload Template Sheet Only", type="primary"):
            final_xlsx = build_template(results, df_cat, is_fash, valid_sizes, st.session_state.sku_overrides)
            st.download_button("Download Template", data=final_xlsx, file_name="decathlon_upload.xlsx")
