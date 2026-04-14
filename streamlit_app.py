import os, io, re, json, asyncio
import pandas as pd
import numpy as np
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill

# --- PAGE CONFIG ---
st.set_page_config(page_title="Decathlon Product Lookup", layout="wide")

# Custom CSS for Red Shading
st.markdown("""
<style>
    .red-row { background-color: #ffcccc !important; }
    h1 { color: #0082C3; }
</style>
""", unsafe_allow_html=True)

# --- CONSTANTS ---
TEMPLATE_PATH = "product-creation-template.xlsx"
DECA_CAT_PATH = "deca_cat.xlsx"
MASTER_PATH = "Decathlon_Working_File_Split.csv"
SIZES_PATH = "sizes.txt"

# --- CORE LOGIC IMPROVEMENTS ---

def parse_valid_sizes():
    if os.path.exists(SIZES_PATH):
        with open(SIZES_PATH, "r", encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip() and not l.startswith("#")]
    return []

def get_variation_refined(row, is_fashion, valid_sizes, manual_override=None):
    """
    If manual_override is provided via the UI, use it.
    Otherwise, pull from 'size'. If empty, return '...'.
    """
    if manual_override and manual_override != "(auto)":
        return manual_override
    
    raw_size = str(row.get("size", "")).strip()
    if not raw_size or raw_size.lower() in ("nan", "none", ""):
        return "..."
    
    # Clean quotes often found in Decathlon exports
    clean_size = raw_size.replace('"', '').strip()
    return clean_size

# --- TEMPLATE BUILDER (SINGLE SHEET ONLY) ---

def build_template_fast(results_df, df_cat, df_brands, merged_cats, short_descs, is_fashion, valid_sizes, overrides):
    wb = load_workbook(TEMPLATE_PATH)
    # Remove all sheets except the template one to save time/space
    for sheet in wb.sheetnames:
        if sheet != "Upload Template":
            wb.remove(wb[sheet])
    
    ws = wb["Upload Template"]
    header_map = {ws.cell(1, col).value: col for col in range(1, ws.max_column + 1) if ws.cell(1, col).value}
    
    # Lookup for Full Path
    exp_to_fullpath = dict(zip(df_cat["export_category"], df_cat["Category Path"])) if df_cat is not None else {}

    for i, (_, src_row) in enumerate(results_df.iterrows()):
        row_idx = i + 2
        mc = str(src_row.get("model_code", "")).strip()
        
        # Variation Logic
        final_size = get_variation_refined(src_row, is_fashion, valid_sizes, overrides.get(i))
        
        # Mapping Data
        data = {
            "SellerSKU": src_row.get("sku_num_sku_r3"),
            "Name": f"{src_row.get('product_name', '')} - {str(src_row.get('color', '')).split('|')[0].title()}",
            "GTIN_Barcode": src_row.get("bar_code"),
            "Brand": src_row.get("brand_name"),
            "Price_KES": 100000,
            "Stock": 0,
            "variation": final_size,
            "PrimaryCategory": exp_to_fullpath.get(merged_cats[i][0], merged_cats[i][0]),
            "short_description": short_descs[i] if i < len(short_descs) else ""
        }

        for key, val in data.items():
            if key in header_map:
                ws.cell(row=row_idx, column=header_map[key]).value = val

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# --- STREAMLIT UI ---

st.title("Decathlon Product Lookup")

# Sidebar settings
valid_sizes = parse_valid_sizes()
is_fashion = st.sidebar.radio("Mode", ["Fashion", "Other"]) == "Fashion"

# Data Loading (Master)
@st.cache_data
def load_data():
    # Use utf-8-sig to handle special characters like the hiking shoe dash
    df = pd.read_csv(MASTER_PATH, dtype=str, encoding="utf-8-sig") 
    cat = pd.read_excel(DECA_CAT_PATH, sheet_name="category", dtype=str)
    return df, cat

try:
    df_master, df_cat = load_data()
except:
    st.error("Files missing: Ensure Decathlon_Working_File_Split.csv and deca_cat.xlsx are in folder.")
    st.stop()

# Search
search_query = st.text_area("Enter SKUs (one per line)")
if search_query:
    sku_list = [s.strip() for s in search_query.splitlines() if s.strip()]
    results = df_master[df_master["sku_num_sku_r3"].isin(sku_list)].copy()

    if not results.empty:
        st.subheader("Final File Preview & Size Adjustment")
        st.caption("Rows shaded RED have sizes not found in sizes.txt")

        # Prepare Preview Data
        display_df = results.copy()
        
        # Categorization (Simplified keyword match for example)
        # In real app, this calls your existing keyword_match_batch
        
        individual_overrides = {}
        
        # Render Table with Individual Size Selectors
        for i, row in results.iterrows():
            current_size = row['size']
            is_valid = current_size in valid_sizes
            
            col1, col2, col3 = st.columns([2, 4, 2])
            
            with col1:
                st.write(f"**{row['sku_num_sku_r3']}**")
            
            with col2:
                # Highlight logic
                label = f"{row['product_name']} ({row['color']})"
                if not is_valid and is_fashion:
                    st.error(f"⚠️ Invalid Size: {current_size}")
                else:
                    st.success(f"Size: {current_size}")
            
            with col3:
                if is_fashion:
                    new_size = st.selectbox(
                        "Change Size", 
                        options=["(auto)"] + valid_sizes, 
                        key=f"size_{i}"
                    )
                    individual_overrides[i] = new_size

        # Download Section
        st.divider()
        if st.button("Prepare Export"):
            # This generates the final binary using the logic above
            # (Calculation of categories and descriptions happens here)
            dummy_cats = [("Code1", "")] * len(results)
            dummy_descs = ["Bullet 1"] * len(results)
            
            final_xlsx = build_template_fast(
                results, df_cat, None, dummy_cats, dummy_descs, 
                is_fashion, valid_sizes, individual_overrides
            )
            
            st.download_button(
                "Download Upload Template Only",
                data=final_xlsx,
                file_name="decathlon_upload_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.warning("No SKUs matched.")
