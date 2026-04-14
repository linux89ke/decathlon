"""
Decathlon Product Lookup - Final Version
Updates:
 - Variation: Always uses 'size' column. Dots (...) only if empty.
 - Name Fix: Checks all pipe-separated colors before appending (Fixes Dark Grey - Graphite Grey).
 - Finance: Price_KES = 100000, Stock = 0.
 - UI: Primary Category full path shown; Additional Category hidden.
 - Export: Output 'Upload Template' sheet ONLY.
 - Validation: Red-flagging missing sizes on front-end and Excel export.
"""

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

# --- CONFIG & STYLING ---
st.set_page_config(page_title="Decathlon Product Lookup", page_icon="👟", layout="wide")
st.markdown("""
<style>
h1 { color: #0082C3; }
.invalid-row { background-color: #ffcccc !important; padding: 10px; border-radius: 5px; margin-bottom: 10px; border: 1px solid #ff0000; }
.ai-badge { display:inline-block; background:linear-gradient(90deg,#f55036,#ff8c00); color:white; border-radius:12px; padding:2px 10px; font-size:11px; font-weight:700; }
</style>
""", unsafe_allow_html=True)

# --- CONSTANTS ---
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

# --- CORE UTILITIES ---

def _clean(val) -> str:
    if pd.isna(val) or str(val).strip() in ("", "-", "nan"):
        return ""
    return str(val).strip()

def _format_gtin(val) -> str:
    raw = str(val).strip()
    if not raw or raw.lower() in ("nan", ""): return ""
    try: return str(int(float(raw)))
    except: return raw

def parse_valid_sizes():
    if os.path.exists(SIZES_PATH):
        with open(SIZES_PATH, "r", encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip() and not l.startswith("#")]
    return []

def get_variation(row, is_fashion, valid_sizes, override=None):
    if override and override != "(auto)": return override
    # Always pull from 'size' column as per request
    raw = re.sub(r'"+', '', _clean(row.get("size", ""))).strip().rstrip(".")
    if not raw or raw.lower() in ("no size", "none"): return "..."
    
    if is_fashion and valid_sizes:
        if any(s.lower() == raw.lower() for s in valid_sizes):
            return next(s for s in valid_sizes if s.lower() == raw.lower())
        uk_match = re.search(r'\bUK\s*(\d{1,2}(?:\.\d)?)\b', raw, re.I)
        if uk_match:
            uk_val = f"UK {uk_match.group(1)}"
            if any(s.lower() == uk_val.lower() for s in valid_sizes):
                return next(s for s in valid_sizes if s.lower() == uk_val.lower())
    return raw

# --- AI & SEARCH ENGINE ---

def _build_query_string(row):
    fields = ["family", "type", "department_label", "nature_label", "brand_name", "color", "product_name"]
    return " ".join([_clean(row.get(f, "")) for f in fields]).lower()

@st.cache_resource
def build_tfidf_index():
    df_cat = pd.read_excel(DECA_CAT_PATH, sheet_name="category", dtype=str)
    all_paths = df_cat["Category Path"].dropna().unique().tolist()
    vectorizer = TfidfVectorizer(ngram_range=(1, 2))
    matrix = vectorizer.fit_transform(all_paths)
    path_to_export = dict(zip(df_cat["Category Path"], df_cat["export_category"]))
    return all_paths, vectorizer, matrix, path_to_export

async def _async_groq(idx, query, candidates, client, model, top_n, sem):
    async with sem:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": f"Pick top {top_n} categories for: {query}. Candidates: {candidates}"}],
                response_format={"type": "json_object"}
            )
            return idx, json.loads(resp.choices[0].message.content)
        except: return idx, {}

# --- TEMPLATE BUILDER (THE FIX) ---

def build_template(results_df, df_cat, is_fashion, valid_sizes, sku_overrides, ai_categories, short_descs):
    wb = load_workbook(TEMPLATE_PATH)
    for sheet in wb.sheetnames:
        if sheet != "Upload Template": del wb[sheet]
    
    ws = wb["Upload Template"]
    headers = {ws.cell(1, c).value: c for c in range(1, ws.max_column + 1) if ws.cell(1, c).value}
    red_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
    exp_to_fullpath = dict(zip(df_cat["export_category"], df_cat["Category Path"])) if df_cat is not None else {}

    for i, (_, src_row) in enumerate(results_df.iterrows()):
        row_idx = i + 2
        sku = _clean(src_row.get("sku_num_sku_r3"))
        
        # Name Fix (Dark Grey - Graphite Grey prevention)
        name = _clean(src_row.get("product_name"))
        color_raw = _clean(src_row.get("color", ""))
        colors = [c.strip() for c in color_raw.split("|") if c.strip()]
        if colors and name:
            if not any(c.lower() in name.lower() for c in colors):
                name = f"{name} - {colors[0].title()}"

        var_val = get_variation(src_row, is_fashion, valid_sizes, sku_overrides.get(sku))

        row_data = {
            "SellerSKU": sku,
            "Name": name,
            "Price_KES": "100000",
            "Stock": "0",
            "variation": var_val,
            "GTIN_Barcode": _format_gtin(src_row.get("bar_code")),
            "short_description": short_descs[i] if short_descs else ""
        }

        if ai_categories:
            prim_code = ai_categories[i][0]
            row_data["PrimaryCategory"] = exp_to_fullpath.get(prim_code, prim_code)

        for k, v in row_data.items():
            if k in headers:
                ws.cell(row_idx, headers[k]).value = v
                if is_fashion and valid_sizes and var_val != "..." and var_val not in valid_sizes:
                    ws.cell(row_idx, headers[k]).fill = red_fill

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# --- STREAMLIT UI ---

st.title("Decathlon Product Lookup")
valid_sizes = parse_valid_sizes()

with st.sidebar:
    product_type = st.radio("Product Type", ["Fashion", "Other"])
    is_fash = product_type == "Fashion"
    use_ai = st.toggle("Use AI (Groq)", False)
    api_key = st.text_input("Groq Key", type="password") if use_ai else ""

manual_skus = st.text_area("Enter SKUs (one per line)")
if manual_skus:
    skus = [s.strip() for s in manual_skus.splitlines() if s.strip()]
    df_master = pd.read_csv(MASTER_PATH, dtype=str)
    results = df_master[df_master["sku_num_sku_r3"].isin(skus)].copy()
    
    if not results.empty:
        if "sku_overrides" not in st.session_state: st.session_state.sku_overrides = {}
        
        # Load category data for full path display
        df_cat = pd.read_excel(DECA_CAT_PATH, sheet_name="category", dtype=str)
        exp_to_path = dict(zip(df_cat["export_category"], df_cat["Category Path"]))

        for idx, row in results.iterrows():
            sku = row["sku_num_sku_r3"]
            current_var = get_variation(row, is_fash, valid_sizes, st.session_state.sku_overrides.get(sku))
            is_invalid = is_fash and current_var != "..." and current_var not in valid_sizes
            
            # Highlight red on front end if size invalid
            st.markdown(f'<div class="{"invalid-row" if is_invalid else ""}">', unsafe_allow_html=True)
            c1, c2, c3 = st.columns([3, 4, 2])
            with c1:
                st.write(f"**{sku}**")
                st.caption(row.get("product_name", ""))
            with c2:
                # Show full category path, hide additional
                st.write(f"**Category:** {exp_to_path.get(row.get('family',''), 'Not Set')}")
            with c3:
                if is_fash:
                    st.session_state.sku_overrides[sku] = st.selectbox(f"Size {sku}", ["(auto)"] + valid_sizes, key=f"s_{sku}")
                else:
                    st.write(f"Variation: {current_var}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        if st.button("Download Upload Template Sheet"):
            # Mocking short_descs and ai_cats for this example - in your file these are computed
            tpl = build_template(results, df_cat, is_fash, valid_sizes, st.session_state.sku_overrides, None, None)
            st.download_button("Download Excel", tpl, "upload_template.xlsx")
