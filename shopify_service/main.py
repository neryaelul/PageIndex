import csv
import io
import json
import os
import re

import openai
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

load_dotenv()


def _fetch_mongo_settings() -> tuple[dict, bool]:
    """
    Returns (settings_doc, connected_ok).
    Collection: app_settings, document _id: 'app' with optional keys:
      openai_api_key | chatgpt_api_key, jwt_secret
    """
    uri = (os.getenv("MONGODB_URI") or "").strip()
    if not uri:
        return {}, False
    try:
        from pymongo import MongoClient
    except ImportError:
        return {}, False
    try:
        db_name = (os.getenv("MONGO_DB_NAME") or "shopify_service").strip() or "shopify_service"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        doc = client[db_name].app_settings.find_one({"_id": "app"})
        client.close()
        d = doc if isinstance(doc, dict) else {}
        return d, True
    except Exception:
        return {}, False


_mongo_settings, MONGODB_CONNECTED = _fetch_mongo_settings()

OPENAI_API_KEY = (
    _mongo_settings.get("openai_api_key")
    or _mongo_settings.get("chatgpt_api_key")
    or os.getenv("CHATGPT_API_KEY")
    or os.getenv("OPENAI_API_KEY")
)
JWT_SECRET = _mongo_settings.get("jwt_secret") or os.getenv("JWT_SECRET")

app = FastAPI(title="Shopify CSV Converter")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )

SHOPIFY_COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type",
    "Tags", "Published", "Option1 Name", "Option1 Value", "Option2 Name",
    "Option2 Value", "Option3 Name", "Option3 Value", "Variant SKU",
    "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty",
    "Variant Inventory Policy", "Variant Fulfillment Service", "Variant Price",
    "Variant Compare At Price", "Variant Requires Shipping", "Variant Taxable",
    "Variant Barcode", "Image Src", "Image Position", "Image Alt Text",
    "Gift Card", "SEO Title", "SEO Description", "Status",
]

SHOPIFY_FIELD_DESCRIPTIONS = {
    "Handle": "Unique URL slug (e.g. 'blue-jeans')",
    "Title": "Product name/title",
    "Body (HTML)": "Product description in HTML",
    "Vendor": "Brand or supplier name",
    "Type": "Product type or category",
    "Tags": "Comma-separated tags",
    "Option1 Name": "First variant attribute name (e.g. Color, Size)",
    "Option1 Value": "First variant attribute value (e.g. Red, Large)",
    "Option2 Name": "Second variant attribute name",
    "Option2 Value": "Second variant attribute value",
    "Variant SKU": "Product ID / stock keeping unit",
    "Variant Grams": "Product weight in grams (integer)",
    "Variant Inventory Qty": "Quantity in stock",
    "Variant Price": "Selling price (number)",
    "Variant Compare At Price": "Original price before discount",
    "Variant Barcode": "Barcode / EAN / UPC",
    "Image Src": "URL of main product image",
    "SEO Title": "SEO page title",
    "SEO Description": "SEO meta description (max 320 chars)",
    "Status": "active, draft, or archived",
}

# Shopify metafield value types
METAFIELD_TYPES = [
    "single_line_text_field",
    "multi_line_text_field",
    "number_integer",
    "number_decimal",
    "boolean",
    "url",
    "color",
    "date",
    "json",
]


# ─────────────────────────────────────────────────────────────
# LLM helpers (PageIndex-style)
# ─────────────────────────────────────────────────────────────

def call_llm(prompt: str) -> str:
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return response.choices[0].message.content


def extract_json(content: str) -> dict | list:
    try:
        start = content.find("```json")
        if start != -1:
            start += 7
            end = content.rfind("```")
            content = content[start:end]
        return json.loads(content.strip())
    except Exception:
        try:
            content = content.replace(",]", "]").replace(",}", "}")
            return json.loads(content.strip())
        except Exception:
            return {}


# ─────────────────────────────────────────────────────────────
# AI Step 1 — standard column mapping
# ─────────────────────────────────────────────────────────────

def ai_detect_mapping(
    headers: list[str],
    sample_rows: list[dict],
    custom_instructions: str = "",
) -> dict:
    custom_block = ""
    if custom_instructions.strip():
        custom_block = f"""
User-defined instructions (apply these when building the mapping and transformations):
---
{custom_instructions.strip()}
---
"""

    prompt = f"""You are an expert data analyst. Analyze this product CSV and map its columns
to Shopify's standard product import fields.

CSV columns: {json.dumps(headers, ensure_ascii=False)}

Sample rows:
{json.dumps(sample_rows[:2], ensure_ascii=False, indent=2)}

Shopify fields (with descriptions):
{json.dumps(SHOPIFY_FIELD_DESCRIPTIONS, ensure_ascii=False, indent=2)}
{custom_block}
Rules:
1. Columns may be in Hebrew, English, or any language — reason about their meaning.
2. Map each Shopify field to the correct source column name (or null if no match).
3. For "Variant Grams" detect the weight unit (grams or kg).
4. If there are color/size/variant columns map them to Option1/Option2.
5. If the user instructions specify a hardcoded value for a field (e.g. "Vendor should always be X"),
   set that field's value to the string "__hardcoded:<value>" so it can be applied at conversion time.
6. If the user instructions require a transformation (e.g. "translate titles to English", "add SALE tag"),
   capture it in the "field_transforms" object as a free-text instruction per Shopify field name.

Return ONLY this JSON:
```json
{{
  "reasoning": "brief explanation",
  "weight_unit": "grams or kg",
  "mapping": {{
    "Title": "<col, '__hardcoded:value', or null>",
    "Body (HTML)": "<col or null>",
    "Vendor": "<col, '__hardcoded:value', or null>",
    "Type": "<col or null>",
    "Tags": "<col or null>",
    "Option1 Name": "<label like 'Color' or null>",
    "Option1 Value": "<col or null>",
    "Option2 Name": "<label like 'Size' or null>",
    "Option2 Value": "<col or null>",
    "Variant SKU": "<col or null>",
    "Variant Grams": "<col or null>",
    "Variant Inventory Qty": "<col or null>",
    "Variant Price": "<col or null>",
    "Variant Compare At Price": "<col or null>",
    "Variant Barcode": "<col or null>",
    "Image Src": "<col or null>",
    "SEO Title": "<col or null>",
    "SEO Description": "<col or null>"
  }},
  "field_transforms": {{
    "Title": "<transform instruction or null>",
    "Tags": "<transform instruction or null>",
    "Body (HTML)": "<transform instruction or null>"
  }}
}}
```"""
    return extract_json(call_llm(prompt))


# ─────────────────────────────────────────────────────────────
# AI Step 2 — metafield suggestions for unmapped columns
# ─────────────────────────────────────────────────────────────

def ai_detect_metafields(
    headers: list[str],
    mapped_cols: set[str],
    sample_rows: list[dict],
) -> list[dict]:
    """For every column not used in the standard mapping, ask AI to suggest a metafield."""
    unmapped = [h for h in headers if h not in mapped_cols]
    if not unmapped:
        return []

    samples = {col: [r.get(col, "") for r in sample_rows[:3]] for col in unmapped}

    prompt = f"""You are a Shopify expert. The following product CSV columns were NOT mapped
to standard Shopify fields. Suggest a Shopify metafield definition for each one.

Unmapped columns and their sample values:
{json.dumps(samples, ensure_ascii=False, indent=2)}

Available metafield value types: {", ".join(METAFIELD_TYPES)}

For each column return:
- source_col: the exact column name from the CSV
- namespace: use "custom" unless the data clearly belongs to another namespace (e.g. "reviews", "seo")
- key: snake_case key name in English (e.g. "material", "care_instructions")
- label: human-readable label in English
- type: the most appropriate Shopify metafield value type
- sample: a representative sample value from the data

Return ONLY a JSON array:
```json
[
  {{
    "source_col": "<exact column name>",
    "namespace": "custom",
    "key": "<snake_case_key>",
    "label": "<Human Label>",
    "type": "<value_type>",
    "sample": "<sample value>"
  }}
]
```"""
    result = extract_json(call_llm(prompt))
    return result if isinstance(result, list) else []


# ─────────────────────────────────────────────────────────────
# Row conversion
# ─────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text)


def weight_to_grams(value: str, unit: str) -> str:
    try:
        w = float(value)
        return str(int(w * 1000)) if unit == "kg" else str(int(w))
    except (ValueError, TypeError):
        return "0"


def get_val(row: dict, col: str | None) -> str:
    if not col:
        return ""
    return row.get(col, "").strip()


def resolve_field(row: dict, mapping_value: str | None) -> str:
    """
    Resolve a mapping value which can be:
    - None                  → ""
    - "__hardcoded:<val>"   → the literal value after the prefix
    - a column name         → the value from that column in the row
    """
    if not mapping_value:
        return ""
    if mapping_value.startswith("__hardcoded:"):
        return mapping_value[len("__hardcoded:"):]
    return row.get(mapping_value, "").strip()


def apply_transform(value: str, transform: str | None, row: dict, call_llm_fn) -> str:
    """
    If a field_transform instruction exists, ask the LLM to apply it to the value.
    We batch per-row calls into one prompt to keep it fast.
    """
    if not transform or not value:
        return value
    prompt = f"""Apply the following transformation to the product field value.
Transformation: {transform}
Original value: {value}
Product context: {json.dumps({k: v for k, v in row.items() if v}, ensure_ascii=False)[:400]}

Return ONLY the transformed value. No explanation."""
    result = call_llm_fn(prompt)
    return result.strip()


def row_to_shopify(
    row: dict,
    ai_result: dict,
    metafield_cols: list[str],
    metafield_sources: list[str],
    apply_transforms: bool = False,
) -> dict:
    m = ai_result.get("mapping", {})
    transforms = ai_result.get("field_transforms", {}) or {}
    weight_unit = ai_result.get("weight_unit", "grams")

    def get(field: str) -> str:
        val = resolve_field(row, m.get(field))
        if apply_transforms and transforms.get(field):
            val = apply_transform(val, transforms[field], row, call_llm)
        return val

    title       = get("Title")
    sku         = get("Variant SKU")
    handle      = slugify(title) if title else slugify(sku)
    description = get("Body (HTML)")

    weight_raw = resolve_field(row, m.get("Variant Grams"))
    grams = weight_to_grams(weight_raw, weight_unit)

    opt1_name  = m.get("Option1 Name") or ""
    opt1_value = resolve_field(row, m.get("Option1 Value"))
    opt2_name  = m.get("Option2 Name") or ""
    opt2_value = resolve_field(row, m.get("Option2 Value"))

    tags = get("Tags")
    if apply_transforms and transforms.get("Tags"):
        tags = apply_transform(tags, transforms["Tags"], row, call_llm)

    base = {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": f"<p>{description}</p>" if description else "",
        "Vendor": get("Vendor"),
        "Product Category": "",
        "Type": get("Type"),
        "Tags": tags,
        "Published": "TRUE",
        "Option1 Name": opt1_name,
        "Option1 Value": opt1_value,
        "Option2 Name": opt2_name,
        "Option2 Value": opt2_value,
        "Option3 Name": "",
        "Option3 Value": "",
        "Variant SKU": sku,
        "Variant Grams": grams,
        "Variant Inventory Tracker": "shopify",
        "Variant Inventory Qty": resolve_field(row, m.get("Variant Inventory Qty")),
        "Variant Inventory Policy": "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price": resolve_field(row, m.get("Variant Price")),
        "Variant Compare At Price": resolve_field(row, m.get("Variant Compare At Price")),
        "Variant Requires Shipping": "TRUE",
        "Variant Taxable": "TRUE",
        "Variant Barcode": resolve_field(row, m.get("Variant Barcode")),
        "Image Src": resolve_field(row, m.get("Image Src")),
        "Image Position": "1",
        "Image Alt Text": title,
        "Gift Card": "FALSE",
        "SEO Title": get("SEO Title") or title,
        "SEO Description": (get("SEO Description") or description)[:320],
        "Status": "active",
    }

    for mf_col, src_col in zip(metafield_cols, metafield_sources):
        base[mf_col] = row.get(src_col, "").strip()

    return base


# ─────────────────────────────────────────────────────────────
# Parse & cache CSV bytes
# ─────────────────────────────────────────────────────────────

def parse_csv_bytes(file_bytes: bytes) -> tuple[list[str], list[dict]]:
    # Try common encodings in order — handles UTF-8, Hebrew Windows, Latin
    for enc in ("utf-8-sig", "utf-8", "cp1255", "iso-8859-8", "windows-1252", "latin-1"):
        try:
            content = file_bytes.decode(enc)
            reader = csv.DictReader(io.StringIO(content))
            headers = list(reader.fieldnames or [])
            rows = list(reader)
            return headers, rows
        except (UnicodeDecodeError, Exception):
            continue
    raise ValueError("Could not decode the CSV file — unsupported encoding")


# ─────────────────────────────────────────────────────────────
# API Routes
# ─────────────────────────────────────────────────────────────

@app.post("/api/analyze")
async def analyze(
    files: list[UploadFile] = File(...),
    custom_instructions: str = Form(""),
):
    """
    Phase 1: Analyze uploaded CSVs.
    Returns per-file standard mapping + metafield suggestions (no CSV output yet).
    """
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured")
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    results = []
    for upload in files:
        if not upload.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail=f"{upload.filename} is not a CSV file")

        raw = await upload.read()
        headers, rows = parse_csv_bytes(raw)
        if not headers or not rows:
            raise HTTPException(status_code=422, detail=f"{upload.filename} is empty")

        ai_result = ai_detect_mapping(headers, rows, custom_instructions)
        if not ai_result or "mapping" not in ai_result:
            raise HTTPException(status_code=422, detail=f"AI could not parse {upload.filename}")

        # Collect all source columns that were used in the standard mapping
        mapped_cols: set[str] = set()
        for v in ai_result["mapping"].values():
            if v and isinstance(v, str) and v in headers:
                mapped_cols.add(v)

        metafields = ai_detect_metafields(headers, mapped_cols, rows)

        results.append({
            "filename": upload.filename,
            "product_count": len(rows),
            "reasoning": ai_result.get("reasoning", ""),
            "mapping": ai_result.get("mapping", {}),
            "weight_unit": ai_result.get("weight_unit", "grams"),
            "field_transforms": ai_result.get("field_transforms", {}),
            "metafields": metafields,
        })

    # Deduplicate metafields across files (by namespace.key)
    seen_keys: set[str] = set()
    all_metafields: list[dict] = []
    for r in results:
        for mf in r["metafields"]:
            k = f"{mf['namespace']}.{mf['key']}"
            if k not in seen_keys:
                seen_keys.add(k)
                all_metafields.append(mf)

    return {
        "files": results,
        "metafields": all_metafields,
        "total_products": sum(r["product_count"] for r in results),
    }


@app.post("/api/convert")
async def convert(
    files: list[UploadFile] = File(...),
    analysis: str = Form("{}"),          # JSON string: the /api/analyze result
    selected_metafields: str = Form("[]"),  # JSON array of namespace.key strings to include
):
    """
    Phase 2: Convert with selected metafields.
    Accepts the analysis result from /api/analyze to avoid re-running AI.
    """
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured")

    try:
        analysis_data: dict = json.loads(analysis)
        mf_selection: list[str] = json.loads(selected_metafields)   # e.g. ["custom.material", "custom.weight_unit"]
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid analysis/metafields JSON")

    files_by_name: dict[str, bytes] = {}
    for upload in files:
        files_by_name[upload.filename] = await upload.read()

    # Build metafield column headers from the analysis result
    all_metafields: list[dict] = analysis_data.get("metafields", [])
    selected_mf_defs = [
        mf for mf in all_metafields
        if f"{mf['namespace']}.{mf['key']}" in mf_selection
    ]

    # Shopify metafield column name format: "Metafield: namespace.key [type]"
    mf_col_headers = [
        f"Metafield: {mf['namespace']}.{mf['key']} [{mf['type']}]"
        for mf in selected_mf_defs
    ]
    mf_source_cols = [mf["source_col"] for mf in selected_mf_defs]

    all_columns = SHOPIFY_COLUMNS + mf_col_headers
    all_shopify_rows: list[dict] = []

    for file_result in analysis_data.get("files", []):
        fname = file_result["filename"]
        raw = files_by_name.get(fname)
        if raw is None:
            continue

        _, rows = parse_csv_bytes(raw)
        ai_result = {
            "mapping": file_result["mapping"],
            "weight_unit": file_result.get("weight_unit", "grams"),
        }

        has_transforms = bool(file_result.get("field_transforms"))
        for row in rows:
            shopify_row = row_to_shopify(
                row, ai_result, mf_col_headers, mf_source_cols,
                apply_transforms=has_transforms,
            )
            for col in all_columns:
                shopify_row.setdefault(col, "")
            all_shopify_rows.append(shopify_row)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_shopify_rows)

    csv_bytes = output.getvalue().encode("utf-8-sig")

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=shopify_products.csv",
            "X-Total-Products": str(len(all_shopify_rows)),
            "X-File-Count": str(len(analysis_data.get("files", []))),
            "X-Metafields-Count": str(len(selected_mf_defs)),
        },
    )


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "ai": "gpt-4o",
        "openai_configured": bool(OPENAI_API_KEY),
        "mongodb_connected": MONGODB_CONNECTED,
        "mongo_settings_loaded": bool(_mongo_settings),
        "jwt_configured": bool(JWT_SECRET),
    }


@app.get("/", response_class=HTMLResponse)
def index():
    with open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8") as f:
        return f.read()
