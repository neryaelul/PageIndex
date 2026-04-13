"""
AI-Powered CSV to Shopify Converter — powered by PageIndex reasoning approach.

Instead of hardcoded column mappings, this uses an LLM (same as PageIndex uses)
to REASON about the CSV structure and automatically map any columns to Shopify format.
Works with any language, any column naming convention, any supplier.
"""

import csv
import json
import os
import re
import sys
from pathlib import Path

import openai
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("CHATGPT_API_KEY") or os.getenv("OPENAI_API_KEY")

SHOPIFY_COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type",
    "Tags", "Published", "Option1 Name", "Option1 Value", "Option2 Name",
    "Option2 Value", "Option3 Name", "Option3 Value", "Variant SKU",
    "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty",
    "Variant Inventory Policy", "Variant Fulfillment Service", "Variant Price",
    "Variant Compare At Price", "Variant Requires Shipping", "Variant Taxable",
    "Variant Barcode", "Image Src", "Image Position", "Image Alt Text",
    "Gift Card", "SEO Title", "SEO Description", "Status"
]

SHOPIFY_FIELD_DESCRIPTIONS = {
    "Handle": "Unique URL slug for the product (e.g. 'blue-jeans')",
    "Title": "Product name/title",
    "Body (HTML)": "Product description in HTML",
    "Vendor": "Brand or supplier name",
    "Type": "Product type/category",
    "Tags": "Comma-separated tags for search/filtering",
    "Option1 Name": "First variant attribute name (e.g. Color, Size)",
    "Option1 Value": "First variant attribute value (e.g. Red, Large)",
    "Option2 Name": "Second variant attribute name",
    "Option2 Value": "Second variant attribute value",
    "Variant SKU": "Stock keeping unit / product ID code",
    "Variant Grams": "Product weight in grams (integer)",
    "Variant Inventory Qty": "Quantity in stock",
    "Variant Price": "Selling price (number)",
    "Variant Compare At Price": "Original/compare-at price for showing discounts",
    "Variant Barcode": "Barcode / EAN / UPC",
    "Image Src": "URL of the main product image",
    "Image Alt Text": "Alt text for the product image",
    "SEO Title": "SEO page title (usually same as product title)",
    "SEO Description": "SEO meta description",
    "Status": "Product status: active, draft, or archived",
}


# ─────────────────────────────────────────────────────────────────────────────
# PageIndex-style LLM call (identical pattern to pageindex/utils.py)
# ─────────────────────────────────────────────────────────────────────────────

def call_llm(prompt: str, model: str = "gpt-4o") -> str:
    """Call OpenAI — same pattern as PageIndex's ChatGPT_API."""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return response.choices[0].message.content


def extract_json(content: str) -> dict:
    """Extract JSON from LLM response — same helper as PageIndex."""
    try:
        start = content.find("```json")
        if start != -1:
            start += 7
            end = content.rfind("```")
            content = content[start:end]
        return json.loads(content.strip())
    except json.JSONDecodeError:
        try:
            content = content.replace(",]", "]").replace(",}", "}")
            return json.loads(content.strip())
        except Exception:
            return {}


# ─────────────────────────────────────────────────────────────────────────────
# AI column mapping (the PageIndex "reasoning" step)
# ─────────────────────────────────────────────────────────────────────────────

def ai_detect_column_mapping(headers: list[str], sample_rows: list[dict]) -> dict:
    """
    Ask the LLM to reason about the CSV columns and return a mapping
    from Shopify field → source CSV column name.
    This mirrors exactly how PageIndex uses LLM reasoning to understand
    document structure without hardcoded rules.
    """
    sample_text = json.dumps(sample_rows[:3], ensure_ascii=False, indent=2)
    shopify_fields_desc = json.dumps(SHOPIFY_FIELD_DESCRIPTIONS, ensure_ascii=False, indent=2)

    prompt = f"""You are an expert data analyst. Your job is to analyze a product CSV file
and map its columns to Shopify's product import format.

The CSV has the following columns:
{json.dumps(headers, ensure_ascii=False)}

Here are sample rows from the CSV:
{sample_text}

Here are the Shopify fields you need to map TO, with their descriptions:
{shopify_fields_desc}

Your task:
1. Carefully reason about what each source column represents (columns may be in Hebrew, English, or any language).
2. Map each relevant Shopify field to the correct source column name.
3. For "Variant Grams", also note the weight unit (grams or kg) so we can convert.
4. If a Shopify field has no matching source column, set it to null.
5. If the CSV has color/size/variant columns, map them to Option1/Option2.

Return ONLY a JSON object in this format:
```json
{{
  "reasoning": "brief explanation of what you understood about this CSV",
  "weight_unit": "grams or kg",
  "mapping": {{
    "Handle": null,
    "Title": "<source column name or null>",
    "Body (HTML)": "<source column name or null>",
    "Vendor": "<source column name or null>",
    "Type": "<source column name or null>",
    "Tags": "<source column name or null>",
    "Option1 Name": "<hardcoded value like 'Color' if applicable, or null>",
    "Option1 Value": "<source column name or null>",
    "Option2 Name": "<hardcoded value like 'Size' if applicable, or null>",
    "Option2 Value": "<source column name or null>",
    "Variant SKU": "<source column name or null>",
    "Variant Grams": "<source column name or null>",
    "Variant Inventory Qty": "<source column name or null>",
    "Variant Price": "<source column name or null>",
    "Variant Compare At Price": "<source column name or null>",
    "Variant Barcode": "<source column name or null>",
    "Image Src": "<source column name or null>",
    "SEO Title": "<source column name or null>",
    "SEO Description": "<source column name or null>"
  }}
}}
```"""

    print(f"\n  [AI] שולח prompt לניתוח עמודות...")
    raw = call_llm(prompt)
    result = extract_json(raw)

    if result:
        print(f"  [AI] הבנה: {result.get('reasoning', '')[:120]}...")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Row conversion using the AI-generated mapping
# ─────────────────────────────────────────────────────────────────────────────

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


def row_to_shopify(row: dict, mapping: dict, weight_unit: str) -> dict:
    m = mapping.get("mapping", {})

    title = get_val(row, m.get("Title"))
    sku = get_val(row, m.get("Variant SKU"))
    handle = slugify(title) if title else slugify(sku)

    description = get_val(row, m.get("Body (HTML)"))
    body_html = f"<p>{description}</p>" if description else ""

    weight_raw = get_val(row, m.get("Variant Grams"))
    grams = weight_to_grams(weight_raw, weight_unit)

    # Option1 — could be hardcoded name ("Color") + source column value
    opt1_name = m.get("Option1 Name", "") or ""
    opt1_col = m.get("Option1 Value")
    opt1_value = get_val(row, opt1_col) if opt1_col else ""

    opt2_name = m.get("Option2 Name", "") or ""
    opt2_col = m.get("Option2 Value")
    opt2_value = get_val(row, opt2_col) if opt2_col else ""

    shopify_row = {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": body_html,
        "Vendor": get_val(row, m.get("Vendor")),
        "Product Category": "",
        "Type": get_val(row, m.get("Type")),
        "Tags": get_val(row, m.get("Tags")),
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
        "Variant Inventory Qty": get_val(row, m.get("Variant Inventory Qty")),
        "Variant Inventory Policy": "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price": get_val(row, m.get("Variant Price")),
        "Variant Compare At Price": get_val(row, m.get("Variant Compare At Price")),
        "Variant Requires Shipping": "TRUE",
        "Variant Taxable": "TRUE",
        "Variant Barcode": get_val(row, m.get("Variant Barcode")),
        "Image Src": get_val(row, m.get("Image Src")),
        "Image Position": "1",
        "Image Alt Text": title,
        "Gift Card": "FALSE",
        "SEO Title": get_val(row, m.get("SEO Title")) or title,
        "SEO Description": (get_val(row, m.get("SEO Description")) or description)[:320],
        "Status": "active",
    }
    return shopify_row


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def convert_file(input_path: str, output_path: str) -> int:
    with open(input_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        rows = list(reader)

    if not rows:
        print("  ✗ הקובץ ריק")
        return 0

    # AI reasoning step — PageIndex-style LLM call
    ai_mapping = ai_detect_column_mapping(headers, rows)
    if not ai_mapping or "mapping" not in ai_mapping:
        print("  ✗ ה-AI לא הצליח לנתח את הקובץ")
        return 0

    weight_unit = ai_mapping.get("weight_unit", "grams")

    shopify_rows = [row_to_shopify(row, ai_mapping, weight_unit) for row in rows]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLUMNS)
        writer.writeheader()
        writer.writerows(shopify_rows)

    return len(shopify_rows)


def main():
    if not OPENAI_API_KEY:
        print("✗ לא נמצא מפתח API של OpenAI")
        print("  הוסף CHATGPT_API_KEY=your_key לקובץ .env")
        sys.exit(1)

    input_dir = Path(__file__).parent / "input"
    output_dir = Path(__file__).parent / "output_ai"
    output_dir.mkdir(exist_ok=True)

    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        print("לא נמצאו קבצי CSV בתיקיית input/")
        sys.exit(1)

    print("=" * 65)
    print("   ממיר CSV מוצרים לפורמט שופיפיי — מבוסס PageIndex AI")
    print("=" * 65)
    print("  (ה-AI מנתח את מבנה ה-CSV ומבצע מיפוי אוטומטי)")

    total_products = 0

    for csv_file in csv_files:
        print(f"\n📄 מעבד: {csv_file.name}")
        output_name = f"shopify_ai_{csv_file.stem}.csv"
        output_path = output_dir / output_name

        count = convert_file(str(csv_file), str(output_path))
        if count:
            total_products += count
            print(f"  ✓ הומרו {count} מוצרים → {output_name}")

    print(f"\n{'=' * 65}")
    print(f"  סה\"כ: {total_products} מוצרים הומרו על ידי AI")
    print(f"  קבצי הפלט: output_ai/")
    print("=" * 65)

    # Show preview
    ai_outputs = list(output_dir.glob("*.csv"))
    if ai_outputs:
        print(f"\n📋 תצוגה מקדימה — {ai_outputs[0].name}:")
        print("-" * 65)
        with open(ai_outputs[0], encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                print(f"  [{i+1}] Handle={row['Handle']} | Title={row['Title']}")
                print(f"       SKU={row['Variant SKU']} | Price={row['Variant Price']} | Grams={row['Variant Grams']}")
                if row.get("Option1 Value"):
                    print(f"       {row['Option1 Name']}={row['Option1 Value']}")
                print()


if __name__ == "__main__":
    main()
