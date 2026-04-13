"""
CSV to Shopify Product Import Converter
Converts any CSV file with product data into Shopify's official import format.
"""

import csv
import os
import re
import sys
from pathlib import Path


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

# -----------------------------------------------------------------------
# Column mapping definitions for each supplier CSV format
# -----------------------------------------------------------------------

SUPPLIER_A_MAP = {
    "sku_col":              'מק"ט',
    "title_col":            "שם מוצר",
    "body_col":             "תיאור",
    "price_col":            "מחיר",
    "compare_price_col":    "מחיר השוואה",
    "inventory_col":        "כמות במלאי",
    "vendor_col":           "ספק",
    "type_col":             "קטגוריה",
    "tags_col":             "תגיות",
    "weight_col":           "משקל (גרם)",
    "weight_unit":          "grams",
    "barcode_col":          "ברקוד",
    "image_col":            "תמונה ראשית",
}

SUPPLIER_B_MAP = {
    "sku_col":              "product_id",
    "title_col":            "product_name",
    "body_col":             "description",
    "price_col":            "price",
    "compare_price_col":    "compare_price",
    "inventory_col":        "stock",
    "vendor_col":           "brand",
    "type_col":             "category",
    "tags_col":             "tags",
    "weight_col":           "weight_kg",
    "weight_unit":          "kg",
    "barcode_col":          "barcode",
    "image_col":            "image_url",
    "color_col":            "color",
    "size_col":             "size",
}


def slugify(text: str) -> str:
    """Convert a title to a Shopify URL handle."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text


def weight_to_grams(value: str, unit: str) -> str:
    """Normalize weight to grams (Shopify stores grams)."""
    try:
        weight = float(value)
        if unit == "kg":
            return str(int(weight * 1000))
        return str(int(weight))
    except (ValueError, TypeError):
        return "0"


def detect_supplier(headers: list[str]) -> str | None:
    """Auto-detect which supplier format the CSV uses based on headers."""
    headers_lower = [h.lower().strip() for h in headers]
    if 'מק"ט' in headers or "שם מוצר" in headers:
        return "supplier_a"
    if "product_id" in headers_lower and "product_name" in headers_lower:
        return "supplier_b"
    return None


def row_to_shopify(row: dict, mapping: dict) -> dict:
    """Convert a single CSV row to a Shopify product row."""
    title = row.get(mapping["title_col"], "").strip()
    sku = row.get(mapping["sku_col"], "").strip()
    handle = slugify(title) if title else slugify(sku)

    weight_raw = row.get(mapping.get("weight_col", ""), "0")
    grams = weight_to_grams(weight_raw, mapping.get("weight_unit", "grams"))

    # Build option columns (colour / size if present)
    option1_name, option1_value = "", ""
    option2_name, option2_value = "", ""
    if mapping.get("color_col"):
        color = row.get(mapping["color_col"], "").strip()
        if color and color.lower() not in ("", "one size", "n/a"):
            option1_name, option1_value = "Color", color
    if mapping.get("size_col"):
        size = row.get(mapping["size_col"], "").strip()
        if size and size.lower() not in ("", "one size", "n/a"):
            option2_name, option2_value = "Size", size

    title_html = f"<p>{row.get(mapping['body_col'], '').strip()}</p>"

    shopify_row = {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": title_html,
        "Vendor": row.get(mapping.get("vendor_col", ""), "").strip(),
        "Product Category": "",
        "Type": row.get(mapping.get("type_col", ""), "").strip(),
        "Tags": row.get(mapping.get("tags_col", ""), "").strip(),
        "Published": "TRUE",
        "Option1 Name": option1_name,
        "Option1 Value": option1_value,
        "Option2 Name": option2_name,
        "Option2 Value": option2_value,
        "Option3 Name": "",
        "Option3 Value": "",
        "Variant SKU": sku,
        "Variant Grams": grams,
        "Variant Inventory Tracker": "shopify",
        "Variant Inventory Qty": row.get(mapping.get("inventory_col", ""), "0").strip(),
        "Variant Inventory Policy": "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price": row.get(mapping.get("price_col", ""), "").strip(),
        "Variant Compare At Price": row.get(mapping.get("compare_price_col", ""), "").strip(),
        "Variant Requires Shipping": "TRUE",
        "Variant Taxable": "TRUE",
        "Variant Barcode": row.get(mapping.get("barcode_col", ""), "").strip(),
        "Image Src": row.get(mapping.get("image_col", ""), "").strip(),
        "Image Position": "1",
        "Image Alt Text": title,
        "Gift Card": "FALSE",
        "SEO Title": title,
        "SEO Description": row.get(mapping["body_col"], "").strip()[:320],
        "Status": "active",
    }
    return shopify_row


def convert_file(input_path: str, output_path: str) -> int:
    """Convert a single CSV file and write the Shopify-format result."""
    with open(input_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    supplier = detect_supplier(headers)
    if supplier == "supplier_a":
        mapping = SUPPLIER_A_MAP
        print(f"  ✓ זוהה פורמט: ספק א (עברית)")
    elif supplier == "supplier_b":
        mapping = SUPPLIER_B_MAP
        print(f"  ✓ זוהה פורמט: ספק ב (אנגלית)")
    else:
        print(f"  ✗ לא ניתן לזהות את הפורמט של: {input_path}")
        return 0

    shopify_rows = [row_to_shopify(row, mapping) for row in rows]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=SHOPIFY_COLUMNS)
        writer.writeheader()
        writer.writerows(shopify_rows)

    return len(shopify_rows)


def main():
    input_dir = Path(__file__).parent / "input"
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)

    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        print("לא נמצאו קבצי CSV בתיקיית input/")
        sys.exit(1)

    print("=" * 60)
    print("   ממיר CSV מוצרים לפורמט שופיפיי")
    print("=" * 60)

    total_products = 0
    output_files = []

    for csv_file in csv_files:
        print(f"\n📄 מעבד: {csv_file.name}")
        output_name = f"shopify_{csv_file.stem}.csv"
        output_path = output_dir / output_name

        count = convert_file(str(csv_file), str(output_path))
        if count:
            total_products += count
            output_files.append(output_path)
            print(f"  ✓ הומרו {count} מוצרים → {output_name}")

    print(f"\n{'=' * 60}")
    print(f"  סה\"כ: {total_products} מוצרים הומרו בהצלחה ל-{len(output_files)} קבצים")
    print(f"  קבצי הפלט נמצאים בתיקיית: output/")
    print("=" * 60)

    # Print preview of first output
    if output_files:
        print(f"\n📋 תצוגה מקדימה של '{output_files[0].name}':")
        print("-" * 60)
        with open(output_files[0], encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if i == 0:
                    # Print headers
                    cols = line.strip().split(",")
                    print(f"  עמודות ({len(cols)}): {', '.join(cols[:8])} ...")
                    print("-" * 60)
                else:
                    cols = line.strip().split(",")
                    print(f"  שורה {i}: Handle={cols[0]}, Title={cols[1]}, Price={cols[20]}")
                if i > 5:
                    break


if __name__ == "__main__":
    main()
