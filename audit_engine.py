import pandas as pd
import numpy as np
from datetime import datetime
import re
import os

def check_mismatches(csv_df: pd.DataFrame, shopify_df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Compares CSV and Shopify dataframes and returns a dataframe of mismatches.
    """
    print("Running audit comparison engine...")
    pd.set_option('mode.chained_assignment', None)
    is_clearance_file = 'clearance' in filename.lower()
    
    # 1. Clean Data Types
    # Ensure SKUs are strings and lowercased for reliable joining
    csv_df['sku_lower'] = csv_df['sku'].astype(str).str.lower().str.strip()
    shopify_df['sku_lower'] = shopify_df['sku'].astype(str).str.lower().str.strip()
    
    # Clean Prices (Removing $ and commas if any, converting to float)
    for col in ['price', 'compareAtPrice']:
        if col in csv_df.columns:
            csv_df[col] = pd.to_numeric(csv_df[col].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
        if col in shopify_df.columns:
            shopify_df[col] = pd.to_numeric(shopify_df.astype(str)[col], errors='coerce')
            
    # Clean Inventory
    if 'inventory' in csv_df.columns:
         csv_df['inventory'] = pd.to_numeric(csv_df['inventory'], errors='coerce')
    if 'inventoryQuantity' in shopify_df.columns:
         shopify_df['inventoryQuantity'] = pd.to_numeric(shopify_df['inventoryQuantity'], errors='coerce')

    # 2. Merge Dataframes on SKU
    # outer join allows us to find matches, missing in shopify, and not in csv
    merged = pd.merge(csv_df, shopify_df, on='sku_lower', how='outer', suffixes=('_csv', '_shopify'))
    
    mismatches = []
    
    # Detect per-location qty columns from shopify_df (columns ending with ' Qty')
    location_qty_cols = [c for c in shopify_df.columns if c.endswith(' Qty')]
    
    # 3. Process matches and look for discrepancies
    matches = merged[merged['sku_csv'].notna() & merged['sku_shopify'].notna()]
    print(f"Analyzing {len(matches)} matching SKUs for discrepancies...")
    
    # Calculate group discounts (If ANY variant has a discount, parent is clearance)
    # Replicating `isUseParentClearanceOverride`
    if 'handle_csv' in matches.columns:
        matches['has_discount_csv'] = (matches['compareAtPrice_csv'].notna()) & (matches['price_csv'] < matches['compareAtPrice_csv'])
        parent_discounts = matches.groupby('handle_csv')['has_discount_csv'].transform('any')
    else:
        parent_discounts = pd.Series(False, index=matches.index)
        
    for idx, row in matches.iterrows():
        sku = row['sku_shopify']
        handle = row.get('handle_shopify', row.get('handle', 'Unknown'))
        variant_id = row.get('variant_id_shopify', row.get('variant_id'))
        product_id = row.get('product_id_shopify', row.get('product_id'))
        inventory_item_id = row.get('inventoryItemId_shopify', row.get('inventoryItemId'))
        
        # Collect per-location qty values for this row (column may have _shopify suffix after merge)
        loc_qty_data = {}
        for col in location_qty_cols:
            merged_col = f"{col}_shopify" if f"{col}_shopify" in row.index else col
            loc_qty_data[col] = row.get(merged_col, 0)
        
        # Helper to construct mismatch dict
        def make_mismatch(field, csv_val, shopify_val, **kwargs):
            mismatch = {
                'sku': sku, 'handle': handle, 'field': field,
                'csv_value': csv_val, 'shopify_value': shopify_val,
                'variant_id': variant_id, 'product_id': product_id, 'inventory_item_id': inventory_item_id,
                'shopify_price': row.get('price_shopify'),
                'shopify_compare_at_price': row.get('compareAtPrice_shopify'),
                'is_clearance_file': is_clearance_file
            }
            mismatch.update(loc_qty_data)  # Inject per-location qty columns
            mismatch.update(kwargs)
            return mismatch
        
        # --- Rule 1: Price ---
        if pd.notna(row['price_csv']) and pd.notna(row['price_shopify']) and row['price_csv'] != row['price_shopify']:
            mismatches.append(make_mismatch('price', row['price_csv'], row['price_shopify']))
            
        # --- Rule 1.5: Compare At Price & Clearance vs Sticky Sale ---
        if is_clearance_file:
            if pd.notna(row['compareAtPrice_csv']) and pd.notna(row['compareAtPrice_shopify']) and row['compareAtPrice_csv'] != row['compareAtPrice_shopify']:
                 mismatches.append(make_mismatch('compare_at_price', row['compareAtPrice_csv'], row['compareAtPrice_shopify']))
        else:
            # Not clearance: Compare at price should be NULL or equal to Price.
            if pd.notna(row['compareAtPrice_shopify']) and row['compareAtPrice_shopify'] != row['price_shopify']:
                mismatches.append(make_mismatch('sticky_sale', 'N/A (Should be null or equal to price)', row['compareAtPrice_shopify']))

        # --- Rule 2: Inventory ---
        # Inventory mismatch (Stock check) functionality has been removed per user request.


        # --- Rule 3: Tags / Oversize / Clearance ---
        # Parse tags safely: lowercased, stripped, and collected as a set
        shop_tags_raw = str(row.get('tags_shopify', row.get('tags', '')))
        csv_tags_raw = str(row.get('tags_csv', ''))
        
        shop_tags = {t.strip().lower() for t in shop_tags_raw.split(',')} if shop_tags_raw and shop_tags_raw != 'nan' else set()
        csv_tags = {t.strip().lower() for t in csv_tags_raw.split(',')} if csv_tags_raw and csv_tags_raw != 'nan' else set()
        
        is_oversize = 'oversize' in shop_tags or 'oversize' in csv_tags or 'overweight' in shop_tags or 'overweight' in csv_tags
        
        # Determine actual Shopify template from merged df (suffix _shopify to avoid conflict with CSV if available)
        template_raw = row.get('templateSuffix_shopify', row.get('templateSuffix'))
        template = str(template_raw).lower() if pd.notna(template_raw) else 'default template'
        if template in ('none', 'nan', 'null', ''):
            template = 'default template'

        # Default initialization for expected template
        expected_template = 'default template'
        real_discount = False
        
        # Check discount
        # Compare CSV prices if valid, otherwise fallback to Shopify prices
        if pd.notna(row['compareAtPrice_csv']) and pd.notna(row['price_csv']):
             if row['compareAtPrice_csv'] > row['price_csv']:
                  real_discount = True
        elif pd.notna(row['compareAtPrice_shopify']) and pd.notna(row['price_shopify']):
             if row['compareAtPrice_shopify'] > row['price_shopify']:
                  real_discount = True

        if is_oversize:
            expected_template = 'heavy-products'
        elif 'clearance' in shop_tags and real_discount:
            expected_template = 'clearance'
            
        # Fallbacks depending on file type (if it's a clearance file and should have a discount)
        if is_clearance_file and expected_template == 'default template':
            # If it's a clearance file and has a real discount and not overweight, we EXPECT clearance tag, so it SHOULD be clearance template.
            if real_discount and not is_oversize:
                 expected_template = 'clearance'

        # Template check mismatch
        if template != expected_template:
            display_expected = 'Default Template' if expected_template == 'default template' else expected_template
            display_actual = 'Default Template' if template == 'default template' else str(template_raw) if pd.notna(template_raw) and str(template_raw).lower() not in ('none', 'nan', 'null', '') else 'Default Template'
            mismatches.append(make_mismatch('incorrect_template_suffix', display_expected, display_actual))
            
        # Oversize/overweight tag checks
        if is_oversize and 'oversize' not in shop_tags and 'overweight' not in shop_tags:
            mismatches.append(make_mismatch('missing_oversize_tag', 'oversize or overweight', ", ".join(shop_tags) if shop_tags else "None"))
            
        # Clearance specific tag/price checks
        if is_clearance_file:
            # Not a real discount
            if pd.notna(row['compareAtPrice_csv']) and row['price_csv'] == row['compareAtPrice_csv']:
                 if not parent_discounts.loc[idx]: # Sibling override check
                     has_clearance_issues = 'clearance' in shop_tags or template == 'clearance'
                     if has_clearance_issues:
                          mismatches.append(make_mismatch('clearance_price_mismatch', 'Regular Price (No Clearance)', 'Marked as Clearance'))
            else:
                 # Should have clearance tags
                 if 'clearance' not in shop_tags:
                      mismatches.append(make_mismatch('missing_clearance_tag', 'Clearance', shop_tags_raw if shop_tags_raw and shop_tags_raw != 'nan' else "None"))
                      
        # --- Rule 4: SEO / H1 in Description ---
        # Fetch the descriptionHtml attribute correctly from row
        desc_html = row.get('descriptionHtml_shopify', row.get('descriptionHtml', ''))
        if pd.notna(desc_html) and isinstance(desc_html, str):
            # perform case-insensitive check for H1 tags
            if re.search(r'<h1\b[^>]*>', desc_html, re.IGNORECASE):
                # Calculate the fixed HTML
                # Replace <h1 ...> with <h2 ...>
                fixed_html = re.sub(r'<h1(\b[^>]*)>', r'<h2\1>', desc_html, flags=re.IGNORECASE)
                # Replace </h1> with </h2>
                fixed_html = re.sub(r'</h1>', r'</h2>', fixed_html, flags=re.IGNORECASE)
                
                mismatches.append(make_mismatch('h1_in_description', 'Uses H2 Tags', 'Contains H1 Tag', fixed_descriptionHtml=fixed_html))
                     
    # 4. Missing in Shopify
    missing = merged[merged['sku_csv'].notna() & merged['sku_shopify'].isna()]
    print(f"Found {len(missing)} SKUs missing in Shopify.")
    
    missing_records = []
    # Identify CSV columns (excluding the temporary sku_lower)
    csv_cols = [col for col in csv_df.columns if col != 'sku_lower']
    
    for _, row in missing.iterrows():
        record = {}
        for col in csv_cols:
            # Overlapping columns have _csv suffix in merged df
            val = row.get(f"{col}_csv") if f"{col}_csv" in missing.columns else row.get(col)
            record[col] = val
            
        # Special handling for 'type' fallback logic
        if 'type' in record and (pd.isna(record['type']) or str(record['type']).strip() == ''):
             record['type'] = record.get('product_type', record.get('category', ''))
             
        # Fallback for Title
        if 'title' in record and (pd.isna(record['title']) or str(record['title']).strip() == ''):
            record['title'] = f"Product {record.get('sku', 'Unknown')}"
            
        missing_records.append(record)
        
    mismatch_df = pd.DataFrame(mismatches)
    missing_df = pd.DataFrame(missing_records)
    return mismatch_df, missing_df, len(matches)

def check_stale_clearance(shopify_df: pd.DataFrame, csv_df: pd.DataFrame) -> pd.DataFrame:
    """Finds products that have the clearance tag in Shopify but are missing from the clearance CSV"""
    print("Checking for stale clearance tags in Shopify...")
    if shopify_df.empty: return pd.DataFrame()
    
    stale = []
    csv_skus = set(csv_df['sku'].astype(str).str.lower().str.strip())
    
    for _, row in shopify_df.iterrows():
         tags = str(row.get('tags', '')).lower().replace(' ', '').split(',')
         if 'clearance' in tags:
             inv = pd.to_numeric(row.get('inventoryQuantity', 0), errors='coerce')
             if pd.notna(inv) and inv > 0:
                 sku = str(row.get('sku', '')).lower().strip()
                 if sku not in csv_skus:
                      stale.append({
                          'sku': row.get('sku'), 'handle': row.get('handle'), 
                          'field': 'stale_clearance_tag', 'csv_value': 'Not in Clearance file', 'shopify_value': 'Has Clearance tag',
                          'variant_id': row.get('id'), 'product_id': row.get('product_id'), 'inventory_item_id': row.get('inventoryItemId')
                      })
                      
    stale_df = pd.DataFrame(stale)
    if not stale_df.empty:
         print(f"Warning: Found {len(stale_df)} stale clearance variants.")
    return stale_df
