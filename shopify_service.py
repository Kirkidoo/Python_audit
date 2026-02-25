import os
import requests
import pandas as pd
import json
import time
from dotenv import load_dotenv

# Load Environment Variables from the parent directory's .env
load_dotenv(dotenv_path='.env')

SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME', '').replace('.myshopify.com', '').replace('https://', '').replace('http://', '')
ACCESS_TOKEN = os.getenv('SHOPIFY_API_ACCESS_TOKEN')
API_VERSION = '2024-07' # Using a recent stable version
GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

import math
def _clean_nans(obj):
    """Recursively removes pandas NA and math nan from dicts/lists for JSON serialization"""
    if isinstance(obj, dict):
        return {k: _clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_clean_nans(item) for item in obj]
    elif pd.isna(obj): # Catches np.nan, pd.NA, None
        return None
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj

def execute_graphql_query(query: str, variables: dict = None) -> dict:
    """Executes a GraphQL query against the Shopify API."""
    payload = {"query": query}
    if variables:
        payload["variables"] = _clean_nans(variables)
        
    response = requests.post(GRAPHQL_URL, headers=HEADERS, json=payload)
    response.raise_for_status() # Raise exception for HTTP errors
    
    data = response.json()
    if 'errors' in data:
        raise Exception(f"GraphQL Error: {data['errors']}")
        
    return data['data']


GET_PRODUCTS_BY_SKU_QUERY = """
query getProductsBySku($query: String!, $cursor: String) {
  productVariants(first: 250, query: $query, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        sku
        price
        compareAtPrice
        inventoryQuantity
        inventoryItem {
          id
          inventoryLevels(first: 10) {
            edges {
              node {
                location {
                  name
                }
              }
            }
          }
        }
        product {
          id
          handle
          title
          tags
          templateSuffix
          descriptionHtml
        }
      }
    }
  }
}
"""

def get_shopify_data_for_skus(skus: list) -> pd.DataFrame:
    """
    Fetches Shopify product variants for a list of SKUs and returns a Pandas DataFrame.
    """
    print(f"Fetching {len(skus)} SKUs from Shopify...")
    
    # Batch SKUs to avoid excessively long query strings
    batch_size = 50 
    all_variants = []
    
    for i in range(0, len(skus), batch_size):
        batch = skus[i:i+batch_size]
        # Escape quotes in SKUs
        query_str = " OR ".join(['sku:"' + str(sku).replace('"', '\\"') + '"' for sku in batch if pd.notna(sku)])
        
        if not query_str:
             continue
             
        has_next_page = True
        cursor = None
        
        while has_next_page:
            variables = {"query": query_str, "cursor": cursor}
            
            try:
                data = execute_graphql_query(GET_PRODUCTS_BY_SKU_QUERY, variables)
                variants_connection = data.get('productVariants', {})
                
                for edge in variants_connection.get('edges', []):
                    node = edge['node']
                    product = node.get('product', {})
                    
                    # Flatten the data structure for Pandas
                    product_tags = product.get('tags', [])
                    if isinstance(product_tags, str):
                        tags_str = product_tags
                    elif isinstance(product_tags, list):
                        tags_str = ", ".join(product_tags)
                    else:
                        tags_str = ""

                    # Extract locations
                    locations = []
                    inventory_item = node.get('inventoryItem') or {}
                    for level_edge in inventory_item.get('inventoryLevels', {}).get('edges', []):
                        loc_name = level_edge.get('node', {}).get('location', {}).get('name')
                        if loc_name:
                            locations.append(loc_name)
                    locations_str = ", ".join(locations)

                    all_variants.append({
                        'id': node.get('id'),
                        'variant_id': node.get('id'),
                        'sku': node.get('sku'),
                        'price': node.get('price'),
                        'compareAtPrice': node.get('compareAtPrice'),
                        'inventoryQuantity': node.get('inventoryQuantity'),
                        'inventoryItemId': inventory_item.get('id'),
                        'locations': locations_str,
                        'product_id': product.get('id'),
                        'handle': product.get('handle'),
                        'title': product.get('title'),
                        'tags': tags_str,
                        'templateSuffix': product.get('templateSuffix'),
                        'descriptionHtml': product.get('descriptionHtml')
                    })
                
                page_info = variants_connection.get('pageInfo', {})
                has_next_page = page_info.get('hasNextPage', False)
                cursor = page_info.get('endCursor')
                
            except Exception as e:
                 print(f"Error fetching batch: {e}")
                 has_next_page = False # Stop paginating this batch on error
                 
    print(f"Retrieved {len(all_variants)} total matching variants from Shopify.")
    df = pd.DataFrame(all_variants)
    
    # If no results, return empty dataframe with expected columns to prevent downstream errors
    if df.empty:
         return pd.DataFrame(columns=['id', 'sku', 'price', 'compareAtPrice', 'inventoryQuantity', 'inventoryItemId', 'locations', 'product_id', 'handle', 'title', 'tags', 'templateSuffix', 'descriptionHtml'])
         
    return df

def get_shopify_data_bulk(skus: list) -> pd.DataFrame:
    """
    Fetches ALL Shopify product variants via Bulk Operations API, then filters 
    locally by the provided list of SKUs. This is highly efficient for very large datasets (50,000+ SKUs).
    """
    print("Initiating Bulk Operation for Shopify Data...")
    
    # 1. Start the Bulk Operation
    query = """
    mutation {
      bulkOperationRunQuery(
        query: \"\"\"
        {
          products {
            edges {
              node {
                id
                handle
                title
                tags
                templateSuffix
                descriptionHtml
                mediaCount {
                  count
                }
                variants {
                  edges {
                    node {
                      id
                      sku
                      price
                      compareAtPrice
                      inventoryQuantity
                      inventoryItem {
                        id
                      }
                    }
                  }
                }
              }
            }
          }
        }
        \"\"\"
      ) {
        bulkOperation {
          id
          status
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    data = execute_graphql_query(query)
    errors = data.get('bulkOperationRunQuery', {}).get('userErrors', [])
    if errors:
        msg = ", ".join([e.get('message', str(e)) for e in errors])
        if "already in progress" in msg.lower():
            # If an operation is already running, we might as well just poll the current one since it contains products.
            print("A bulk operation is already in progress, will attempt to attach to it.")
        else:
            raise Exception(f"Failed to start bulk operation: {msg}")
        
    print("Started Shopify Bulk Export. This may take several minutes depending on catalog size...")
    
    # 2. Poll for completion
    poll_query = """
    query {
      currentBulkOperation {
        id
        status
        errorCode
        createdAt
        completedAt
        objectCount
        fileSize
        url
        partialDataUrl
      }
    }
    """
    
    url = None
    polling = True
    while polling:
        poll_data = execute_graphql_query(poll_query)
        current_op = poll_data.get('currentBulkOperation')
        
        if not current_op:
             raise Exception("Bulk operation disappeared or no active operation found.")
             
        status = current_op.get('status')
        print(f"Bulk Operation Status: {status} ({current_op.get('objectCount', 0)} objects processed)")
        
        if status == 'COMPLETED':
             url = current_op.get('url')
             break
        elif status in ['FAILED', 'CANCELED', 'EXPIRED']:
             raise Exception(f"Bulk operation failed with status: {status}, Error Code: {current_op.get('errorCode')}")
             
        time.sleep(5) # Wait 5 seconds before polling again
        
    if not url:
        return pd.DataFrame(columns=['id', 'sku', 'price', 'compareAtPrice', 'inventoryQuantity', 'inventoryItemId', 'product_id', 'handle', 'title', 'tags', 'templateSuffix', 'descriptionHtml'])
        
    # 3. Download the JSONL file
    print("Bulk Export completed. Downloading and processing data from Shopify...")
    response = requests.get(url)
    response.raise_for_status()
    
    # 4. Parse the JSONL file
    jsonl_content = response.text
    
    products_map = {}
    variants = []
    product_variant_counts = {}
    
    inventory_levels_map = {} # inventory_item_id -> list of location names
    
    for line in jsonl_content.splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        
        if '__parentId' not in obj:
            # It's a Product
            product_id = obj.get('id')
            
            # Map tags
            product_tags = obj.get('tags', [])
            if isinstance(product_tags, str):
                tags_str = product_tags
            elif isinstance(product_tags, list):
                tags_str = ", ".join(product_tags)
            else:
                tags_str = ""
                
            products_map[product_id] = {
                'product_id': product_id,
                'handle': obj.get('handle'),
                'title': obj.get('title'),
                'tags': tags_str,
                'templateSuffix': obj.get('templateSuffix'),
                'descriptionHtml': obj.get('descriptionHtml'),
                'mediaCount': obj.get('mediaCount', {}).get('count', 0)
            }
            product_variant_counts[product_id] = 0
        else:
            parent_id = obj.get('__parentId')
            
            # Check if it's an InventoryLevel (has 'location')
            if 'location' in obj:
                loc_name = obj.get('location', {}).get('name')
                if loc_name:
                    if parent_id not in inventory_levels_map:
                        inventory_levels_map[parent_id] = []
                    inventory_levels_map[parent_id].append(loc_name)
            else:
                # It's a Variant
                product = products_map.get(parent_id, {})
                
                # Increment variant count
                if parent_id in product_variant_counts:
                    product_variant_counts[parent_id] += 1
                
                # We'll stash variants now, and attach locations in a second pass 
                # because the inventoryLevels might appear after the variant in the JSONL stream.
                variants.append(obj)
                
    # Second pass: Map variants to their product info and inventory levels
    final_variants = []
    for var_obj in variants:
        parent_id = var_obj.get('__parentId')
        product = products_map.get(parent_id, {})
        
        inv_item_id = var_obj.get('inventoryItem', {}).get('id')
        locations_list = inventory_levels_map.get(inv_item_id, []) if inv_item_id else []
        locations_str = ", ".join(locations_list)
        
        final_variants.append({
            'id': var_obj.get('id'),
            'variant_id': var_obj.get('id'),
            'sku': var_obj.get('sku'),
            'price': var_obj.get('price'),
            'compareAtPrice': var_obj.get('compareAtPrice'),
            'inventoryQuantity': var_obj.get('inventoryQuantity'),
            'inventoryItemId': inv_item_id,
            'locations': locations_str,
            'product_id': product.get('product_id'),
            'handle': product.get('handle'),
            'title': product.get('title'),
            'tags': product.get('tags'),
            'templateSuffix': product.get('templateSuffix'),
            'descriptionHtml': product.get('descriptionHtml')
        })
            
    # Convert to DataFrame
    df = pd.DataFrame(final_variants)
    if df.empty:
         return pd.DataFrame(columns=['id', 'sku', 'price', 'compareAtPrice', 'inventoryQuantity', 'inventoryItemId', 'locations', 'product_id', 'handle', 'title', 'tags', 'templateSuffix', 'descriptionHtml'])
         
    # 5. Determine excessive media products
    excessive_media_products = []
    for pid, p_data in products_map.items():
        v_count = product_variant_counts.get(pid, 0)
        m_count = p_data.get('mediaCount', 0)
        if m_count > v_count:
            excessive_media_products.append({
                'product_id': pid,
                'handle': p_data.get('handle'),
                'title': p_data.get('title'),
                'media_count': m_count,
                'variant_count': v_count
            })
            
    excessive_media_df = pd.DataFrame(excessive_media_products)
         
    # 6. Filter locally by requested SKUs
    skus_lower = {str(s).lower().strip() for s in skus if pd.notna(s)}
    df['sku_lower'] = df['sku'].astype(str).str.lower().str.strip()
    
    filtered_df = df[df['sku_lower'].isin(skus_lower)].copy()
    filtered_df = filtered_df.drop(columns=['sku_lower'])
    
    print(f"Bulk Operation complete. Filtered down to {len(filtered_df)} matching variants out of {len(df)} total variants. Found {len(excessive_media_df)} products with excessive media.")
    return filtered_df, excessive_media_df

# --- Mutations ---

def update_product_tags(product_id: str, tags: str) -> tuple[bool, str]:
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          tags
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        "input": {
            "id": product_id,
            "tags": tags
        }
    }
    try:
        data = execute_graphql_query(mutation, variables)
        errors = data.get('productUpdate', {}).get('userErrors', [])
        if errors:
            msg = ", ".join([e.get('message', str(e)) for e in errors])
            print(f"Error updating tags: {msg}")
            return False, msg
        return True, ""
    except Exception as e:
        print(f"Exception updating tags: {e}")
        return False, str(e)

def remove_product_tag(product_id: str, tag: str) -> tuple[bool, str]:
    """Removes a specific tag from a product."""
    mutation = """
    mutation tagsRemove($id: ID!, $tags: [String!]!) {
      tagsRemove(id: $id, tags: $tags) {
        node {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        "id": product_id,
        "tags": [tag]
    }
    try:
        data = execute_graphql_query(mutation, variables)
        errors = data.get('tagsRemove', {}).get('userErrors', [])
        if errors:
            msg = ", ".join([e.get('message', str(e)) for e in errors])
            print(f"Error removing tag: {msg}")
            return False, msg
        return True, ""
    except Exception as e:
        print(f"Exception removing tag: {e}")
        return False, str(e)

def update_product_template_suffix(product_id: str, template_suffix: str) -> tuple[bool, str]:
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          templateSuffix
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        "input": {
            "id": product_id,
            "templateSuffix": template_suffix if template_suffix and template_suffix != 'None' else ""
        }
    }
    try:
        data = execute_graphql_query(mutation, variables)
        errors = data.get('productUpdate', {}).get('userErrors', [])
        if errors:
            msg = ", ".join([e.get('message', str(e)) for e in errors])
            print(f"Error updating template suffix: {msg}")
            return False, msg
        return True, ""
    except Exception as e:
        print(f"Exception updating template suffix: {e}")
        return False, str(e)

def update_variant_price(product_id: str, variant_id: str, price: str = None, compare_at_price: str = None) -> tuple[bool, str]:
    mutation = """
    mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants {
          id
          price
          compareAtPrice
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variant_input = {
        "id": variant_id
    }
    
    # Handle price
    if price is not None and not pd.isna(price) and str(price).lower() != 'nan':
        variant_input["price"] = str(price)
    
    # Handle compareAtPrice (remove a sale if it's null/NaN)
    if compare_at_price is not None and not pd.isna(compare_at_price) and str(compare_at_price).lower() != 'nan':
         variant_input["compareAtPrice"] = str(compare_at_price)
    else:
         variant_input["compareAtPrice"] = None
         
    variables = {
        "productId": product_id,
        "variants": [variant_input]
    }
    try:
        data = execute_graphql_query(mutation, variables)
        errors = data.get('productVariantsBulkUpdate', {}).get('userErrors', [])
        if errors:
            msg = ", ".join([e.get('message', str(e)) for e in errors])
            print(f"Error updating price: {msg}")
            return False, msg
        return True, ""
    except Exception as e:
        print(f"Exception updating price: {e}")
        return False, str(e)

def _get_primary_location_id() -> str:
    """Helper to fetch the first active location ID."""
    query = """
    {
      locations(first: 1) {
        edges {
          node {
            id
          }
        }
      }
    }
    """
    try:
        data = execute_graphql_query(query)
        edges = data.get('locations', {}).get('edges', [])
        if edges:
             return edges[0]['node']['id']
    except Exception as e:
         print(f"Error fetching location ID: {e}")
    return None

def update_inventory(inventory_item_id: str, quantity: int) -> tuple[bool, str]:
    location_id = _get_primary_location_id()
    if not location_id:
         msg = "Cannot update inventory without a valid location ID."
         print(msg)
         return False, msg
         
    mutation = """
    mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        inventoryAdjustmentGroup {
          createdAt
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        "input": {
            "reason": "correction",
             "setQuantities": [
                 {
                     "inventoryItemId": inventory_item_id,
                     "locationId": location_id,
                     "quantity": int(quantity)
                 }
             ]
        }
    }
    try:
        data = execute_graphql_query(mutation, variables)
        errors = data.get('inventorySetOnHandQuantities', {}).get('userErrors', [])
        if errors:
            msg = ", ".join([e.get('message', str(e)) for e in errors])
            print(f"Error updating inventory: {msg}")
            return False, msg
        return True, ""
    except Exception as e:
        print(f"Exception updating inventory: {e}")
        return False, str(e)

def create_product(title: str, variants: list, tags: str = "", template_suffix: str = "", product_type: str = "", vendor: str = "", body_html: str = "", seo_title: str = "", seo_description: str = "") -> tuple[bool, str]:
    """Creates a product entirely from scratch."""
    mutation = """
    mutation productCreate($input: ProductInput!, $media: [CreateMediaInput!]) {
      productCreate(input: $input, media: $media) {
        product {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    # Build standard Shopify variant input array
    variants_input = []
    
    # We will collect all unique option names across variants to set on the product
    # Assume up to 3 options. Usually if variants share options, their names are consistent.
    option_names = []
    if variants and len(variants) > 0:
         for i in range(1, 4):
              opt_name = variants[0].get(f'option{i}_name')
              if opt_name and not pd.isna(opt_name) and str(opt_name).strip() != '':
                   option_names.append(str(opt_name).strip())
                   
    for v in variants:
         v_in = {
             "sku": str(v.get('sku', '')),
             "price": str(v.get('price', '0.00'))
         }
         
         if v.get('compareAtPrice') and not pd.isna(v.get('compareAtPrice')) and str(v.get('compareAtPrice')).strip() != '':
             v_in['compareAtPrice'] = str(v.get('compareAtPrice'))
             
         if v.get('barcode') and not pd.isna(v.get('barcode')) and str(v.get('barcode')).strip() != '':
             v_in['barcode'] = str(v.get('barcode'))
             
         weight_val = v.get('weight') if v.get('weight') else v.get('grams')
         if weight_val and not pd.isna(weight_val) and str(weight_val).strip() != '':
             try:
                 v_in['weight'] = float(weight_val)
                 
                 # Shopify GraphQL typically wants POUNDS, OUNCES, KILOGRAMS, GRAMS
                 unit = str(v.get('weightUnit', '')).strip().upper()
                 if unit in ['POUNDS', 'OUNCES', 'KILOGRAMS', 'GRAMS']:
                     v_in['weightUnit'] = unit
                 elif 'lb' in unit.lower():
                     v_in['weightUnit'] = 'POUNDS'
                 elif 'kg' in unit.lower():
                     v_in['weightUnit'] = 'KILOGRAMS'
                 elif 'g' in unit.lower():
                     v_in['weightUnit'] = 'GRAMS'
                 else:
                     v_in['weightUnit'] = 'POUNDS'
             except ValueError:
                 pass
                 
         # Handle Options
         variant_options = []
         for i in range(1, 4):
              opt_val = v.get(f'option{i}_value')
              if opt_val and not pd.isna(opt_val) and str(opt_val).strip() != '':
                   variant_options.append(str(opt_val))
         if variant_options:
              v_in['options'] = variant_options
              
         variants_input.append(v_in)
         
    product_input = {
        "title": title,
        "vendor": vendor if vendor and not pd.isna(vendor) else "SyncShop",
        "status": "ACTIVE",
        "tags": tags,
        "variants": variants_input
    }
    
    if option_names:
         product_input['productOptions'] = [{"name": name, "position": i+1} for i, name in enumerate(option_names)]
    
    if body_html and str(body_html).lower() != 'nan':
         product_input['descriptionHtml'] = str(body_html)
         
    if (seo_title and str(seo_title).lower() != 'nan') or (seo_description and str(seo_description).lower() != 'nan'):
         product_input['seo'] = {}
         if seo_title and str(seo_title).lower() != 'nan':
             product_input['seo']['title'] = str(seo_title)
         if seo_description and str(seo_description).lower() != 'nan':
             product_input['seo']['description'] = str(seo_description)
    
    if template_suffix and template_suffix != 'None':
         product_input['templateSuffix'] = template_suffix
         
    if product_type and str(product_type).lower() != 'nan':
         product_input['productType'] = str(product_type)

    variables = {
        "input": product_input,
    }
    
    try:
        data = execute_graphql_query(mutation, variables)
        errors = data.get('productCreate', {}).get('userErrors', [])
        if errors:
            msg = ", ".join([e.get('message', str(e)) for e in errors])
            print(f"Error creating product: {msg}")
            return False, msg
        return True, ""
    except Exception as e:
        print(f"Exception creating product: {e}")
        return False, str(e)
        
def batch_process_mismatches(df_to_fix: pd.DataFrame) -> dict:
    """
    Takes a dataframe of mismatches to fix and processes them in batches via GraphQL.
    Returns a dictionary mapping the DataFrame index to an error message string.
    If an index is not in the returned dict, it was processed successfully.
    """
    errors = {}
    
    # --- 1. Batch Prices ---
    price_df = df_to_fix[df_to_fix['field'].isin(['price', 'compare_at_price', 'sticky_sale'])]
    if not price_df.empty:
        # Group by product_id since productVariantsBulkUpdate takes a single product ID per call
        for product_id, group in price_df.groupby('product_id'):
            variants_input = []
            idx_map = {} # Maps list index to DataFrame index
            for v_i, (idx, row) in enumerate(group.iterrows()):
                mut = {"id": row['variant_id']}
                if row['field'] == 'price':
                    mut['price'] = str(row['csv_value'])
                elif row['field'] == 'compare_at_price':
                    mut['compareAtPrice'] = str(row['csv_value'])
                elif row['field'] == 'sticky_sale':
                    mut['compareAtPrice'] = None
                variants_input.append(mut)
                idx_map[v_i] = idx
                
            mutation = """
            mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                userErrors {
                  field
                  message
                }
              }
            }
            """
            variables = {"productId": product_id, "variants": variants_input}
            try:
                data = execute_graphql_query(mutation, variables)
                user_errors = data.get('productVariantsBulkUpdate', {}).get('userErrors', [])
                for e in user_errors:
                    # e['field'] is usually ["variants", 0, "price"]
                    field_path = e.get('field', [])
                    if len(field_path) >= 2 and field_path[0] == 'variants' and isinstance(field_path[1], int):
                        v_idx = field_path[1]
                        if v_idx in idx_map:
                            errors[idx_map[v_idx]] = e.get('message', 'Unknown Error')
                    else:
                        # Fallback: assign error to all rows in group if we can't parse the index
                        for idx in idx_map.values():
                            errors[idx] = errors.get(idx, "") + e.get('message', 'Error') + " | "
            except Exception as e:
                for idx in idx_map.values():
                    errors[idx] = str(e)
                    
    # --- 2. Batch Tags, Template Suffixes, and HTML Descriptions (via Aliased Mutations) ---
    other_df = df_to_fix[~df_to_fix['field'].isin(['price', 'compare_at_price', 'sticky_sale', 'stale_clearance_tag'])]
    if not other_df.empty:
        # Process in chunks of 50 to avoid request size limits / timeout
        chunk_size = 50
        rows = list(other_df.iterrows())
        
        for c in range(0, len(rows), chunk_size):
            chunk = rows[c:c+chunk_size]
            query_parts = []
            variables = {}
            var_defs = []
            idx_to_alias = {}
            
            for i, (idx, row) in enumerate(chunk):
                alias = f"mut_{i}"
                idx_to_alias[alias] = idx
                
                if row['field'] in ['missing_oversize_tag', 'missing_clearance_tag']:
                    tag_to_add = "oversize" if row['field'] == 'missing_oversize_tag' else "clearance"
                    query_parts.append(f"""
                    {alias}: tagsAdd(id: $id_{i}, tags: $tags_{i}) {{ userErrors {{ message }} }}
                    """)
                    var_defs.append(f"$id_{i}: ID!, $tags_{i}: [String!]!")
                    variables[f"id_{i}"] = row['product_id']
                    variables[f"tags_{i}"] = [tag_to_add]
                    
                elif row['field'] == 'clearance_price_mismatch': # Needs tag removal + suffix clear
                    query_parts.append(f"""
                    {alias}_tag: tagsRemove(id: $id_{i}, tags: $tags_{i}) {{ userErrors {{ message }} }}
                    {alias}_suffix: productUpdate(input: $input_{i}) {{ userErrors {{ message }} }}
                    """)
                    var_defs.extend([f"$id_{i}: ID!, $tags_{i}: [String!]!", f"$input_{i}: ProductInput!"])
                    variables[f"id_{i}"] = row['product_id']
                    variables[f"tags_{i}"] = ["clearance"]
                    variables[f"input_{i}"] = {"id": row['product_id'], "templateSuffix": ""}
                    
                elif row['field'] == 'incorrect_template_suffix':
                    query_parts.append(f"""
                    {alias}: productUpdate(input: $input_{i}) {{ userErrors {{ message }} }}
                    """)
                    var_defs.append(f"$input_{i}: ProductInput!")
                    variables[f"input_{i}"] = {"id": row['product_id'], "templateSuffix": str(row['csv_value']) if row['csv_value'] != 'None' else ""}
                    
                elif row['field'] == 'h1_in_description':
                    query_parts.append(f"""
                    {alias}: productUpdate(input: $input_{i}) {{ userErrors {{ message }} }}
                    """)
                    var_defs.append(f"$input_{i}: ProductInput!")
                    variables[f"input_{i}"] = {"id": row['product_id'], "descriptionHtml": row.get('fixed_descriptionHtml', '')}
            
            if not query_parts:
                continue
                
            mutation = f"mutation batchUpdate({', '.join(var_defs)}) {{\n" + "\n".join(query_parts) + "\n}"
            try:
                data = execute_graphql_query(mutation, variables)
                for alias, _ in data.items():
                    res = data.get(alias, {})
                    user_errors = res.get('userErrors', [])
                    if user_errors:
                        msg = ", ".join([e.get('message', '') for e in user_errors])
                        # Map back to original idx
                        base_alias = alias.replace('_tag', '').replace('_suffix', '')
                        real_idx = idx_to_alias.get(base_alias)
                        if real_idx is not None:
                            errors[real_idx] = errors.get(real_idx, "") + msg + " | "
            except Exception as e:
                for idx in idx_to_alias.values():
                    errors[idx] = str(e)
                    
    # Clean up trailing pipes in errors
    for k, v in errors.items():
        errors[k] = v.strip(" |")
        
    # Handle manual review items automatically as errors
    manual_df = df_to_fix[df_to_fix['field'] == 'stale_clearance_tag']
    for idx, _ in manual_df.iterrows():
         errors[idx] = "Manual review required"

    return errors

if __name__ == "__main__":
    # Test getting location id
    print("Testing primary location:", _get_primary_location_id())
