import pandas as pd
from shopify_service import get_shopify_data_for_skus

def test():
    # Attempt to fetch a short list of SKUs. We don't know the exact SKUs the user is trying but any SKU might at least return an empty df. Let's try grabbing ANY products to see the location data.
    # Actually wait, let's just make a GraphQL query to grab the first 5 products to see their location data directly and see how the parser handles it.
    import os
    import requests
    import json
    from dotenv import load_dotenv

    load_dotenv(dotenv_path='.env')
    SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME', '').replace('.myshopify.com', '').replace('https://', '').replace('http://', '')
    ACCESS_TOKEN = os.getenv('SHOPIFY_API_ACCESS_TOKEN')
    GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/2024-07/graphql.json"
    HEADERS = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}

    product_query = """
    {
      products(first: 5) {
        edges {
          node {
            variants(first: 5) {
              edges {
                node {
                  sku
                  inventoryItem {
                    inventoryLevels(first: 5) {
                      edges {
                        node {
                          location {
                            name
                          }
                          quantities(names: ["available"]) {
                            name
                            quantity
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    response2 = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": product_query})
    data = response2.json()
    
    print("Fetched 5 products:")
    for p_edge in data.get('data', {}).get('products', {}).get('edges', []):
        for v_edge in p_edge.get('node', {}).get('variants', {}).get('edges', []):
             node = v_edge.get('node', {})
             sku = node.get('sku')
             inventory_item = node.get('inventoryItem') or {}
             locations = []
             for level_edge in inventory_item.get('inventoryLevels', {}).get('edges', []):
                  loc_name = level_edge.get('node', {}).get('location', {}).get('name')
                  quantities = level_edge.get('node', {}).get('quantities', [])
                  available = next((q.get('quantity') for q in quantities if q.get('name') == 'available'), None)
                  
                  if loc_name:
                       locations.append(f"{loc_name} (Avail: {available})")
             print(f"SKU {sku} -> Locations: {', '.join(locations)}")
             
if __name__ == '__main__':
    test()
