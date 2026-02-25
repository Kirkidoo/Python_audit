import os
from dotenv import load_dotenv
import requests
import json

load_dotenv(dotenv_path='.env')
SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME', '').replace('.myshopify.com', '').replace('https://', '').replace('http://', '')
ACCESS_TOKEN = os.getenv('SHOPIFY_API_ACCESS_TOKEN')
GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/2024-07/graphql.json"
HEADERS = {"X-Shopify-Access-Token": ACCESS_TOKEN, "Content-Type": "application/json"}

query = """
{
  locations(first: 5) {
    edges {
      node {
        id
        name
      }
    }
  }
}
"""
response = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query})
print("Locations:")
print(json.dumps(response.json(), indent=2))

product_query = """
{
  products(first: 1) {
    edges {
      node {
        variants(first: 1) {
          edges {
            node {
              inventoryItem {
                inventoryLevels(first: 5) {
                  edges {
                    node {
                      location {
                        id
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
print("\nProduct Inventory Levels:")
print(json.dumps(response2.json(), indent=2))
