import os
import requests
import json
from dotenv import load_dotenv

load_dotenv('.env')

SHOP_NAME = os.getenv('SHOPIFY_SHOP_NAME', '').replace('.myshopify.com', '').replace('https://', '').replace('http://', '')
ACCESS_TOKEN = os.getenv('SHOPIFY_API_ACCESS_TOKEN')
API_VERSION = '2024-07'
GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

query = """
{
  products(first: 1) {
    edges {
      node {
        id
        mediaCount {
          count
        }
      }
    }
  }
}
"""

response = requests.post(GRAPHQL_URL, headers=HEADERS, json={"query": query})
print(json.dumps(response.json(), indent=2))
