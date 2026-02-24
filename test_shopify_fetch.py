import shopify_service
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv('.env.local')

def test_fetch():
    # Use some SKUs that were in the audit report
    skus = ['022-1003', '33-09003']
    print(f"Fetching data for SKUs: {skus}")
    df = shopify_service.get_shopify_data_for_skus(skus)
    print("Columns:", df.columns.tolist())
    if not df.empty:
        print(df[['sku', 'templateSuffix', 'tags']])
    else:
        print("No data found.")

if __name__ == "__main__":
    test_fetch()
