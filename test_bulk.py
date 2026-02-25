from shopify_service import get_shopify_data_bulk

print("Testing get_shopify_data_bulk...")
try:
    df1, df2 = get_shopify_data_bulk(['dummy_sku'])
    print(f"Variants DF length: {len(df1)}")
    print(f"Excessive Media DF length: {len(df2)}")
    if not df2.empty:
        print(df2.head())
except Exception as e:
    print(f"Error: {e}")
