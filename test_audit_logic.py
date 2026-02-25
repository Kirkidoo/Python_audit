import pandas as pd
from audit_engine import check_mismatches

def test_audit_logic():
    # Mock CSV data
    # Item-Clearance: Clearance tag + discount -> expected: clearance
    # Item-Overweight: overweight tag -> expected: heavy-products
    # Item-Default: no tags -> expected: Default Template
    # Item-Both: Clearance + overweight + discount -> expected: heavy-products (priority)
    # Item-NoDiscount: Clearance tag + no discount (price==compare) -> expected: Default Template
    # Item-H1: description contains an h1 tag -> expected: mismatch for H1 tag
    csv_data = {
        'sku': ['Item-Clearance', 'Item-Overweight', 'Item-Default', 'Item-Both', 'Item-NoDiscount', 'Item-H1'],
        'price': [10.0, 50.0, 20.0, 15.0, 10.0, 45.0],
        'compareAtPrice': [20.0, 50.0, 20.0, 30.0, 10.0, 45.0],
        'tags': ['Clearance', 'overweight', '', 'Clearance, overweight', 'Clearance', ''],
        'templateSuffix': ['clearance', 'heavy-products', 'Default Template', 'heavy-products', 'Default Template', 'Default Template']
    }
    csv_df = pd.DataFrame(csv_data)

    # Mock Shopify data - Intentionally set to WRONG actual values so they show up as mismatches
    shopify_data = {
        'sku': ['Item-Clearance', 'Item-Overweight', 'Item-Default', 'Item-Both', 'Item-NoDiscount', 'Item-H1'],
        'price': [10.0, 50.0, 20.0, 15.0, 10.0, 45.0],
        'compareAtPrice': [20.0, 50.0, 20.0, 30.0, 10.0, 45.0],
        'tags': ['Clearance', 'overweight', '', 'Clearance, overweight', 'Clearance', ''],
        'templateSuffix': ['Default Template', 'Default Template', 'clearance', 'clearance', 'clearance', 'Default Template'], # Intentionally wrong to trigger mismatches
        'descriptionHtml': ['', '', '', '', '', '<p>Desc</p>\n<h1 class="main-title">A Big Title</h1>\n<p>More desc</p>']
    }
    shopify_df = pd.DataFrame(shopify_data)

    mismatch_df, missing_df, matched_count = check_mismatches(csv_df, shopify_df, 'clearance_file.csv')
    
    if not mismatch_df.empty:
        # Filter for template mismatches
        template_mismatches = mismatch_df[mismatch_df['field'] == 'incorrect_template_suffix']
        print("Template Mismatches:")
        print(template_mismatches[['sku', 'csv_value', 'shopify_value']].to_string(index=False))
        
        # Filter for H1 mismatches
        h1_mismatches = mismatch_df[mismatch_df['field'] == 'h1_in_description']
        print("\nH1 Mismatches:")
        if not h1_mismatches.empty:
            print(f"Found {len(h1_mismatches)} mismatches.")
            for _, row in h1_mismatches.iterrows():
                print(f"SKU: {row['sku']}")
                print(f"Fixed Description:\n{row['fixed_descriptionHtml']}")
        else:
            print("No H1 mismatches found.")
    else:
        print("No mismatches found.")

if __name__ == '__main__':
    test_audit_logic()
