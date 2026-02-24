import sys
from ftp_service import list_csv_files, get_csv_as_dataframe
from shopify_service import get_shopify_data_for_skus
from audit_engine import check_mismatches, check_stale_clearance

def run_audit(target_file: str = None):
    # 1. Provide clear options for the user
    files = list_csv_files()
    if not files:
        print("No CSV files found in FTP.")
        return
        
    if not target_file:
         print("Available files:")
         for i, f in enumerate(files):
             print(f"[{i}] {f}")
         choice = input("Enter the index of the file to audit: ")
         try:
             target_file = files[int(choice)]
         except (ValueError, IndexError):
             print("Invalid choice.")
             return
             
    print(f"\n--- Starting Audit for {target_file} ---")
    
    # 2. Fetch FTP Data
    csv_df = get_csv_as_dataframe(target_file)
    if csv_df.empty:
        print("File is empty.")
        return
        
    skus = csv_df['sku'].dropna().unique().tolist()
    print(f"Total unique SKUs in CSV: {len(skus)}")
    
    # 3. Fetch Shopify Data
    shopify_df = get_shopify_data_for_skus(skus)
    
    # 4. Run Audit
    mismatch_df, matched_count, missing_count = check_mismatches(csv_df, shopify_df, target_file)
    
    # check stale clearance if applicable
    if 'clearance' in target_file.lower():
        # Ideally, we query Shopify specifically for `tag:clearance` here to check the whole catalog.
        # But for testing the engine with immediate data, we check amongst what we fetched.
        stale_df = check_stale_clearance(shopify_df, csv_df)
        if not stale_df.empty:
             mismatch_df = pd.concat([mismatch_df, stale_df], ignore_index=True)
             
    # 5. Output Report
    print("\n--- Audit Summary ---")
    print(f"Matched SKUs: {matched_count}")
    print(f"Missing in Shopify: {missing_count}")
    print(f"Total Mismatches Found: {len(mismatch_df)}")
    
    if not mismatch_df.empty:
         report_file = "audit_report.csv"
         mismatch_df.to_csv(report_file, index=False)
         print(f"Mismatch report saved to {report_file}")
         print("\nSample of Mismatches:")
         print(mismatch_df.head(10).to_string())
    else:
         print("All good! No mismatches found.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_audit(sys.argv[1])
    else:
        run_audit()
