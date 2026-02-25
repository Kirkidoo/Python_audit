import os
import ftplib
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from the parent directory's .env
load_dotenv(dotenv_path='.env')

FTP_HOST = os.getenv('GAMMA_FTP_HOST')
FTP_USER = os.getenv('GAMMA_FTP_USER')
FTP_PASS = os.getenv('GAMMA_FTP_PASSWORD')
FTP_DIR = os.getenv('FTP_DIRECTORY', '/Gamma_Product_Files/Shopify_Files/')

def list_csv_files():
    """Returns a list of CSV files available in the FTP directory."""
    files = []
    try:
        print(f"Connecting to FTP: {FTP_HOST}")
        with ftplib.FTP_TLS(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.prot_p() # switch to secure data connection
            ftp.cwd(FTP_DIR)
            files = [f for f in ftp.nlst() if f.lower().endswith('.csv')]
            print(f"Found {len(files)} CSV files: {files}")
    except Exception as e:
        print(f"Error listing files: {e}")
        # Fallback to non-secure if needed based on original TS logic
        # For simplicity in this script, we'll try secure first.
    return files

def get_csv_as_dataframe(filename: str) -> pd.DataFrame:
    """Downloads a CSV from FTP and loads it directly into a Pandas DataFrame."""
    print(f"Downloading {filename} from FTP...")
    memory_file = BytesIO()
    
    try:
        # Try Secure FTP first
        with ftplib.FTP_TLS(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.prot_p()
            ftp.cwd(FTP_DIR)
            ftp.retrbinary(f"RETR {filename}", memory_file.write)
    except Exception as e:
        print(f"Secure FTP failed ({e}). Trying insecure...")
        # Fallback to standard FTP
        memory_file = BytesIO() # Reset buffer
        with ftplib.FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.cwd(FTP_DIR)
            ftp.retrbinary(f"RETR {filename}", memory_file.write)

    memory_file.seek(0)
    print(f"Parsing {filename} into DataFrame...")
    # Read CSV, forcing all columns to string initially to avoid type inference issues
    df = pd.read_csv(memory_file, dtype=str) 
    
    # Standardize column names to lowercase with underscores to match our engine expectations
    rename_map = {
        'SKU': 'sku',
        'Handle': 'handle',
        'Title': 'title',
        'Vendor': 'vendor',
        'Body (HTML)': 'body_html',
        'Type': 'type',
        'Product Type': 'product_type',
        'Category': 'category',
        'Tags': 'tags',
        'Price': 'price',
        'Compare At Price': 'compareAtPrice',
        'Cost Per Item': 'cost',
        'Variant Inventory Qty': 'inventory',
        'Variant Grams': 'grams',
        'Weight': 'weight',
        'Variant Weight': 'weight',
        'Variant Weight Unit': 'weightUnit',
        'Variant Barcode': 'barcode',
        'Variant Image': 'image',
        'Option1 Name': 'option1_name',
        'Option1 Value': 'option1_value',
        'Option2 Name': 'option2_name',
        'Option2 Value': 'option2_value',
        'Option3 Name': 'option3_name',
        'Option3 Value': 'option3_value',
        'SEO Title': 'seo_title',
        'SEO Description': 'seo_description',
        'Template Suffix': 'templateSuffix'
    }
    # Create an inverse map to only keep the first occurrence of a target column
    # since we might have multiple variants pointing to the same key (e.g. Weight/Variant Weight)
    actual_rename = {}
    for source, target in rename_map.items():
        if source in df.columns and target not in actual_rename.values():
             actual_rename[source] = target
             
    df = df.rename(columns=actual_rename)
    # Ensure all expected columns exist even if not in CSV to prevent KeyErrors
    for col in rename_map.values():
         if col not in df.columns:
              df[col] = pd.NA
              
    return df

if __name__ == "__main__":
    files = list_csv_files()
    if files:
        # Test download the first file
        df = get_csv_as_dataframe(files[0])
        print(f"Successfully loaded {len(df)} rows.")
        print(df.head())
