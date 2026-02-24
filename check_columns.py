from ftp_service import list_csv_files, get_csv_as_dataframe

files = list_csv_files()
clearance_files = [f for f in files if 'clearance' in f.lower()]

if clearance_files:
    print(f"Checking columns for: {clearance_files[0]}")
    # Don't rename columns so we see the raw ones
    import ftplib
    import pandas as pd
    from io import BytesIO
    from ftp_service import FTP_HOST, FTP_USER, FTP_PASS, FTP_DIR

    memory_file = BytesIO()
    try:
        with ftplib.FTP_TLS(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.prot_p()
            ftp.cwd(FTP_DIR)
            ftp.retrbinary(f"RETR {clearance_files[0]}", memory_file.write)
    except Exception as e:
        memory_file = BytesIO()
        with ftplib.FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.cwd(FTP_DIR)
            ftp.retrbinary(f"RETR {clearance_files[0]}", memory_file.write)

    memory_file.seek(0)
    df = pd.read_csv(memory_file, dtype=str)
    print("Columns:")
    for col in df.columns:
        print(f" - {col}")
else:
    print("No clearance file found.")
