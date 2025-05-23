import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import warnings # For suppressing InsecureRequestWarning
from urllib3.exceptions import InsecureRequestWarning # For suppressing InsecureRequestWarning
from google.cloud import storage # Import Google Cloud Storage library

# Suppress only the InsecureRequestWarning caused by verify=False
warnings.simplefilter('ignore', InsecureRequestWarning)

# --- Configuration ---
BASE_URL = "https://bso.hscni.net/directorates/operations/family-practitioner-services/directorates-operations-family-practitioner-services-information-unit/general-pharmaceutical-services-and-prescribing-statistics/dispensing-by-contractor/"
# OUTPUT_FOLDER = r"D:\Data2" # Removed: Local output folder
OUTPUT_FILENAME = "merged_bso_dispensing_data.csv"
COLUMNS_TO_KEEP = ['Practice', 'Chemist', 'Year', 'Month', 'Number of Items'] # Case-sensitive

# Temporary directory to store downloaded XLSX files before merging
# TEMP_DOWNLOAD_DIR = os.path.join(OUTPUT_FOLDER, "temp_bso_xlsxs") # Removed: Local temp dir
TEMP_DOWNLOAD_DIR = "/tmp/temp_bso_xlsxs" # Use /tmp in Cloud Functions

# Google Cloud Storage Configuration
BUCKET_NAME = "project-2025-bso-data-storage" # Replace with your GCS bucket name
# OUTPUT_GCS_PATH = f"bso_data/{OUTPUT_FILENAME}" # Optional: Prefix within the bucket
OUTPUT_GCS_PATH = OUTPUT_FILENAME # Save directly in the bucket root

def create_directory_if_not_exists(directory_path):
    """Creates a directory if it doesn't already exist."""
    if not os.path.exists(directory_path):
        try:
            os.makedirs(directory_path)
            print(f"Created directory: {directory_path}")
        except OSError as e:
            print(f"Error creating directory {directory_path}: {e}")
            return False
    return True

def download_file(file_url, download_path):
    """Downloads a file from a given URL to the specified path."""
    try:
        print(f"Downloading {os.path.basename(download_path)} from {file_url}...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # WARNING: SSL verification disabled!
        response = requests.get(file_url, stream=True, headers=headers, verify=False)
        response.raise_for_status()

        with open(download_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded: {download_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {file_url}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while downloading {file_url}: {e}")
        return False

def main(request):
    """Main function to scrape, download, process, and merge XLSX files and upload to GCS."""
    if not create_directory_if_not_exists("/tmp"): # Use /tmp for Cloud Functions
        return
    if not create_directory_if_not_exists(TEMP_DOWNLOAD_DIR):
        return

    print(f"Attempting to scrape data from: {BASE_URL}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # WARNING: SSL verification disabled!
        page_response = requests.get(BASE_URL, headers=headers, verify=False)
        page_response.raise_for_status()
        html_content = page_response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {BASE_URL}: {e}")
        return

    soup = BeautifulSoup(html_content, 'html.parser')
    xlsx_links_found = []

    print("Searching for XLSX file links on the page...")
    for link_tag in soup.find_all('a', href=True):
        href = link_tag.get('href', '')

        # Look for links ending with .xlsx
        if href.endswith('.xlsx'):
            if ('wp-content/uploads/' in href or 'bso.hscni.net' in href):
                absolute_file_url = urljoin(BASE_URL, href)
                if absolute_file_url not in xlsx_links_found:
                    xlsx_links_found.append(absolute_file_url)
                    print(f"Found and added potential XLSX link: {absolute_file_url}")

    if not xlsx_links_found:
        print("No XLSX file links matching all criteria were found on the page.")
        return

    all_dataframes = []
    downloaded_file_paths = []

    for file_url in xlsx_links_found:
        file_name = file_url.split('/')[-1].split('?')[0]
        # Ensure filename ends with .xlsx for consistency if query params were present
        if not file_name.lower().endswith('.xlsx'):
            file_name += ".xlsx"
        temp_file_path = os.path.join(TEMP_DOWNLOAD_DIR, file_name)

        if download_file(file_url, temp_file_path):
            downloaded_file_paths.append(temp_file_path)
        else:
            print(f"Skipping processing for {file_name} due to download error.")
            continue

    if not downloaded_file_paths:
        print("No files were successfully downloaded. Exiting.")
        return

    print(f"\nStarting to process {len(downloaded_file_paths)} downloaded XLSX files...")
    for file_path in downloaded_file_paths:
        print(f"Processing file: {file_path}")
        try:
            # Read Excel file, typically the first sheet is read by default
            # Ensure 'openpyxl' is installed: pip install openpyxl
            df_temp = pd.read_excel(file_path, sheet_name=0)
            print(f"Successfully read {os.path.basename(file_path)}.")

            if df_temp.empty:
                print(f"Warning: File {os.path.basename(file_path)} is empty. Skipping.")
                continue

            available_cols_to_keep = [col for col in COLUMNS_TO_KEEP if col in df_temp.columns]

            if not available_cols_to_keep:
                print(f"Warning: None of the desired columns {COLUMNS_TO_KEEP} found in {os.path.basename(file_path)}. Skipping this file.")
                continue

            missing_in_this_file = [col for col in COLUMNS_TO_KEEP if col not in available_cols_to_keep]
            if missing_in_this_file:
                print(f"Warning: File {os.path.basename(file_path)} is missing columns: {', '.join(missing_in_this_file)}. Only available desired columns will be used.")

            df_filtered = df_temp[available_cols_to_keep]
            all_dataframes.append(df_filtered)
            print(f"Added data from {os.path.basename(file_path)} to the merge list (columns: {', '.join(available_cols_to_keep)}).")

        except FileNotFoundError:
            print(f"Error: File not found at {file_path} during processing. Skipping.")
        except ImportError:
            print(f"Error: Missing library for reading Excel files. Please install 'openpyxl' (pip install openpyxl). Skipping {file_path}.")
        except Exception as e: # Catch other pandas or file processing errors
            print(f"Error processing file {file_path}: {e}. Skipping this file.")

    if not all_dataframes:
        print("No data was successfully processed from any XLSX file. Cannot create merged file.")
        return

    print("\nConcatenating all processed DataFrames...")
    try:
        master_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        print("Concatenation complete.")
    except Exception as e:
        print(f"Error during final concatenation of DataFrames: {e}")
        return

    for col in COLUMNS_TO_KEEP:
        if col not in master_df.columns:
            master_df[col] = pd.NA

    master_df = master_df[COLUMNS_TO_KEEP]

    # Output to Google Cloud Storage
    try:
        # Save DataFrame to CSV in memory
        csv_buffer = master_df.to_csv(index=False, encoding='utf-8')

        # Initialize GCS client
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(OUTPUT_GCS_PATH)

        # Upload CSV data to GCS
        blob.upload_from_string(csv_buffer, 'text/csv')

        print(f"\nSuccessfully saved merged data to: gs://{BUCKET_NAME}/{OUTPUT_GCS_PATH}")
        print(f"Merged DataFrame info: {master_df.shape[0]} rows, {master_df.shape[1]} columns.")
        print("First 5 rows of merged data:")
        print(master_df.head())

    except Exception as e:
        print(f"Error saving merged data to Google Cloud Storage: {e}")

    # Optional: Clean up temporary downloaded files
    # print("\nCleaning up temporary downloaded files...")
    # for file_path in downloaded_file_paths:
    #     try:
    #         os.remove(file_path)
    #     except Exception as e:
    #         print(f"Error removing temporary file {file_path}: {e}")
    # try:
    #     os.rmdir(TEMP_DOWNLOAD_DIR) # Only removes if empty
    # except Exception as e:
    #     print(f"Error removing temporary directory {TEMP_DOWNLOAD_DIR}: {e} (may not be empty).")

if __name__ == "__main__":
    # Ensure you have the necessary library for reading .xlsx files
    # You might need to install it: pip install openpyxl
    main()