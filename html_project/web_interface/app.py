import os
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session, jsonify
import io # For sending file in memory
import logging
import time
from google.cloud import storage # Import Google Cloud Storage

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = 'your_very_secret_key_for_session_and_flash_bso_analyzer_v7_search' # CHANGE THIS IN PRODUCTION!

# --- Configuration ---
# Update to read from GCS
GCS_BUCKET_NAME = "project-2025-bso-data-storage" # << REPLACE WITH YOUR GCS BUCKET NAME
GCS_DATA_FILENAME = "merged_bso_dispensing_data.csv"

# CHEMIST_DETAILS_CSV_PATH should still be part of your Flask app's deployment
# (i.e., included in the same directory as app.py in your Cloud Run container)
CHEMIST_DETAILS_CSV_PATH = "formatted_chemist_list.csv"


# --- Helper Functions ---
_chemist_details_df_cache = None
_chemist_details_mtime_cache = 0

# Modified to read from GCS
def load_csv_data_from_gcs(bucket_name, file_name, file_description="CSV"):
    app.logger.info(f"Attempting to load data from GCS bucket '{bucket_name}', file '{file_name}' ({file_description})...")
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Download blob to a BytesIO object
        csv_data = blob.download_as_bytes()
        app.logger.info(f"Successfully downloaded {file_name} from GCS.")
        
        # Read CSV data into pandas DataFrame
        df = pd.read_csv(io.BytesIO(csv_data))
        app.logger.info(f"Successfully loaded data from GCS {file_description} file.")
        return df
    except Exception as e:
        app.logger.error(f"Error loading {file_description} file from GCS: {e}")
        flash(f"Error: Could not load {file_description} data from cloud. Please try again later. Details: {e}", "danger")
        return pd.DataFrame() # Return empty DataFrame on error

# Load chemist details from local file (part of the Flask app's deployment)
def load_chemist_details(file_path):
    global _chemist_details_df_cache, _chemist_details_mtime_cache
    
    # Check if the file has been modified since last load
    try:
        # In Cloud Run, the local file system is ephemeral.
        # For a truly dynamic cache, you might need to store mtime in a database or GCS metadata.
        # For simplicity, we'll just reload it each time for now, or assume it's static after deployment.
        # If formatted_chemist_list.csv changes, you'd need to redeploy the Flask app.
        if _chemist_details_df_cache is not None: # Simple in-memory cache for one deployment instance
            app.logger.info("Chemist details data is cached.")
            return _chemist_details_df_cache
        else:
            app.logger.info(f"Loading chemist details from {file_path} (first load).")
            df = pd.read_csv(file_path)
            _chemist_details_df_cache = df
            app.logger.info(f"Chemist details loaded: {len(df)} entries.")
            return df
    except FileNotFoundError:
        app.logger.error(f"Chemist details file not found at {file_path}")
        flash(f"Error: Chemist details file not found at {file_path}", "danger")
        return pd.DataFrame()
    except Exception as e:
        app.logger.error(f"Error loading chemist details CSV: {e}")
        flash(f"Error loading chemist details CSV: {e}", "danger")
        return pd.DataFrame()

# Define your main data DataFrame outside of route functions to load once
# and then access globally or through a similar caching mechanism.
# We will load it within the route for simplicity, but for large files
# consider loading it once at app startup if feasible within Cloud Run's cold start limits.
# For now, it will be loaded on demand per request.

# --- Route Definitions ---

@app.route('/')
def index():
    # Load main data from GCS whenever the index page is accessed
    df_main_data = load_csv_data_from_gcs(GCS_BUCKET_NAME, GCS_DATA_FILENAME, "main BSO dispensing data")
    
    # Load chemist details from local deployment
    df_chemist_details = load_chemist_details(CHEMIST_DETAILS_CSV_PATH)
    
    if df_main_data.empty or df_chemist_details.empty:
        # flash messages are handled in load_csv_data_from_gcs and load_chemist_details
        return render_template('index.html', years=[], months=[])
    
    # Original logic to extract years and months
    if 'Year' in df_main_data.columns and 'Month' in df_main_data.columns:
        years = sorted(df_main_data['Year'].dropna().unique().tolist(), reverse=True)
        months = sorted(df_main_data['Month'].dropna().unique().tolist())
    else:
        years = []
        months = []
        app.logger.warning("Missing 'Year' or 'Month' columns in main data.")
        flash("Data is missing 'Year' or 'Month' columns. Filtering might be limited.", "warning")

    # Pass chemist IDs for search functionality
    # CHANGED: Use 'Chemist ID' instead of 'ID'
    chemist_ids_json = df_chemist_details[['Chemist ID', 'Name']].to_dict(orient='records') if not df_chemist_details.empty else []

    return render_template('index.html', years=years, months=months, chemist_ids_json=chemist_ids_json)

# Rest of your Flask app's routes and logic...
# Make sure any other function that uses INPUT_DATA_CSV_FILE_PATH
# now calls load_csv_data_from_gcs instead.

@app.route('/get_chemist_suggestions')
def get_chemist_suggestions():
    query = request.args.get('query', '').lower()
    df_chemist_details = load_chemist_details(CHEMIST_DETAILS_CSV_PATH)
    if df_chemist_details.empty:
        return jsonify([])

    filtered_chemists = df_chemist_details[
        df_chemist_details['Name'].str.lower().str.contains(query, na=False) |
        df_chemist_details['Chemist ID'].astype(str).str.contains(query, na=False)
    ].head(10)[['Chemist ID', 'Name', 'Full Address']].to_dict(orient='records') # ADDED 'Full Address' here
    return jsonify(filtered_chemists)

# Make sure to update the data filtering logic (e.g., in /filter_data)
# to use the DataFrame returned by load_csv_data_from_gcs.
@app.route('/filter_data', methods=['POST'])
def filter_data():
    try:
        # Load the latest data from GCS for each filter request
        df_main_data = load_csv_data_from_gcs(GCS_BUCKET_NAME, GCS_DATA_FILENAME, "main BSO dispensing data")
        df_chemist_details = load_chemist_details(CHEMIST_DETAILS_CSV_PATH)

        if df_main_data.empty or df_chemist_details.empty:
            return redirect(url_for('index')) # Flash messages already handled

        # Get filters
        chemist_id_1 = request.form.get('selected_chemist_id_1')
        chemist_id_2 = request.form.get('selected_chemist_id_2')
        selected_year = request.form.get('year')
        selected_month = request.form.get('month')

        # Determine view type
        view_type = "single"
        if chemist_id_1 and chemist_id_2:
            view_type = "comparison"
        elif chemist_id_1:
            view_type = "single"
        else:
            flash("Please select at least one chemist ID to filter.", "warning")
            return redirect(url_for('index'))

        # Initialize variables for rendering
        table_html = ""
        table_html_1 = ""
        table_html_2 = ""
        chemist_id_display = ""
        download_filename = ""
        
        # New variables for addresses
        chemist_address_1 = ""
        chemist_address_2 = ""
        
        # Convert 'Chemist' in main data to string for consistent comparison
        df_main_data['Chemist'] = df_main_data['Chemist'].astype(str)
        # Convert 'Chemist ID' in chemist details to string for consistent comparison
        df_chemist_details['Chemist ID'] = df_chemist_details['Chemist ID'].astype(str)

        # Helper function for aggregation and formatting
        def process_chemist_data(df_data, chemist_id, df_details):
            if chemist_id:
                # Filter data for the specific chemist
                filtered_df = df_data[df_data['Chemist'] == chemist_id].copy()

                # Apply year filter
                if selected_year and selected_year != 'all':
                    filtered_df = filtered_df[filtered_df['Year'] == int(selected_year)].copy()

                # Apply month filter
                if selected_month and selected_month != 'all':
                    filtered_df = filtered_df[filtered_df['Month'] == selected_month].copy()

                if not filtered_df.empty:
                    # Group by Chemist, Year, Month and sum 'Number of Items'
                    aggregated_df = filtered_df.groupby(['Chemist', 'Year', 'Month'])['Number of Items'].sum().reset_index()
                    
                    # Merge with df_details to get the 'Name' of the chemist
                    final_display_df = pd.merge(
                        aggregated_df,
                        df_details[['Chemist ID', 'Name']], # Only select necessary columns from chemist details
                        left_on='Chemist',
                        right_on='Chemist ID',
                        how='left'
                    )
                    
                    # Drop the original 'Chemist' column and rename 'Chemist ID' from the merge
                    final_display_df = final_display_df.drop(columns=['Chemist']).rename(columns={'Chemist ID': 'Chemist ID_from_details'})
                    final_display_df = final_display_df.rename(columns={'Chemist ID_from_details': 'Chemist ID'})

                    # Reorder columns for final display
                    display_columns_order = ['Chemist ID', 'Name', 'Year', 'Month', 'Number of Items']
                    final_display_df = final_display_df[display_columns_order]

                    # --- SORTING: Most recent to least recent ---
                    # Sort by Year (descending), then Month (descending), then Chemist ID (ascending)
                    final_display_df = final_display_df.sort_values(
                        by=['Year', 'Month', 'Chemist ID'],
                        ascending=[False, False, True]
                    ).reset_index(drop=True)
                    
                    return final_display_df
            return pd.DataFrame(columns=['Chemist ID', 'Name', 'Year', 'Month', 'Number of Items']) # Return empty df with expected columns

        if view_type == "single":
            chemist_info = df_chemist_details[df_chemist_details['Chemist ID'] == chemist_id_1]
            chemist_name = chemist_info['Name'].iloc[0] if not chemist_info.empty else chemist_id_1
            chemist_address_1 = chemist_info['Full Address'].iloc[0] if not chemist_info.empty and 'Full Address' in chemist_info.columns else "Address not found" # Get address
            chemist_id_display = f"{chemist_id_1} ({chemist_name})"
            
            final_display_df = process_chemist_data(df_main_data, chemist_id_1, df_chemist_details)
            download_filename = f"chemist_{chemist_id_1}_filtered_data.xlsx"
            table_html = final_display_df.to_html(classes='data-table table table-striped table-hover', index=False)

        elif view_type == "comparison":
            chemist_info_1 = df_chemist_details[df_chemist_details['Chemist ID'] == chemist_id_1]
            chemist_name_1 = chemist_info_1['Name'].iloc[0] if not chemist_info_1.empty else chemist_id_1
            chemist_address_1 = chemist_info_1['Full Address'].iloc[0] if not chemist_info_1.empty and 'Full Address' in chemist_info_1.columns else "Address not found" # Get address
            
            chemist_info_2 = df_chemist_details[df_chemist_details['Chemist ID'] == chemist_id_2]
            chemist_name_2 = chemist_info_2['Name'].iloc[0] if not chemist_info_2.empty else chemist_id_2
            chemist_address_2 = chemist_info_2['Full Address'].iloc[0] if not chemist_info_2.empty and 'Full Address' in chemist_info_2.columns else "Address not found" # Get address
            
            chemist_id_display = f"{chemist_id_1} ({chemist_name_1}) vs {chemist_id_2} ({chemist_name_2})"
            
            final_display_df_1 = process_chemist_data(df_main_data, chemist_id_1, df_chemist_details)
            final_display_df_2 = process_chemist_data(df_main_data, chemist_id_2, df_chemist_details)

            table_html_1 = final_display_df_1.to_html(classes='data-table table table-striped table-hover', index=False)
            table_html_2 = final_display_df_2.to_html(classes='data-table table table-striped table-hover', index=False)

            # For download, combine data from both chemists for the Excel file
            combined_download_df = pd.concat([final_display_df_1, final_display_df_2], ignore_index=True)
            # Ensure combined_download_df is sorted for consistent download
            combined_download_df = combined_download_df.sort_values(
                by=['Chemist ID', 'Year', 'Month'],
                ascending=[True, False, False] # Sort by chemist ID then by most recent
            ).reset_index(drop=True)

            # Use combined_download_df for session storage for Excel download
            session['processed_data'] = combined_download_df.to_dict(orient='records')
            download_filename = f"comparison_{chemist_id_1}_vs_{chemist_id_2}_filtered_data.xlsx"
            session['output_filename'] = download_filename
            
            # Pass individual tables and addresses to template for side-by-side display
            return render_template('results.html', 
                                   table_data_1=table_html_1, 
                                   table_data_2=table_html_2,
                                   chemist_id_display=chemist_id_display, 
                                   view_type=view_type, 
                                   download_filename=download_filename,
                                   chemist_name_1=chemist_name_1,
                                   chemist_name_2=chemist_name_2,
                                   chemist_address_1=chemist_address_1, # Pass address 1
                                   chemist_address_2=chemist_address_2) # Pass address 2

        # Store data for download for single view
        if view_type == "single":
            session['processed_data'] = final_display_df.to_dict(orient='records')
            session['output_filename'] = download_filename

        if final_display_df.empty:
            flash("No data found for the selected filters. Please adjust your criteria.", "info")

        # Render for single view
        return render_template('results.html', 
                               table_data=table_html, 
                               chemist_id_display=chemist_id_display, 
                               view_type=view_type, 
                               download_filename=download_filename,
                               chemist_address_1=chemist_address_1) # Pass address for single view

    except Exception as e:
        app.logger.error(f"Error during data filtering: {e}")
        flash(f"An unexpected error occurred during filtering: {e}. Please try again.", "danger")
        return redirect(url_for('index'))

@app.route('/download_excel')
def download_excel():
    if 'processed_data' in session and session['processed_data']:
        try:
            data_records = session.pop('processed_data', None)
            output_filename = session.pop('output_filename', 'filtered_data.xlsx')
            if not data_records:
                flash("No data found to download. Please try filtering again.", "warning")
                return redirect(url_for('index'))
            df_to_download = pd.DataFrame(data_records)
            excel_buffer = io.BytesIO()
            df_to_download.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            return send_file(excel_buffer, as_attachment=True, download_name=output_filename,
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        except Exception as e:
            app.logger.error(f"Error during Excel download: {e}")
            flash(f"Error generating Excel file for download: {e}", "danger")
            return redirect(url_for('index'))
    else:
        flash("No processed data available for download. Please filter first.", "warning")
        return redirect(url_for('index'))

def flash_is_pending():
    return bool(request.environ.get('werkzeug.request') and request.environ['werkzeug.request'].session.get('_flashes'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # When running locally for testing, you might need to adjust BUCKET_NAME in main.py
    # or mock GCS access if you don't want to connect to real GCS during local dev.
    # For Cloud Run, this will correctly connect to GCS.
    app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 8080))