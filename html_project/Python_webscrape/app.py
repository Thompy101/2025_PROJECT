import os
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session, jsonify
import io # For sending file in memory
import logging
import time 

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = 'your_very_secret_key_for_session_and_flash_bso_analyzer_v7_search' # Unique secret key

# --- Configuration ---
INPUT_DATA_CSV_FILE_PATH = r"D:\Data2\merged_bso_dispensing_data.csv" 
CHEMIST_DETAILS_CSV_PATH = r"E:\Project\2025_PROJECT\processed_chemist_data\formatted_chemist_list.csv" # EXAMPLE PATH - PLEASE UPDATE

# --- Helper Functions ---
# Cache for chemist details to avoid reading the file on every search request
# For a production app, consider a more robust caching mechanism or loading at startup if file is static
_chemist_details_df_cache = None
_chemist_details_mtime_cache = 0

def load_csv_data(file_path, file_description="CSV"):
    app.logger.info(f"Attempting to load data from {file_description} file: {file_path}")
    if not os.path.exists(file_path):
        app.logger.error(f"Input {file_description} file not found at {file_path}")
        flash(f"Error: Input {file_description} file not found at {file_path}. Please ensure the file exists.", "danger")
        return pd.DataFrame()
    try:
        # Check if it's the chemist details CSV to use caching
        if file_path == CHEMIST_DETAILS_CSV_PATH:
            global _chemist_details_df_cache, _chemist_details_mtime_cache
            current_mtime = os.path.getmtime(file_path)
            if _chemist_details_df_cache is not None and _chemist_details_mtime_cache == current_mtime:
                app.logger.info(f"Using cached {file_description} data.")
                return _chemist_details_df_cache.copy() # Return a copy to prevent modification of cache

        encodings_to_try = ['utf-8', 'latin1', 'cp1252'] 
        df = None
        for encoding in encodings_to_try:
            try:
                app.logger.info(f"Trying to read {file_description} with encoding: {encoding}")
                df = pd.read_csv(file_path, encoding=encoding)
                app.logger.info(f"Successfully loaded {file_description}: {os.path.basename(file_path)} with encoding: {encoding}. Shape: {df.shape if df is not None else 'None'}")
                
                if file_path == CHEMIST_DETAILS_CSV_PATH and df is not None:
                    _chemist_details_df_cache = df.copy() # Cache a copy
                    _chemist_details_mtime_cache = current_mtime
                return df 
            except UnicodeDecodeError:
                app.logger.warning(f"Failed to decode {os.path.basename(file_path)} with encoding: {encoding}")
            except pd.errors.EmptyDataError:
                app.logger.warning(f"{file_description} file {file_path} is empty (encoding: {encoding}).")
                flash(f"Warning: The {file_description} data file '{os.path.basename(file_path)}' appears to be empty.", "warning")
                return pd.DataFrame() 
            except Exception as e:
                app.logger.error(f"Error reading {file_description} file {file_path} with encoding {encoding}: {e}")

        app.logger.error(f"Failed to decode {os.path.basename(file_path)} with all attempted encodings: {encodings_to_try}")
        flash(f"Error: Could not decode the {file_description} file with common encodings. Please check the file format.", "danger")
        return pd.DataFrame()
    except Exception as e: # Catch issues like os.path.getmtime failing if file disappears
        app.logger.error(f"General error loading {file_description} file {file_path}: {e}")
        flash(f"Error: Could not load the {file_description} file. {e}", "danger")
        return pd.DataFrame()


def get_chemist_display_details(chemist_details_df):
    """Prepares chemist details for dropdown/search from a pre-loaded DataFrame."""
    if chemist_details_df.empty:
        app.logger.warning("Chemist details DataFrame is empty for display prep.")
        return []

    required_details_cols = ['Chemist ID', 'Name'] 
    if not all(col in chemist_details_df.columns for col in required_details_cols):
        missing_cols = [col for col in required_details_cols if col not in chemist_details_df.columns]
        app.logger.error(f"Chemist details data is missing critical columns: {', '.join(missing_cols)}")
        # No flash here, as this is an internal function. Caller should handle.
        return [] 

    dropdown_list = []
    for index, row in chemist_details_df.iterrows():
        try:
            chemist_id_raw = row.get('Chemist ID')
            if pd.isna(chemist_id_raw): continue
            chemist_id_numeric = pd.to_numeric(chemist_id_raw, errors='coerce')
            if pd.isna(chemist_id_numeric): continue
            chemist_id = int(chemist_id_numeric)

            name_raw = row.get('Name', '') 
            name_str = str(name_raw).strip()
            name_display = f"Chemist {chemist_id} (Name N/A)" if pd.isna(name_raw) or not name_str or name_str.lower() == 'nan' else name_str
            
            parts_for_display = [str(chemist_id), name_display]

            address_raw = row.get('Full Address')
            if pd.notna(address_raw):
                address_str = str(address_raw).strip()
                if address_str and address_str.lower() != 'nan': parts_for_display.append(address_str)
            
            postcode_raw = row.get('Postcode')
            if pd.notna(postcode_raw):
                postcode_str = str(postcode_raw).strip()
                if postcode_str and postcode_str.lower() != 'nan': parts_for_display.append(postcode_str)
            
            display_text = " - ".join(parts_for_display)
            dropdown_list.append({'id': chemist_id, 'display_text': display_text})
        except Exception as e: 
            app.logger.error(f"Error processing row for display (ID: {row.get('Chemist ID', 'Unknown')}): {e}")
            
    return sorted(dropdown_list, key=lambda x: x['id'])


def process_one_chemist_data(df_full, chemist_id):
    # (This function remains the same as your last working version)
    df_full_copy = df_full.copy()
    df_full_copy['Chemist'] = pd.to_numeric(df_full_copy['Chemist'], errors='coerce')
    df_chemist = df_full_copy[df_full_copy['Chemist'] == float(chemist_id)].copy()
    if df_chemist.empty: return pd.DataFrame()
    df_chemist.loc[:, 'Year'] = pd.to_numeric(df_chemist['Year'], errors='coerce').fillna(0).astype(int)
    df_chemist.loc[:, 'Month'] = pd.to_numeric(df_chemist['Month'], errors='coerce').fillna(0).astype(int)
    df_chemist.loc[:, 'Number of Items'] = pd.to_numeric(df_chemist['Number of Items'], errors='coerce').fillna(0)
    df_chemist = df_chemist[(df_chemist['Year'] > 0) & (df_chemist['Month'] >= 1) & (df_chemist['Month'] <= 12)].copy()
    if df_chemist.empty: return pd.DataFrame()
    aggregated_df = df_chemist.groupby(['Year', 'Month'])['Number of Items'].sum().reset_index()
    aggregated_df = aggregated_df.rename(columns={'Number of Items': 'Total Items'})
    aggregated_df = aggregated_df.sort_values(by=['Year', 'Month']).reset_index(drop=True)
    aggregated_df['Rolling 12m Avg Items'] = aggregated_df['Total Items'].rolling(window=12, min_periods=1).mean().round(0).astype(int)
    aggregated_df['Chemist'] = chemist_id 
    return aggregated_df[['Chemist', 'Year', 'Month', 'Total Items', 'Rolling 12m Avg Items']]

def process_and_aggregate_data(df_input, chemist_id1, chemist_id2=None):
    # (This function remains the same as your last working version)
    if df_input.empty: return pd.DataFrame(), "single" 
    required_cols = ['Chemist', 'Year', 'Month', 'Number of Items'] 
    if not all(col in df_input.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_input.columns]
        flash(f"Error: Merged dispensing data is missing required columns: {', '.join(missing)}.", "danger")
        return pd.DataFrame(), "single"
    df_clean = df_input.copy()
    df_c1 = process_one_chemist_data(df_clean, chemist_id1)
    if df_c1.empty:
        flash(f"No dispensing data found for Chemist {chemist_id1}.", "info")
        return pd.DataFrame(), "single" 
    if chemist_id2 is None or chemist_id1 == chemist_id2: return df_c1, "single"
    df_c2 = process_one_chemist_data(df_clean, chemist_id2)
    if df_c2.empty:
        flash(f"No dispensing data for Chemist {chemist_id2}. Displaying Chemist {chemist_id1} only.", "info")
        return df_c1, "single" 
    c1_id_int = int(float(chemist_id1)); c2_id_int = int(float(chemist_id2))
    c1_items_col = f'Items C{c1_id_int}'; c1_roll_col = f'Rolling Avg C{c1_id_int}'
    c2_items_col = f'Items C{c2_id_int}'; c2_roll_col = f'Rolling Avg C{c2_id_int}'
    sep_col = ' ' 
    df_c1_r = df_c1.rename(columns={'Total Items': c1_items_col, 'Rolling 12m Avg Items': c1_roll_col}).drop(columns=['Chemist'])
    df_c2_r = df_c2.rename(columns={'Total Items': c2_items_col, 'Rolling 12m Avg Items': c2_roll_col}).drop(columns=['Chemist'])
    comp_df = pd.merge(df_c1_r, df_c2_r, on=['Year', 'Month'], how='outer')
    comp_df[sep_col] = '' 
    disp_cols = ['Year', 'Month', c1_items_col, c1_roll_col, sep_col, c2_items_col, c2_roll_col]
    for col in disp_cols:
        if col not in comp_df.columns: comp_df[col] = pd.NA 
    comp_df = comp_df[disp_cols].sort_values(by=['Year', 'Month']).reset_index(drop=True)
    return comp_df, "comparison"

# --- Flask Routes ---
@app.route('/', methods=['GET'])
def index():
    """Serves the main page with the form. Dropdown data is now fetched by JS."""
    # No need to pass chemist_options here anymore, JavaScript will fetch them.
    return render_template('index.html')

@app.route('/search_chemists', methods=['GET'])
def search_chemists():
    """Endpoint for JavaScript to fetch filtered chemist details."""
    query = request.args.get('query', '').lower()
    
    # Load chemist details (uses cache if available)
    df_all_chemists = load_csv_data(CHEMIST_DETAILS_CSV_PATH, file_description="Chemist Details for Search")
    if df_all_chemists.empty:
        return jsonify([]) # Return empty list if data can't be loaded

    # Ensure required columns for searching are present
    search_cols = ['Chemist ID', 'Name', 'Full Address', 'Postcode']
    available_search_cols = [col for col in search_cols if col in df_all_chemists.columns]
    
    if not available_search_cols:
        app.logger.error("No searchable columns found in chemist details file for search endpoint.")
        return jsonify([])

    # Filter based on query
    # Convert all searchable columns to string type for .str.contains
    # and fill NA with empty string to avoid errors with .str accessor on NA values
    filtered_df = df_all_chemists[
        df_all_chemists[available_search_cols].fillna('').astype(str).apply(
            lambda row: row.str.lower().str.contains(query, na=False) # na=False for contains
        ).any(axis=1)
    ]
    
    # Prepare data for JSON response (id, display_text)
    results = get_chemist_display_details(filtered_df) # Re-use formatting logic
    
    # Limit number of results to send to frontend (e.g., top 10-20)
    return jsonify(results[:20])


@app.route('/process', methods=['POST'])
def process_data_route():
    """Handles form submission, processes data, and then shows it or allows download."""
    try:
        # These will now come from hidden input fields populated by JavaScript
        str_chemist_input_1 = request.form.get('chemist_number_1') 
        str_chemist_input_2 = request.form.get('chemist_number_2') 

        if not str_chemist_input_1: # Check if the hidden field was populated
            flash("Please select Chemist 1 using the search.", "warning")
            return redirect(url_for('index'))

        chemist_filter_number_1 = int(str_chemist_input_1)
        chemist_filter_number_2 = None
        if str_chemist_input_2 and str_chemist_input_2.isdigit(): 
            chemist_filter_number_2 = int(str_chemist_input_2)
            if chemist_filter_number_1 == chemist_filter_number_2:
                flash("Please select two different chemists for comparison, or leave Chemist 2 blank for a single view.", "info")
                chemist_filter_number_2 = None 
        elif str_chemist_input_2: # Handles empty string from optional second chemist
             chemist_filter_number_2 = None

    except ValueError:
        flash("Invalid Chemist ID submitted. Please use the search to select.", "danger")
        return redirect(url_for('index'))
    except Exception as e:
        app.logger.error(f"Error processing form input: {e}")
        flash(f"Error processing your request: {e}", "danger")
        return redirect(url_for('index'))

    dispensing_data = load_csv_data(INPUT_DATA_CSV_FILE_PATH, file_description="Dispensing Data") 
    if dispensing_data.empty:
        return redirect(url_for('index'))

    processed_df, view_type = process_and_aggregate_data(dispensing_data, chemist_filter_number_1, chemist_filter_number_2)

    if not processed_df.empty:
        c1_id_for_filename = int(float(chemist_filter_number_1))
        output_file_ref = "Analysis" 

        if view_type == "comparison" and chemist_filter_number_2 is not None:
            c2_id_for_filename = int(float(chemist_filter_number_2))
            output_filename = f'Compare_C{c1_id_for_filename}_vs_C{c2_id_for_filename}_{output_file_ref}.xlsx'
            title_chemist_id_display = f"{c1_id_for_filename} vs {c2_id_for_filename}"
        else: 
            output_filename = f'Chemist{c1_id_for_filename}_{output_file_ref}.xlsx'
            title_chemist_id_display = str(c1_id_for_filename)

        df_for_html = processed_df.copy().fillna('') 
        data_html_table = df_for_html.to_html(classes='data-table table table-striped table-hover', 
                                               index=False, border=0, escape=False) 
        
        session['processed_data'] = processed_df.to_dict('records') 
        session['output_filename'] = output_filename
        session['view_type'] = view_type 

        app.logger.info(f"Data processed. Rendering results page for: {title_chemist_id_display}")
        return render_template('results.html', 
                               table_data=data_html_table, 
                               chemist_id_display=title_chemist_id_display, 
                               download_filename=output_filename,
                               view_type=view_type)
    else:
        if not flash_is_pending(): 
             flash("No data to display or generate Excel file after processing.", "info")
        return redirect(url_for('index'))

@app.route('/download_excel')
def download_excel():
    # (This function remains the same as your last working version)
    if 'processed_data' in session and 'output_filename' in session:
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
    app.logger.info("Flask application starting...")
    app.run(debug=True, host='0.0.0.0', port=5001)
