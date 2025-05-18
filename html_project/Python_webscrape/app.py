import os
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session
import io # For sending file in memory
import logging
import time 

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = 'your_very_secret_key_for_session_and_flash_bso_analyzer_v3' # Unique secret key

# --- Configuration ---
# Path to the main data CSV file (output of your scraper script)
INPUT_DATA_CSV_FILE_PATH = r"D:\Data2\merged_bso_dispensing_data.csv" # Ensure this path is correct

# Path to the chemist details CSV file (output of your chemist_list_processor script)
CHEMIST_DETAILS_CSV_PATH = r"E:\Project\2025_PROJECT\processed_chemist_data\formatted_chemist_list.csv" # EXAMPLE PATH - PLEASE UPDATE

# --- Helper Functions ---
def load_csv_data(file_path, file_description="CSV"):
    """Reads a single CSV file into a pandas DataFrame, trying common encodings."""
    app.logger.info(f"Attempting to load data from {file_description} file: {file_path}")
    if not os.path.exists(file_path):
        app.logger.error(f"Input {file_description} file not found at {file_path}")
        flash(f"Error: Input {file_description} file not found at {file_path}. Please ensure the file exists.", "danger")
        return pd.DataFrame()

    encodings_to_try = ['utf-8', 'latin1', 'cp1252'] 
    df = None

    for encoding in encodings_to_try:
        try:
            app.logger.info(f"Trying to read {file_description} with encoding: {encoding}")
            df = pd.read_csv(file_path, encoding=encoding)
            app.logger.info(f"Successfully loaded {file_description}: {os.path.basename(file_path)} with encoding: {encoding}. Shape: {df.shape if df is not None else 'None'}")
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

def get_chemist_details_for_dropdown(chemist_details_file_path):
    """Loads chemist details and prepares them for the dropdown."""
    df_chemists = load_csv_data(chemist_details_file_path, file_description="Chemist Details")
    
    if df_chemists.empty:
        app.logger.warning("Chemist details DataFrame is empty. Dropdown will be empty or show an error.")
        return []

    # Expected columns in formatted_chemist_list.csv: 'Chemist ID', 'Name', 'Full Address', 'Postcode'
    required_details_cols = ['Chemist ID', 'Name'] # 'Full Address' and 'Postcode' are desirable but optional for display
    
    if not all(col in df_chemists.columns for col in required_details_cols):
        missing_cols = [col for col in required_details_cols if col not in df_chemists.columns]
        app.logger.error(f"Chemist details file is missing critical columns: {', '.join(missing_cols)}")
        flash(f"Error: Chemist details file is missing critical columns: {', '.join(missing_cols)}. Dropdown may not populate correctly.", "danger")
        return [] # If critical ID or Name is missing, don't proceed

    dropdown_list = []
    for index, row in df_chemists.iterrows():
        try:
            chemist_id = int(pd.to_numeric(row['Chemist ID'], errors='coerce'))
            name = str(row.get('Name', 'N/A')).strip()
            # Get Full Address and Postcode, defaulting to 'N/A' if missing or NaN
            full_address = str(row.get('Full Address', 'N/A')).strip()
            if pd.isna(row.get('Full Address')) or not full_address: # Handle NaN or empty string for address
                full_address = "Address N/A"

            postcode = str(row.get('Postcode', 'N/A')).strip()
            if pd.isna(row.get('Postcode')) or not postcode: # Handle NaN or empty string for postcode
                postcode = "Postcode N/A"

            # Construct the display text
            display_text_parts = [str(chemist_id), name]
            if full_address and full_address != "Address N/A":
                display_text_parts.append(full_address)
            if postcode and postcode != "Postcode N/A":
                display_text_parts.append(postcode)
            
            display_text = " - ".join(filter(None, display_text_parts)) # Filter out any None or empty parts before joining

            dropdown_list.append({'id': chemist_id, 'display_text': display_text})
        except ValueError: # For pd.to_numeric or int() conversion
            app.logger.warning(f"Skipping chemist due to invalid Chemist ID format: {row.get('Chemist ID')}")
        except Exception as e:
            app.logger.error(f"Error processing row for dropdown: {row}, Error: {e}")
            
    dropdown_list = sorted(dropdown_list, key=lambda x: x['id'])
    return dropdown_list


def process_one_chemist_data(df_full, chemist_id):
    df_full_copy = df_full.copy()
    df_full_copy['Chemist'] = pd.to_numeric(df_full_copy['Chemist'], errors='coerce')
    df_chemist = df_full_copy[df_full_copy['Chemist'] == float(chemist_id)].copy()

    if df_chemist.empty:
        app.logger.info(f"No data found for Chemist ID: {chemist_id} after initial filtering.")
        return pd.DataFrame()

    df_chemist.loc[:, 'Year'] = pd.to_numeric(df_chemist['Year'], errors='coerce').fillna(0).astype(int)
    df_chemist.loc[:, 'Month'] = pd.to_numeric(df_chemist['Month'], errors='coerce').fillna(0).astype(int)
    df_chemist.loc[:, 'Number of Items'] = pd.to_numeric(df_chemist['Number of Items'], errors='coerce').fillna(0)
    
    df_chemist = df_chemist[(df_chemist['Year'] > 0) & (df_chemist['Month'] >= 1) & (df_chemist['Month'] <= 12)].copy()

    if df_chemist.empty:
        app.logger.info(f"No valid Year/Month data for Chemist ID: {chemist_id} after type conversion.")
        return pd.DataFrame()

    app.logger.info(f"Aggregating data for Chemist ID: {chemist_id}")
    aggregated_df = df_chemist.groupby(['Year', 'Month'])['Number of Items'].sum().reset_index()
    aggregated_df = aggregated_df.rename(columns={'Number of Items': 'Total Items'})
    
    aggregated_df = aggregated_df.sort_values(by=['Year', 'Month']).reset_index(drop=True)
    aggregated_df['Rolling 12m Avg Items'] = aggregated_df['Total Items'].rolling(window=12, min_periods=1).mean().round(0).astype(int)
    
    aggregated_df['Chemist'] = chemist_id 
    
    return aggregated_df[['Chemist', 'Year', 'Month', 'Total Items', 'Rolling 12m Avg Items']]


def process_and_aggregate_data(df_input, chemist_id1, chemist_id2=None):
    if df_input.empty:
        app.logger.warning("Input DataFrame is empty for processing.")
        return pd.DataFrame(), "single" 

    required_cols = ['Chemist', 'Year', 'Month', 'Number of Items'] 
    if not all(col in df_input.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_input.columns]
        app.logger.error(f"Input DataFrame is missing required columns: {', '.join(missing)}")
        flash(f"Error: Merged dispensing data is missing required columns: {', '.join(missing)}. Please ensure 'Chemist', 'Year', 'Month', 'Number of Items' are present.", "danger")
        return pd.DataFrame(), "single"

    df_clean = df_input.copy()

    df_c1 = process_one_chemist_data(df_clean, chemist_id1)
    if df_c1.empty:
        flash(f"No dispensing data found for Chemist {chemist_id1}.", "info")
        return pd.DataFrame(), "single" 

    if chemist_id2 is None or chemist_id1 == chemist_id2:
        app.logger.info(f"Processing complete for single Chemist ID: {chemist_id1}")
        return df_c1, "single"
    else:
        app.logger.info(f"Processing data for second Chemist ID: {chemist_id2} for comparison.")
        df_c2 = process_one_chemist_data(df_clean, chemist_id2)

        if df_c2.empty:
            flash(f"No dispensing data found for the second Chemist {chemist_id2}. Displaying data for Chemist {chemist_id1} only.", "info")
            return df_c1, "single" 

        c1_id_int = int(float(chemist_id1))
        c2_id_int = int(float(chemist_id2))

        c1_items_col_name = f'Items C{c1_id_int}'
        c1_rolling_col_name = f'Rolling Avg C{c1_id_int}'
        c2_items_col_name = f'Items C{c2_id_int}'
        c2_rolling_col_name = f'Rolling Avg C{c2_id_int}'
        separator_col_name = ' ' 

        df_c1_renamed = df_c1.rename(columns={
            'Total Items': c1_items_col_name,
            'Rolling 12m Avg Items': c1_rolling_col_name
        }).drop(columns=['Chemist'])

        df_c2_renamed = df_c2.rename(columns={
            'Total Items': c2_items_col_name,
            'Rolling 12m Avg Items': c2_rolling_col_name
        }).drop(columns=['Chemist'])

        comparison_df = pd.merge(df_c1_renamed, df_c2_renamed, on=['Year', 'Month'], how='outer')
        
        comparison_df[separator_col_name] = '' 

        display_columns = ['Year', 'Month', 
                           c1_items_col_name, c1_rolling_col_name, 
                           separator_col_name, 
                           c2_items_col_name, c2_rolling_col_name]
        
        for col in display_columns:
            if col not in comparison_df.columns:
                comparison_df[col] = pd.NA 

        comparison_df = comparison_df[display_columns] 
        comparison_df = comparison_df.sort_values(by=['Year', 'Month']).reset_index(drop=True)
        
        app.logger.info(f"Comparison data generated for Chemist {c1_id_int} and Chemist {c2_id_int}.")
        return comparison_df, "comparison"

# --- Flask Routes ---
@app.route('/', methods=['GET'])
def index():
    """Serves the main page with the form."""
    chemist_dropdown_data = get_chemist_details_for_dropdown(CHEMIST_DETAILS_CSV_PATH)
    
    if not chemist_dropdown_data:
        if not flash_is_pending():
            flash("Could not populate chemist selection. Please check the chemist details file (formatted_chemist_list.csv).", "warning")
            
    return render_template('index.html', chemist_options=chemist_dropdown_data)

@app.route('/process', methods=['POST'])
def process_data_route():
    """Handles form submission, processes data, and then shows it or allows download."""
    try:
        y_input = request.form.get('y_input', 'ALL_Data') 
        str_chemist_input_1 = request.form.get('chemist_number_1')
        str_chemist_input_2 = request.form.get('chemist_number_2') 

        if not str_chemist_input_1:
            flash("Please select Chemist 1.", "warning")
            return redirect(url_for('index'))

        chemist_filter_number_1 = int(str_chemist_input_1)
        chemist_filter_number_2 = None
        if str_chemist_input_2 and str_chemist_input_2.isdigit(): 
            chemist_filter_number_2 = int(str_chemist_input_2)
            if chemist_filter_number_1 == chemist_filter_number_2:
                flash("Please select two different chemists for comparison, or leave Chemist 2 blank for a single view.", "info")
                chemist_filter_number_2 = None 
        elif str_chemist_input_2: 
             chemist_filter_number_2 = None

    except ValueError:
        flash("Invalid Chemist number selected. Ensure it's a whole number.", "danger")
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
        
        if view_type == "comparison" and chemist_filter_number_2 is not None:
            c2_id_for_filename = int(float(chemist_filter_number_2))
            output_filename = f'Compare_C{c1_id_for_filename}_vs_C{c2_id_for_filename}_{y_input.replace(" ", "_")}.xlsx'
            title_chemist_id_display = f"{c1_id_for_filename} vs {c2_id_for_filename}"
        else: 
            output_filename = f'Chemist{c1_id_for_filename}_FilteredData_{y_input.replace(" ", "_")}.xlsx'
            title_chemist_id_display = str(c1_id_for_filename)

        data_html_table = processed_df.to_html(classes='data-table table table-striped table-hover', index=False, border=0, escape=False)
        
        session['processed_data'] = processed_df.to_dict('records') 
        session['output_filename'] = output_filename
        session['view_type'] = view_type 

        app.logger.info(f"Data processed. Rendering results page for: {title_chemist_id_display}")
        return render_template('results.html', 
                               table_data=data_html_table, 
                               chemist_id_display=title_chemist_id_display, 
                               filename_ref=y_input,
                               download_filename=output_filename,
                               view_type=view_type)
    else:
        if not flash_is_pending(): 
             flash("No data to display or generate Excel file after processing.", "info")
        return redirect(url_for('index'))

@app.route('/download_excel')
def download_excel():
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

            app.logger.info(f"Sending file for download: {output_filename}")
            return send_file(
                excel_buffer,
                as_attachment=True,
                download_name=output_filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
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
