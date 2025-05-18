import os
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session
import io # For sending file in memory
import logging
import time # For potential timing, though not strictly used in this version

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = 'your_very_secret_key_for_session_and_flash_bso_analyzer' # Unique secret key

# --- Configuration ---
# Path to the single merged CSV file (output of your scraper script)
INPUT_CSV_FILE_PATH = r"D:\Data2\merged_bso_dispensing_data.csv" # Ensure this path is correct

# --- Helper Functions ---
def load_single_csv_file(file_path):
    """Reads a single CSV file into a pandas DataFrame, trying common encodings."""
    app.logger.info(f"Attempting to load data from CSV file: {file_path}")
    if not os.path.exists(file_path):
        app.logger.error(f"Input CSV file not found at {file_path}")
        flash(f"Error: Input CSV file not found at {file_path}. Please run the scraper script first.", "danger")
        return pd.DataFrame()

    encodings_to_try = ['utf-8', 'latin1', 'cp1252'] # Common encodings
    df = None

    for encoding in encodings_to_try:
        try:
            app.logger.info(f"Trying to read CSV with encoding: {encoding}")
            df = pd.read_csv(file_path, encoding=encoding)
            app.logger.info(f"Successfully loaded CSV: {os.path.basename(file_path)} with encoding: {encoding}. Shape: {df.shape if df is not None else 'None'}")
            return df # Return as soon as a successful encoding is found
        except UnicodeDecodeError:
            app.logger.warning(f"Failed to decode {os.path.basename(file_path)} with encoding: {encoding}")
        except pd.errors.EmptyDataError:
            app.logger.warning(f"CSV file {file_path} is empty (encoding: {encoding}).")
            flash(f"Warning: The CSV data file '{os.path.basename(file_path)}' appears to be empty.", "warning")
            return pd.DataFrame() # Return empty if file is empty
        except Exception as e:
            app.logger.error(f"Error reading CSV file {file_path} with encoding {encoding}: {e}")
            # Don't flash for every encoding attempt, flash once if all fail
            # flash(f"Error reading CSV file (encoding {encoding}): {e}", "danger")
            # return pd.DataFrame() # Don't return yet, try other encodings

    # If all encodings failed
    app.logger.error(f"Failed to decode {os.path.basename(file_path)} with all attempted encodings: {encodings_to_try}")
    flash(f"Error: Could not decode the CSV file with common encodings. Please check the file format.", "danger")
    return pd.DataFrame()


def get_unique_chemist_ids(df):
    """Extracts unique, sorted chemist IDs from the DataFrame."""
    if df.empty or 'Chemist' not in df.columns:
        app.logger.warning("DataFrame is empty or 'Chemist' column is missing for unique ID extraction.")
        return []
    try:
        # Convert to numeric, drop NaNs, convert to int, get unique, sort
        unique_ids = pd.to_numeric(df['Chemist'], errors='coerce').dropna().unique()
        # Ensure they are integers and sorted
        return sorted([int(uid) for uid in unique_ids])
    except Exception as e:
        app.logger.error(f"Error extracting unique chemist IDs: {e}")
        flash(f"Could not extract chemist IDs from the data: {e}", "warning")
        return []

def process_one_chemist_data(df_full, chemist_id):
    """Processes data for a single chemist, including rolling average."""
    # Work on a copy to avoid modifying the original df passed around
    df_full_copy = df_full.copy()
    # Ensure 'Chemist' column is numeric for filtering
    df_full_copy['Chemist'] = pd.to_numeric(df_full_copy['Chemist'], errors='coerce')
    
    # Filter by the specific chemist ID (ensure chemist_id is also numeric)
    df_chemist = df_full_copy[df_full_copy['Chemist'] == float(chemist_id)].copy()

    if df_chemist.empty:
        app.logger.info(f"No data found for Chemist ID: {chemist_id} after initial filtering.")
        return pd.DataFrame()

    # Ensure 'Year', 'Month', and 'Number of Items' are in appropriate formats
    df_chemist.loc[:, 'Year'] = pd.to_numeric(df_chemist['Year'], errors='coerce').fillna(0).astype(int)
    df_chemist.loc[:, 'Month'] = pd.to_numeric(df_chemist['Month'], errors='coerce').fillna(0).astype(int)
    df_chemist.loc[:, 'Number of Items'] = pd.to_numeric(df_chemist['Number of Items'], errors='coerce').fillna(0)
    
    # Filter out rows with invalid Year or Month
    df_chemist = df_chemist[(df_chemist['Year'] > 0) & (df_chemist['Month'] >= 1) & (df_chemist['Month'] <= 12)].copy()

    if df_chemist.empty:
        app.logger.info(f"No valid Year/Month data for Chemist ID: {chemist_id} after type conversion.")
        return pd.DataFrame()

    app.logger.info(f"Aggregating data for Chemist ID: {chemist_id}")
    aggregated_df = df_chemist.groupby(['Year', 'Month'])['Number of Items'].sum().reset_index()
    aggregated_df = aggregated_df.rename(columns={'Number of Items': 'Total Items'})
    
    # Sort by Year and Month BEFORE calculating rolling average
    aggregated_df = aggregated_df.sort_values(by=['Year', 'Month']).reset_index(drop=True)

    # Calculate rolling 12-month average for 'Total Items' and round to 0 decimal places, then convert to int
    aggregated_df['Rolling 12m Avg Items'] = aggregated_df['Total Items'].rolling(window=12, min_periods=1).mean().round(0).astype(int) # MODIFIED HERE
    
    aggregated_df['Chemist'] = chemist_id # Use the original chemist_id for this column
    
    return aggregated_df[['Chemist', 'Year', 'Month', 'Total Items', 'Rolling 12m Avg Items']]


def process_and_aggregate_data(df_input, chemist_id1, chemist_id2=None):
    """
    Prepares data for one or two chemists.
    If two chemist_ids are provided, their data is merged for comparison.
    """
    if df_input.empty:
        app.logger.warning("Input DataFrame is empty for processing.")
        return pd.DataFrame(), "single" # Return type indicator

    required_cols = ['Chemist', 'Year', 'Month', 'Number of Items'] # These are from COLUMNS_TO_KEEP in scraper
    if not all(col in df_input.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_input.columns]
        app.logger.error(f"Input DataFrame is missing required columns: {', '.join(missing)}")
        flash(f"Error: Merged data is missing required columns: {', '.join(missing)}. Please ensure 'Chemist', 'Year', 'Month', 'Number of Items' are present.", "danger")
        return pd.DataFrame(), "single"

    df_clean = df_input.copy() # Work with a copy of the input

    # Process data for the first chemist
    df_c1 = process_one_chemist_data(df_clean, chemist_id1)
    if df_c1.empty:
        flash(f"No data found for Chemist {chemist_id1}.", "info")
        return pd.DataFrame(), "single" 

    if chemist_id2 is None or chemist_id1 == chemist_id2:
        app.logger.info(f"Processing complete for single Chemist ID: {chemist_id1}")
        return df_c1, "single"
    else:
        # Process data for the second chemist
        app.logger.info(f"Processing data for second Chemist ID: {chemist_id2} for comparison.")
        df_c2 = process_one_chemist_data(df_clean, chemist_id2)

        if df_c2.empty:
            flash(f"No data found for the second Chemist {chemist_id2}. Displaying data for Chemist {chemist_id1} only.", "info")
            return df_c1, "single" # Fallback to single view if C2 has no data

        c1_id_int = int(float(chemist_id1))
        c2_id_int = int(float(chemist_id2))

        df_c1_renamed = df_c1.rename(columns={
            'Total Items': f'Total Items C{c1_id_int}',
            'Rolling 12m Avg Items': f'Rolling Avg C{c1_id_int}'
        }).drop(columns=['Chemist'])

        df_c2_renamed = df_c2.rename(columns={
            'Total Items': f'Total Items C{c2_id_int}',
            'Rolling 12m Avg Items': f'Rolling Avg C{c2_id_int}'
        }).drop(columns=['Chemist'])

        comparison_df = pd.merge(df_c1_renamed, df_c2_renamed, on=['Year', 'Month'], how='outer')
        comparison_df = comparison_df.sort_values(by=['Year', 'Month']).reset_index(drop=True)
        
        comparison_df['Chemist1_ID'] = c1_id_int
        comparison_df['Chemist2_ID'] = c2_id_int

        app.logger.info(f"Comparison data generated for Chemist {c1_id_int} and Chemist {c2_id_int}.")
        return comparison_df, "comparison"

# --- Flask Routes ---
@app.route('/', methods=['GET'])
def index():
    """Serves the main page with the form."""
    consolidated_data = load_single_csv_file(INPUT_CSV_FILE_PATH) 
    if consolidated_data.empty and not os.path.exists(INPUT_CSV_FILE_PATH):
        return render_template('index.html', unique_chemists=[])
    elif consolidated_data.empty and os.path.exists(INPUT_CSV_FILE_PATH):
        if not flash_is_pending(): 
             flash("Could not load data to populate chemist dropdown. The CSV file might be empty or improperly formatted.", "warning")
        return render_template('index.html', unique_chemists=[])

    unique_chemists = get_unique_chemist_ids(consolidated_data)
    if not unique_chemists and not consolidated_data.empty: 
        flash("No chemist IDs could be extracted. The 'Chemist' column might be missing or empty in the CSV file.", "warning")
    return render_template('index.html', unique_chemists=unique_chemists)

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

    consolidated_data = load_single_csv_file(INPUT_CSV_FILE_PATH) 
    if consolidated_data.empty:
        return redirect(url_for('index'))

    processed_df, view_type = process_and_aggregate_data(consolidated_data, chemist_filter_number_1, chemist_filter_number_2)

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
    """Serves the processed data as an Excel file for download."""
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
    """Checks if there are any messages waiting to be flashed."""
    return bool(request.environ.get('werkzeug.request') and request.environ['werkzeug.request'].session.get('_flashes'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app.logger.info("Flask application starting...")
    app.run(debug=True, host='0.0.0.0', port=5001)
