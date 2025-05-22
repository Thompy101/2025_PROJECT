import os
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session, jsonify
import io # For sending file in memory
import logging
import time # Not explicitly used but good for potential future timing/debugging
from google.cloud import storage # Import Google Cloud Storage
import calendar # For month name/number conversions

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = 'your_very_secret_key_for_session_and_flash_bso_analyzer_v7_search' # CHANGE THIS IN PRODUCTION!

# --- Configuration ---
GCS_BUCKET_NAME = "project-2025-bso-data-storage" # << REPLACE WITH YOUR GCS BUCKET NAME
GCS_DATA_FILENAME = "merged_bso_dispensing_data.csv"
CHEMIST_DETAILS_CSV_PATH = "formatted_chemist_list.csv"

# Month to number mapping for sorting (used for rolling average calculation)
MONTH_TO_NUM = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
    'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
}

# List of ordered month names for Categorical type
ORDERED_MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]

# Helper dictionary to map month numbers (int) to full month names
INT_TO_MONTH_NAME = {i: calendar.month_name[i] for i in range(1, 13)}

def standardize_month_value(month_val):
    """
    Standardizes a month value to its full name (e.g., "January").
    Handles integers, numeric strings (e.g., "1", "01"), and string month names (e.g., "january").
    """
    if isinstance(month_val, (int, float)): # handle float like 1.0 as well
        month_int = int(month_val)
        if 1 <= month_int <= 12:
            return INT_TO_MONTH_NAME[month_int]
        else:
            app.logger.debug(f"Invalid integer month value encountered: {month_val}")
            return str(month_val)

    if isinstance(month_val, str):
        month_str_stripped = month_val.strip()
        if month_str_stripped.isdigit():
            try:
                month_int = int(month_str_stripped)
                if 1 <= month_int <= 12:
                    return INT_TO_MONTH_NAME[month_int]
                else:
                    app.logger.debug(f"Invalid numeric string month value encountered: {month_str_stripped}")
                    return month_str_stripped
            except ValueError:
                app.logger.debug(f"ValueError converting supposedly digit string to int: {month_str_stripped}")
                return month_str_stripped.title()
        else:
            return month_str_stripped.title()
    
    if pd.isna(month_val):
        app.logger.debug("NaN month value encountered in standardize_month_value.")
        return None 
    
    app.logger.debug(f"Unhandled month value type: {type(month_val)}, value: {month_val}")
    return str(month_val)

# --- Helper Functions ---
_chemist_details_df_cache = None

def load_csv_data_from_gcs(bucket_name, file_name, file_description="CSV"):
    app.logger.info(f"Attempting to load data from GCS bucket '{bucket_name}', file '{file_name}' ({file_description})...")
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        csv_data = blob.download_as_bytes()
        app.logger.info(f"Successfully downloaded {file_name} from GCS.")
        df = pd.read_csv(io.BytesIO(csv_data))
        app.logger.info(f"Successfully loaded data from GCS {file_description} file.")
        return df
    except Exception as e:
        app.logger.error(f"Error loading {file_description} file from GCS: {e}", exc_info=True)
        flash(f"Error: Could not load {file_description} data from cloud. Please try again later.", "danger")
        return pd.DataFrame()

def load_chemist_details(file_path):
    global _chemist_details_df_cache
    if _chemist_details_df_cache is not None:
        app.logger.info("Chemist details data is cached.")
        return _chemist_details_df_cache
    else:
        app.logger.info(f"Loading chemist details from {file_path} (first load for this instance).")
        try:
            df = pd.read_csv(file_path)
            _chemist_details_df_cache = df
            app.logger.info(f"Chemist details loaded: {len(df)} entries.")
            return df
        except FileNotFoundError:
            app.logger.error(f"Chemist details file not found at {file_path}")
            flash(f"Error: Chemist details file not found at {file_path}. Ensure it's deployed with the application.", "danger")
            return pd.DataFrame()
        except Exception as e:
            app.logger.error(f"Error loading chemist details CSV: {e}", exc_info=True)
            flash(f"Error loading chemist details CSV: {e}", "danger")
            return pd.DataFrame()

# --- Route Definitions ---
@app.route('/')
def index():
    df_main_data = load_csv_data_from_gcs(GCS_BUCKET_NAME, GCS_DATA_FILENAME, "main BSO dispensing data (for index)")
    df_chemist_details = load_chemist_details(CHEMIST_DETAILS_CSV_PATH)

    if df_main_data.empty and df_chemist_details.empty:
        return render_template('index.html', years=[], months=[], chemist_ids_json=[])
    elif df_main_data.empty:
        flash("Warning: Main dispensing data could not be loaded. Filtering options might be incomplete.", "warning")
    elif df_chemist_details.empty:
        flash("Warning: Chemist details could not be loaded. Chemist search might not work.", "warning")

    years = []
    months = ORDERED_MONTH_NAMES 

    if 'Year' in df_main_data.columns:
        try:
            years = sorted(df_main_data['Year'].dropna().unique().astype(int).tolist(), reverse=True)
        except ValueError:
            app.logger.warning("Could not convert 'Year' column to int for dropdown. Using raw unique values.")
            years = sorted(df_main_data['Year'].dropna().unique().tolist(), reverse=True) # Fallback
            flash("Warning: 'Year' column contains non-numeric data. Year filtering might behave unexpectedly.", "warning")
    else:
        app.logger.warning("Missing 'Year' column in main data for index page.")
        if not df_main_data.empty:
             flash("Data is missing 'Year' column. Year filtering might be limited.", "warning")

    if 'Month' not in df_main_data.columns and not df_main_data.empty:
        app.logger.warning("Missing 'Month' column in main data for index page.")
        flash("Data is missing 'Month' column. Month filtering might be limited.", "warning")

    chemist_ids_json = []
    if not df_chemist_details.empty and 'Chemist ID' in df_chemist_details.columns and 'Name' in df_chemist_details.columns:
        chemist_ids_json = df_chemist_details[['Chemist ID', 'Name']].to_dict(orient='records')
    else:
        if not df_chemist_details.empty:
            flash("Chemist details are missing 'Chemist ID' or 'Name' columns. Chemist search may be impaired.", "warning")

    return render_template('index.html', years=years, months=months, chemist_ids_json=chemist_ids_json)

@app.route('/get_chemist_suggestions')
def get_chemist_suggestions():
    query = request.args.get('query', '').lower()
    df_chemist_details = load_chemist_details(CHEMIST_DETAILS_CSV_PATH)
    if df_chemist_details.empty:
        return jsonify([])

    required_cols = ['Name', 'Chemist ID', 'Full Address']
    for col in required_cols:
        if col not in df_chemist_details.columns:
            app.logger.error(f"'{col}' column missing in chemist details for suggestions.")
            return jsonify([]) 

    df_chemist_details['Full Address'] = df_chemist_details['Full Address'].astype(str)
    df_chemist_details['Name'] = df_chemist_details['Name'].astype(str)
    df_chemist_details['Chemist ID'] = df_chemist_details['Chemist ID'].astype(str)

    filtered_chemists = df_chemist_details[
        df_chemist_details['Name'].str.lower().str.contains(query, na=False) |
        df_chemist_details['Chemist ID'].str.contains(query, na=False) | 
        df_chemist_details['Full Address'].str.lower().str.contains(query, na=False)
    ].head(10)[['Chemist ID', 'Name', 'Full Address']].to_dict(orient='records')
    return jsonify(filtered_chemists)

@app.route('/filter_data', methods=['POST'])
def filter_data():
    try:
        df_main_data = load_csv_data_from_gcs(GCS_BUCKET_NAME, GCS_DATA_FILENAME, "main BSO dispensing data")
        df_chemist_details = load_chemist_details(CHEMIST_DETAILS_CSV_PATH)

        if df_main_data.empty or df_chemist_details.empty:
            return redirect(url_for('index'))

        if 'Month' in df_main_data.columns:
            app.logger.info("Standardizing 'Month' column in main data using enhanced logic...")
            df_main_data['Month'] = df_main_data['Month'].apply(standardize_month_value)
            if not df_main_data.empty and 'Month' in df_main_data.columns:
                app.logger.info(f"Example 'Month' values after standardization (first 5 unique if available): {df_main_data['Month'].dropna().unique()[:5].tolist()}")
        else:
            app.logger.warning("Main data is missing 'Month' column. Month-related processing might be affected.")
            flash("Warning: 'Month' column is missing in the main dataset. Month-based filtering and analysis will be impacted.", "warning")

        chemist_id_1 = request.form.get('selected_chemist_id_1')
        chemist_id_2 = request.form.get('selected_chemist_id_2')
        selected_year = request.form.get('year')
        selected_month = request.form.get('month')

        view_type = "single"
        if chemist_id_1 and chemist_id_2:
            view_type = "comparison"
        elif chemist_id_1:
            view_type = "single"
        else:
            flash("Please select at least one chemist ID to filter.", "warning")
            return redirect(url_for('index'))

        table_html = "" 
        chemist_id_display = "" 
        download_filename = ""
        
        if 'Chemist' in df_main_data.columns:
            df_main_data['Chemist'] = df_main_data['Chemist'].astype(str)
        else:
            flash("Critical Error: 'Chemist' ID column missing from main dispensing data.", "danger")
            return redirect(url_for('index'))

        if 'Chemist ID' in df_chemist_details.columns:
            df_chemist_details['Chemist ID'] = df_chemist_details['Chemist ID'].astype(str)
        else:
            flash("Critical Error: 'Chemist ID' column missing from chemist details data.", "danger")
            return redirect(url_for('index'))

        def process_chemist_data(df_data_proc, chemist_id_to_filter, df_details_proc):
            # This function remains largely the same, preparing individual chemist data
            expected_cols_final_display = ['Chemist ID', 'Name', 'Year', 'Month', 'Number of Items', 'Rolling 12-Month Average']
            if not chemist_id_to_filter: return pd.DataFrame(columns=expected_cols_final_display)
            filtered_df = df_data_proc[df_data_proc['Chemist'] == chemist_id_to_filter].copy()
            if selected_year and selected_year != 'all':
                if 'Year' in filtered_df.columns:
                    try: 
                        filtered_df['Year'] = filtered_df['Year'].astype(int)
                        filtered_df = filtered_df[filtered_df['Year'] == int(selected_year)].copy()
                    except ValueError: app.logger.warning(f"Year filter for chemist {chemist_id_to_filter}: Could not convert 'Year' to int.")
                else: app.logger.warning(f"Year filter applied, but 'Year' column missing for chemist {chemist_id_to_filter}")
            if selected_month and selected_month != 'all':
                if 'Month' in filtered_df.columns: filtered_df = filtered_df[filtered_df['Month'] == selected_month].copy()
                else: app.logger.warning(f"Month filter applied, but 'Month' column missing for chemist {chemist_id_to_filter}")
            if filtered_df.empty: return pd.DataFrame(columns=expected_cols_final_display)
            if not all(col in filtered_df.columns for col in ['Chemist', 'Year', 'Month', 'Number of Items']):
                app.logger.error(f"Missing required columns for aggregation for chemist {chemist_id_to_filter}.")
                return pd.DataFrame(columns=expected_cols_final_display)
            aggregated_df = filtered_df.groupby(['Chemist', 'Year', 'Month'])['Number of Items'].sum().reset_index()
            if 'Month' not in aggregated_df.columns: return pd.DataFrame(columns=expected_cols_final_display)
            app.logger.info(f"Chemist {chemist_id_to_filter}: Unique 'Month' values after aggregation: {aggregated_df['Month'].dropna().unique().tolist()}")
            aggregated_df['Month_Num'] = aggregated_df['Month'].map(MONTH_TO_NUM)
            if aggregated_df['Month_Num'].isnull().any():
                failed_months = aggregated_df[aggregated_df['Month_Num'].isnull()]['Month'].dropna().unique().tolist()
                app.logger.warning(f"Chemist {chemist_id_to_filter}: Month-to-number mapping FAILED for: {failed_months}")
            aggregated_df['Month'] = pd.Categorical(aggregated_df['Month'], categories=ORDERED_MONTH_NAMES, ordered=True)
            if aggregated_df['Month'].isnull().any(): app.logger.warning(f"Chemist {chemist_id_to_filter}: Categorical 'Month' conversion resulted in NaNs.")
            original_rows = len(aggregated_df)
            aggregated_df_sorted_chrono = aggregated_df.dropna(subset=['Month_Num']).sort_values(by=['Chemist', 'Year', 'Month_Num'], ascending=[True, True, True]).copy()
            if original_rows > len(aggregated_df_sorted_chrono): app.logger.warning(f"Chemist {chemist_id_to_filter}: Dropped {original_rows - len(aggregated_df_sorted_chrono)} rows due to unmappable months.")
            if aggregated_df_sorted_chrono.empty: return pd.DataFrame(columns=expected_cols_final_display)
            aggregated_df_sorted_chrono['Rolling 12-Month Average'] = aggregated_df_sorted_chrono.groupby('Chemist')['Number of Items'].transform(lambda x: x.rolling(window=12, min_periods=1).mean()).round(0).astype(int)
            merged_df = pd.merge(aggregated_df_sorted_chrono, df_details_proc[['Chemist ID', 'Name']], left_on='Chemist', right_on='Chemist ID', how='left')
            final_df_with_month_num = merged_df.drop(columns=['Chemist'])
            final_df_display = final_df_with_month_num.sort_values(by=['Year', 'Month', 'Chemist ID'], ascending=[False, False, True]).reset_index(drop=True)
            if 'Month_Num' in final_df_display.columns: final_df_display = final_df_display.drop(columns=['Month_Num'])
            # Do not reorder to expected_cols_final_display here, as we need specific columns for merge later
            return final_df_display


        if view_type == "single":
            chemist_info = df_chemist_details[df_chemist_details['Chemist ID'] == chemist_id_1]
            chemist_name = chemist_info['Name'].iloc[0] if not chemist_info.empty and 'Name' in chemist_info.columns else chemist_id_1
            chemist_address = chemist_info['Full Address'].iloc[0] if not chemist_info.empty and 'Full Address' in chemist_info.columns else "Address not found"
            
            chemist_id_display = f"{chemist_name} ({chemist_id_1})" 
            download_filename = f"chemist_{chemist_id_1}_filtered_data.xlsx"
            
            final_display_df = process_chemist_data(df_main_data, chemist_id_1, df_chemist_details)
            # Select only the original expected columns for single view display
            expected_single_cols = ['Chemist ID', 'Name', 'Year', 'Month', 'Number of Items', 'Rolling 12-Month Average']
            final_display_df_single_view = final_display_df.copy()
            for col in expected_single_cols:
                if col not in final_display_df_single_view.columns:
                     final_display_df_single_view[col] = pd.NA # Add if missing
            final_display_df_single_view = final_display_df_single_view[expected_single_cols]


            html_parts_single = ["<div class='chemist-section'>"]
            if not final_display_df_single_view.empty:
                 if chemist_address and chemist_address != "Address not found":
                    html_parts_single.append(f"<p class='chemist-address'><em>{chemist_address}</em></p>")
                 html_parts_single.append("<div class='table-responsive'>")
                 html_parts_single.append(final_display_df_single_view.to_html(classes='data-table', index=False, na_rep='N/A', border=0))
                 html_parts_single.append("</div>")
            else:
                flash(f"No data found for Chemist ID {chemist_id_1} with the selected filters.", "info")
                html_parts_single.append("<div class='table-responsive'><p style='text-align: center;'>No data to display for the selected filters.</p></div>")
            html_parts_single.append("</div>")
            table_html = "\n".join(html_parts_single)
            
            session['processed_data'] = final_display_df_single_view.to_dict(orient='records')
            session['output_filename'] = download_filename

        elif view_type == "comparison":
            chemist_info_1 = df_chemist_details[df_chemist_details['Chemist ID'] == chemist_id_1]
            chemist_name_1_title = chemist_info_1['Name'].iloc[0] if not chemist_info_1.empty else chemist_id_1
            
            chemist_info_2 = df_chemist_details[df_chemist_details['Chemist ID'] == chemist_id_2]
            chemist_name_2_title = chemist_info_2['Name'].iloc[0] if not chemist_info_2.empty else chemist_id_2

            chemist_id_display = f"{chemist_name_1_title} ({chemist_id_1}) vs {chemist_name_2_title} ({chemist_id_2})"
            download_filename = f"comparison_{chemist_id_1}_vs_{chemist_id_2}_merged_data.xlsx"

            df1_processed = process_chemist_data(df_main_data, chemist_id_1, df_chemist_details)
            df2_processed = process_chemist_data(df_main_data, chemist_id_2, df_chemist_details)

            # Rename columns for Chemist 1
            df1_renamed = df1_processed.rename(columns={
                'Chemist ID': 'Chemist ID_C1', 'Name': 'Name_C1',
                'Number of Items': 'Items_C1', 'Rolling 12-Month Average': 'RollingAvg_C1'
            })[['Year', 'Month', 'Chemist ID_C1', 'Name_C1', 'Items_C1', 'RollingAvg_C1']] if not df1_processed.empty else pd.DataFrame(columns=['Year', 'Month', 'Chemist ID_C1', 'Name_C1', 'Items_C1', 'RollingAvg_C1'])

            # Rename columns for Chemist 2
            df2_renamed = df2_processed.rename(columns={
                'Chemist ID': 'Chemist ID_C2', 'Name': 'Name_C2',
                'Number of Items': 'Items_C2', 'Rolling 12-Month Average': 'RollingAvg_C2'
            })[['Year', 'Month', 'Chemist ID_C2', 'Name_C2', 'Items_C2', 'RollingAvg_C2']] if not df2_processed.empty else pd.DataFrame(columns=['Year', 'Month', 'Chemist ID_C2', 'Name_C2', 'Items_C2', 'RollingAvg_C2'])

            merged_df = pd.DataFrame()
            if df1_renamed.empty and df2_renamed.empty:
                flash("No data found for either chemist with the selected filters.", "info")
                table_html = "<p style='text-align: center;'>No data available for comparison.</p>"
            else:
                # Ensure 'Month' is string for merging, as Categorical might cause issues if not identical
                if not df1_renamed.empty: df1_renamed['Month'] = df1_renamed['Month'].astype(str)
                if not df2_renamed.empty: df2_renamed['Month'] = df2_renamed['Month'].astype(str)

                if df1_renamed.empty:
                    merged_df = df2_renamed.copy()
                    for col in ['Chemist ID_C1', 'Name_C1', 'Items_C1', 'RollingAvg_C1']: merged_df[col] = pd.NA
                elif df2_renamed.empty:
                    merged_df = df1_renamed.copy()
                    for col in ['Chemist ID_C2', 'Name_C2', 'Items_C2', 'RollingAvg_C2']: merged_df[col] = pd.NA
                else:
                    merged_df = pd.merge(df1_renamed, df2_renamed, on=['Year', 'Month'], how='outer')
                
                if not merged_df.empty:
                    merged_df['Separator'] = '' # Blank column
                    
                    # Define the final column order
                    final_columns_ordered = [
                        'Year', 'Month',
                        'Chemist ID_C1', 'Name_C1', 'Items_C1', 'RollingAvg_C1',
                        'Separator',
                        'Chemist ID_C2', 'Name_C2', 'Items_C2', 'RollingAvg_C2'
                    ]
                    # Ensure all columns exist and are in order
                    for col in final_columns_ordered:
                        if col not in merged_df.columns:
                            merged_df[col] = pd.NA
                    merged_df = merged_df[final_columns_ordered]

                    # Sort the merged table
                    merged_df['Month_Num_Sort'] = merged_df['Month'].map(MONTH_TO_NUM)
                    merged_df = merged_df.sort_values(by=['Year', 'Month_Num_Sort'], ascending=[False, False]).drop(columns=['Month_Num_Sort'])
                    
                    table_html = "<div class='table-responsive'>"
                    table_html += merged_df.to_html(classes='data-table', index=False, na_rep='N/A', border=0, escape=False)
                    table_html += "</div>"
                else: # Should be caught by the initial empty check, but as a fallback
                    table_html = "<p style='text-align: center;'>No data available for comparison after merging.</p>"
            
            session['processed_data'] = merged_df.to_dict(orient='records') if not merged_df.empty else []
            session['output_filename'] = download_filename
            
        return render_template('results.html',
                               table_data=table_html, 
                               chemist_id_display=chemist_id_display, 
                               view_type=view_type, 
                               download_filename=download_filename)

    except Exception as e:
        app.logger.error(f"Error during data filtering: {e}", exc_info=True)
        flash(f"An unexpected error occurred during filtering: {str(e)}. Please check logs or try again.", "danger")
        return redirect(url_for('index'))

@app.route('/download_excel')
def download_excel():
    if 'processed_data' in session and session['processed_data'] is not None:
        try:
            data_records = session.pop('processed_data') 
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
            app.logger.error(f"Error during Excel download: {e}", exc_info=True)
            flash(f"Error generating Excel file for download: {str(e)}", "danger")
            return redirect(url_for('index'))
    else:
        flash("No processed data available for download. Please filter first.", "warning")
        return redirect(url_for('index'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')
    logging.getLogger('google.cloud.storage').setLevel(logging.WARNING)
    
    port = int(os.environ.get('PORT', 8080))
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(debug=debug_mode, host='0.0.0.0', port=port)
