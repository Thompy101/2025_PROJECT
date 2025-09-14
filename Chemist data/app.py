import os
import pandas as pd
from flask import Flask, render_template, request, send_file, redirect, url_for, flash, session
import io # For sending file in memory
import logging

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = 'your_very_secret_key_for_session_and_flash' # Important for flashing messages and session

# --- Configuration ---
# Path to the single merged CSV file
# IMPORTANT: Ensure this path is correct and accessible by the Flask app.
# Example: INPUT_CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), 'data', 'merged_bso_dispensing_data.csv')
INPUT_CSV_FILE_PATH = r"C:\Users\Andrew\OneDrive\Documents\2025_PROJECT\2025_PROJECT\html_project\Python_webscrape\Webscrape Data\merged_bso_dispensing_data.csv" # Please ensure this path is correct for your system

# --- Helper Functions (Adapted from your script) ---
def load_single_csv_file(file_path):
    """Reads a single CSV file into a pandas DataFrame."""
    app.logger.info(f"Attempting to load data from: {file_path}")
    if not os.path.exists(file_path):
        app.logger.error(f"Input CSV file not found at {file_path}")
        flash(f"Error: Input CSV file not found at {file_path}", "danger")
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path)
        app.logger.info(f"Successfully loaded CSV: {os.path.basename(file_path)}")
        return df
    except Exception as e:
        app.logger.error(f"Error reading CSV file {file_path}: {e}")
        flash(f"Error reading CSV file: {e}", "danger")
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

def process_and_aggregate_data(df, chemist_id):
    """
    Filters data for a specific chemist and aggregates total items by year and month.
    """
    if df.empty:
        app.logger.warning("Input DataFrame is empty for processing.")
        return pd.DataFrame()

    required_cols = ['Chemist', 'Year', 'Month', 'Number of Items']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        app.logger.error(f"Input DataFrame is missing required columns: {', '.join(missing)}")
        flash(f"Error: Merged data is missing required columns: {', '.join(missing)}. Please ensure 'Chemist', 'Year', 'Month', 'Number of Items' are present.", "danger")
        return pd.DataFrame()

    try:
        # Ensure 'Chemist' column is of a numeric type for comparison
        df.loc[:, 'Chemist'] = pd.to_numeric(df['Chemist'], errors='coerce')
        # Create a copy to avoid SettingWithCopyWarning when modifying
        df_filtered = df.dropna(subset=['Chemist']).copy()
        
        # Filter by the specific chemist ID (ensure chemist_id is also numeric)
        df_filtered = df_filtered[df_filtered['Chemist'] == float(chemist_id)].copy() # Compare as float for safety

        if df_filtered.empty:
            app.logger.info(f"No data found for Chemist ID: {chemist_id} after initial filtering.")
            flash(f"No data found for Chemist ID: {chemist_id}.", "info")
            return pd.DataFrame()

        # Ensure 'Year', 'Month', and 'Number of Items' are in appropriate formats
        df_filtered.loc[:, 'Year'] = pd.to_numeric(df_filtered['Year'], errors='coerce').fillna(0).astype(int)
        df_filtered.loc[:, 'Month'] = pd.to_numeric(df_filtered['Month'], errors='coerce').fillna(0).astype(int)
        df_filtered.loc[:, 'Number of Items'] = pd.to_numeric(df_filtered['Number of Items'], errors='coerce').fillna(0)

        # Filter out rows with invalid Year or Month (e.g., 0)
        df_filtered = df_filtered[(df_filtered['Year'] > 0) & (df_filtered['Month'] >= 1) & (df_filtered['Month'] <= 12)].copy()

        if df_filtered.empty:
            app.logger.info("No valid Year/Month data found after filtering and data type conversion.")
            flash("No valid Year/Month data found after type conversion and filtering.", "info")
            return pd.DataFrame()

        app.logger.info(f"Aggregating data for Chemist ID: {chemist_id}")
        # Group by Year and Month and sum 'Number of Items'
        aggregated_df = df_filtered.groupby(['Year', 'Month'])['Number of Items'].sum().reset_index()

        # Rename the aggregated column for clarity
        aggregated_df = aggregated_df.rename(columns={'Number of Items': 'Total Items'})

        # Add the Chemist ID column back
        aggregated_df['Chemist'] = chemist_id # Store as originally passed

        # Sort by Year and Month for chronological order
        aggregated_df = aggregated_df.sort_values(by=['Year', 'Month']).reset_index(drop=True)
        app.logger.info("Aggregation complete.")
        # Reorder columns for the final output
        return aggregated_df[['Chemist', 'Year', 'Month', 'Total Items']]
    except Exception as e:
        app.logger.error(f"Error during data processing for chemist {chemist_id}: {e}")
        flash(f"An error occurred during data processing: {e}", "danger")
        return pd.DataFrame()

# --- Flask Routes ---
@app.route('/', methods=['GET'])
def index():
    """Serves the main page with the form."""
    # Load data to populate chemist dropdown
    consolidated_data = load_single_csv_file(INPUT_CSV_FILE_PATH)
    if consolidated_data.empty and not os.path.exists(INPUT_CSV_FILE_PATH):
         # If file not found, flash message is already handled by load_single_csv_file
        return render_template('index.html', unique_chemists=[])
    elif consolidated_data.empty and os.path.exists(INPUT_CSV_FILE_PATH):
        # File exists but might be empty or unreadable for chemist IDs
        flash("Could not load data to populate chemist dropdown. The CSV might be empty or improperly formatted.", "warning")
        return render_template('index.html', unique_chemists=[])


    unique_chemists = get_unique_chemist_ids(consolidated_data)
    if not unique_chemists and not consolidated_data.empty:
        flash("No chemist IDs could be extracted. The 'Chemist' column might be missing or empty in the CSV.", "warning")

    return render_template('index.html', unique_chemists=unique_chemists)

@app.route('/process', methods=['POST'])
def process_data_route():
    """Handles form submission, processes data, and then shows it or allows download."""
    try:
        y_input = request.form.get('y_input', 'ALL_Data')
        str_chemist_input = request.form.get('chemist_number') # This will be a string from the form

        if not str_chemist_input:
            flash("Please select a Chemist number.", "warning")
            return redirect(url_for('index'))

        # Convert the chemist number from form (string) to an integer for processing
        chemist_filter_number = int(str_chemist_input) 
    except ValueError:
        flash("Invalid Chemist number selected. Ensure it's a whole number.", "danger")
        return redirect(url_for('index'))
    except Exception as e:
        app.logger.error(f"Error processing form input: {e}")
        flash(f"Error processing your request: {e}", "danger")
        return redirect(url_for('index'))

    consolidated_data = load_single_csv_file(INPUT_CSV_FILE_PATH)
    if consolidated_data.empty:
        # Flash message handled by load_single_csv_file
        return redirect(url_for('index'))

    # Pass the numeric chemist_filter_number to the processing function
    monthly_totals_df = process_and_aggregate_data(consolidated_data, chemist_filter_number)

    if not monthly_totals_df.empty:
        output_filename = f'Chemist{chemist_filter_number}_FilteredData_{y_input.replace(" ", "_")}.xlsx'
        
        # Convert DataFrame to HTML table for display
        # Add some classes for styling and avoid pandas default index
        data_html_table = monthly_totals_df.to_html(classes='data-table table table-striped table-hover', index=False, border=0, escape=False)
        
        # Store DataFrame in session to allow download from results page
        # For larger DataFrames, consider alternative storage or re-generating on download request
        session['processed_data'] = monthly_totals_df.to_dict('records') # Store as list of dicts
        session['output_filename'] = output_filename

        app.logger.info(f"Data processed for Chemist {chemist_filter_number}. Rendering results page.")
        return render_template('results.html', 
                               table_data=data_html_table, 
                               chemist_id=chemist_filter_number, # Pass the original selected ID for display
                               filename_ref=y_input,
                               download_filename=output_filename)
    else:
        # Flash message should be handled by process_and_aggregate_data
        if not flash_is_pending(): # Check if a flash message was already set
             flash("No data to display or generate Excel file after processing.", "info")
        return redirect(url_for('index'))

@app.route('/download_excel')
def download_excel():
    """Serves the processed data as an Excel file for download."""
    if 'processed_data' in session and 'output_filename' in session:
        try:
            # Retrieve data from session and convert back to DataFrame
            data_records = session.pop('processed_data', None) # Pop to clear after download
            output_filename = session.pop('output_filename', 'filtered_data.xlsx')

            if not data_records:
                flash("No data found to download. Please try filtering again.", "warning")
                return redirect(url_for('index'))

            monthly_totals_df = pd.DataFrame(data_records)
            
            excel_buffer = io.BytesIO()
            monthly_totals_df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0) # Reset buffer's position to the beginning

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
    # Accessing session directly like this is generally okay in Flask request context
    # Flask's _flashes is a list of (category, message) tuples.
    return bool(request.environ.get('werkzeug.request') and request.environ['werkzeug.request'].session.get('_flashes'))


if __name__ == '__main__':
    # Set up basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app.logger.info("Flask application starting...")
    # For development: app.run(debug=True)
    # For production, use a proper WSGI server like Gunicorn or Waitress.
    app.run(debug=True, host='0.0.0.0', port=5001) # Using port 5001 as an example
