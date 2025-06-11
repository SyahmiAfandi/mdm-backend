from flask import Flask, request, jsonify
from flask_cors import CORS
from flask import Response
from flask import send_file
from datetime import datetime, timedelta
import xlsxwriter
import io
import os
import pandas as pd
import numpy as np
import glob
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials




def convert_np(obj):
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    return str(obj)

app = Flask(__name__)
CORS(app)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/upload_FCSHPC_OSDP', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return jsonify({"error": "No files provided"}), 400

    files = request.files.getlist('files')
    if not files or len(files) == 0:
        return jsonify({"error": "Empty file list"}), 400

    merged_df = pd.DataFrame()

    for file in files:
        if file.filename == '':
            continue
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        try:
            # Read specific columns from Excel (adjust column indices as needed)
            df = pd.read_excel(filepath, skiprows=2, usecols=[8, 9, 13, 16, 17, 18])
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        except Exception as e:
            return jsonify({"error": f"Error reading {file.filename}: {str(e)}"}), 500

    # Clean and prepare data
    merged_df['Distributor'] = merged_df['Distributor'].ffill()
    merged_df['Distributor Name'] = merged_df['Distributor Name'].ffill()
    
    # Get sorting parameters
    primary_sort = request.args.get('primary_sort', 'Distributor')
    secondary_sort = request.args.get('secondary_sort', 'Sales Route')
    primary_asc = request.args.get('primary_asc', 'true').lower() == 'true'
    secondary_asc = request.args.get('secondary_asc', 'true').lower() == 'true'

    # Sort the data
    sorted_df = merged_df.sort_values(
        by=[primary_sort, secondary_sort],
        ascending=[primary_asc, secondary_asc]
    )

     # Create simplified summary - just distributor code and count
    summary_df = sorted_df.groupby(['Distributor','Distributor Name']).size().reset_index(name='Total Data')

    return jsonify({
        "sorted_data": sorted_df.to_dict(orient='records'),
        "summary_data": summary_df.to_dict(orient='records')
    })

@app.route('/upload_FCSHPC_PBI', methods=['POST'])
def upload_files_pbi():
    if 'files1' not in request.files:
        return jsonify({"error": "No files provided"}), 400

    files = request.files.getlist('files1')
    if not files or len(files) == 0:
        return jsonify({"error": "Empty file list"}), 400

    merged_df1 = pd.DataFrame()

    for file in files:
        if file.filename == '':
            continue
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        try:
            # Read specific columns (adjust as per your actual column names or indices)
            df1 = pd.read_excel(filepath, usecols=[8,9,13,18,22,26])
            df1.drop(df1.tail(3).index,inplace=True)
            merged_df1 = pd.concat([merged_df1, df1], ignore_index=True)
        except Exception as e:
            return jsonify({"error": f"Error reading {file.filename}: {str(e)}"}), 500
    

    # Sorting
    primary_sort = request.args.get('primary_sort', 'Distributor')
    secondary_sort = request.args.get('secondary_sort', 'Sales Route')
    primary_asc = request.args.get('primary_asc', 'true').lower() == 'true'
    secondary_asc = request.args.get('secondary_asc', 'true').lower() == 'true'

    sorted_df = merged_df1.sort_values(
        by=[primary_sort, secondary_sort],
        ascending=[primary_asc, secondary_asc]
    )

    # Summary
    summary_df = sorted_df.groupby(['Distributor', 'Distributor Name']).size().reset_index(name='Total Data')

    return jsonify({
        "sorted_data_PBI": sorted_df.to_dict(orient='records'),
        "summary_data_PBI": summary_df.to_dict(orient='records')
    })

@app.route('/clear', methods=['POST'])
def clear_data():
    try:
        for filename in os.listdir(UPLOAD_FOLDER):
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
        return jsonify({"message": "All uploaded files deleted."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/clear_pbi', methods=['POST'])
def clear_pbi_data():
    files = glob.glob(os.path.join(UPLOAD_FOLDER, '*'))
    for f in files:
        os.remove(f)
    return jsonify({"message": "Power BI data cleared and files deleted"}), 200


@app.route('/reconcile_all', methods=['POST'])
def reconcile_all_data():
    try:
        data = request.get_json()
        osdp_data = data.get('osdp_data', [])
        pbi_data = data.get('pbi_data', [])

        if not osdp_data or not pbi_data:
            return jsonify({"error": "Missing data for reconciliation"}), 400

        osdp_df = pd.DataFrame(osdp_data)
        pbi_df = pd.DataFrame(pbi_data)

        required_columns = ['Distributor', 'Sales Route']
        missing_osdp_cols = [col for col in required_columns if col not in osdp_df.columns]
        missing_pbi_cols = [col for col in required_columns if col not in pbi_df.columns]

        if missing_osdp_cols or missing_pbi_cols:
            return jsonify({
                "error": "Missing required columns",
                "osdp_missing": missing_osdp_cols,
                "pbi_missing": missing_pbi_cols
            }), 400

        # Create composite key
        osdp_df['key'] = osdp_df['Distributor'].astype(str) + ' - ' + osdp_df['Sales Route'].astype(str)
        pbi_df['key'] = pbi_df['Distributor'].astype(str) + ' - ' + pbi_df['Sales Route'].astype(str)

        osdp_keys = set(osdp_df['key'])
        pbi_keys = set(pbi_df['key'])

        # Identify records unique to one dataset
        only_in_osdp = osdp_df[osdp_df['key'].isin(osdp_keys - pbi_keys)].copy()
        only_in_osdp['Mismatch Type'] = 'Missing in PBI'

        only_in_pbi = pbi_df[pbi_df['key'].isin(pbi_keys - osdp_keys)].copy()
        only_in_pbi['Mismatch Type'] = 'Missing in OSDP'

        # Compare common keys for value mismatches
        mismatched_values = []
        common_keys = osdp_keys & pbi_keys
        compare_columns = [col for col in osdp_df.columns if col not in ['key','Distributor Name']]

        for key in common_keys:
            osdp_row = osdp_df[osdp_df['key'] == key].iloc[0]
            pbi_row = pbi_df[pbi_df['key'] == key].iloc[0]

            diffs = {}
            for col in compare_columns:
                osdp_val = osdp_row.get(col, None)
                pbi_val = pbi_row.get(col, None)
                if pd.isna(osdp_val) and pd.isna(pbi_val):
                    continue
                if osdp_val != pbi_val:
                    diffs[col] = {'OSDP': osdp_val, 'PBI': pbi_val}

            if diffs:
                mismatched_values.append({
                    "Distributor": osdp_row['Distributor'],
                    "Distributor Name": osdp_row.get('Distributor Name', ''),
                    "Sales Route": osdp_row['Sales Route'],
                    "Mismatch Type": "Value mismatch",
                    "Differences": diffs
                })

        # Combine all mismatches
        reconciliation_result = []
        reconciliation_result.extend(only_in_osdp.to_dict(orient='records'))
        reconciliation_result.extend(only_in_pbi.to_dict(orient='records'))
        reconciliation_result.extend(mismatched_values)

        # Build a mismatch set from reconciliation result
        mismatch_distributors = { str(item['Distributor']).strip() for item in reconciliation_result }


        # Generate summary OSDP
        summary_osdp = []
        for _, row in osdp_df[['Distributor', 'Distributor Name']].drop_duplicates().dropna().iterrows():
            distributor_code = str(row['Distributor']).strip()
            summary_osdp.append({
                'Distributor': row['Distributor'],
                'Distributor Name': row['Distributor Name'],
                'Status': 'Mismatch' if distributor_code in mismatch_distributors else 'Match'
            })

        # Generate summary PBI
        summary_pbi = []
        for _, row in pbi_df[['Distributor', 'Distributor Name']].drop_duplicates().dropna().iterrows():
            distributor_code = str(row['Distributor']).strip()
            summary_pbi.append({
                'Distributor': row['Distributor'],
                'Distributor Name': row['Distributor Name'],
                'Status': 'Mismatch' if distributor_code in mismatch_distributors else 'Match'
            })

        # Custom converter to fix int64/float64 JSON issues
        def convert_np(obj):
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            if isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            return str(obj)

        return Response(
            json.dumps({
                'summary_osdp': summary_osdp,
                'summary_pbi': summary_pbi,
                'reconciliation_result': reconciliation_result
            }, default=convert_np),
            mimetype='application/json'
        )

    except Exception as e:
        print("Error in /reconcile_all:", str(e))
        return jsonify({"error": str(e)}), 500
    
@app.route('/upload_HPCEFOSSALES_OSDP', methods=['POST'])
def upload_files_HPC_EFOS_sales_OSDP():
    if 'files' not in request.files:
        return jsonify({"error": "No files provided"}), 400

    files = request.files.getlist('files')
    if not files or len(files) == 0:
        return jsonify({"error": "Empty file list"}), 400

    merged_df = pd.DataFrame()

    for file in files:
        if file.filename == '':
            continue
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        try:
            # Read specific columns from Excel (adjust column indices as needed)
            df = pd.read_excel(filepath,skiprows=2,usecols=[0,1,2,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,22,27])
            df.drop(df.tail(2).index,inplace=True)
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        except Exception as e:
            return jsonify({"error": f"Error reading {file.filename}: {str(e)}"}), 500

    # Clean and prepare data
    merged_df['Distributor'] = merged_df['Distributor'].ffill()
    merged_df['Distributor Name'] = merged_df['Distributor Name'].ffill()
    
    # Get sorting parameters
    primary_sort = request.args.get('primary_sort', 'Distributor')
    secondary_sort = request.args.get('secondary_sort', 'Sales Route')
    primary_asc = request.args.get('primary_asc', 'true').lower() == 'true'
    secondary_asc = request.args.get('secondary_asc', 'true').lower() == 'true'

    # Sort the data
    sorted_df = merged_df.sort_values(
        by=[primary_sort, secondary_sort],
        ascending=[primary_asc, secondary_asc]
    )

     # Create simplified summary - just distributor code and count
    summary_df = sorted_df.groupby(['Distributor','Distributor Name']).size().reset_index(name='Total Data')

    return jsonify({
        "sorted_data": sorted_df.to_dict(orient='records'),
        "summary_data": summary_df.to_dict(orient='records')
    })

@app.route('/upload_HPCEFOSSALES_PBI', methods=['POST'])
def upload_files_HPC_EFOS_Sales_pbi():
    if 'files1' not in request.files:
        return jsonify({"error": "No files provided"}), 400

    files = request.files.getlist('files1')
    if not files or len(files) == 0:
        return jsonify({"error": "Empty file list"}), 400

    merged_df1 = pd.DataFrame()

    for file in files:
        if file.filename == '':
            continue
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        try:
            # Read specific columns (adjust as per your actual column names or indices)
            df1 = pd.read_excel(filepath,usecols=[0,1,2,5,7,8,10,11,12,13,14,15,16,17,18,19,20,21,23,28])
            df1.drop(df1.tail(3).index,inplace=True)
            columns_to_truncate = ['#SKU / Actual Calls', 
                       'Effective Outlet Time /Actual Calls', 
                       'PJP Compliance %',
                       'Total Time Spent / Working Days',
                       'Total Transit Time / Working Days',
                       'Effective Outlet Time / Salesman',
                       'Effective Day %'
                       ]

            for col in columns_to_truncate:
                df1[col] = np.trunc(df1[col] * (10**6)) / (10**6)


            merged_df1 = pd.concat([merged_df1, df1], ignore_index=True)
        except Exception as e:
            return jsonify({"error": f"Error reading {file.filename}: {str(e)}"}), 500
    

    # Sorting
    primary_sort = request.args.get('primary_sort', 'Distributor')
    secondary_sort = request.args.get('secondary_sort', 'Sales Route')
    primary_asc = request.args.get('primary_asc', 'true').lower() == 'true'
    secondary_asc = request.args.get('secondary_asc', 'true').lower() == 'true'

    sorted_df = merged_df1.sort_values(
        by=[primary_sort, secondary_sort],
        ascending=[primary_asc, secondary_asc]
    )

    # Summary
    summary_df = sorted_df.groupby(['Distributor', 'Distributor Name']).size().reset_index(name='Total Data')

    return jsonify({
        "sorted_data_PBI": sorted_df.to_dict(orient='records'),
        "summary_data_PBI": summary_df.to_dict(orient='records')
    })

@app.route('/upload_HPCIQSALES_OSDP', methods=['POST'])
def upload_files_HPC_IQ_sales_OSDP():
    if 'files' not in request.files:
        return jsonify({"error": "No files provided"}), 400

    files = request.files.getlist('files')
    if not files or len(files) == 0:
        return jsonify({"error": "Empty file list"}), 400

    merged_df = pd.DataFrame()

    for file in files:
        if file.filename == '':
            continue
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        try:
            # Read specific columns from Excel (adjust column indices as needed)
            df = pd.read_excel(filepath,skiprows=2,usecols=[0,1,2,5,6,7,10,11,12,25,26,27,35,36,37,55])
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        except Exception as e:
            return jsonify({"error": f"Error reading {file.filename}: {str(e)}"}), 500

    # Clean and prepare data
    merged_df['Distributor'] = merged_df['Distributor'].ffill()
    merged_df['Distributor Name'] = merged_df['Distributor Name'].ffill()
    
    # Get sorting parameters
    primary_sort = request.args.get('primary_sort', 'Distributor')
    secondary_sort = request.args.get('secondary_sort', 'Sales Route')
    primary_asc = request.args.get('primary_asc', 'true').lower() == 'true'
    secondary_asc = request.args.get('secondary_asc', 'true').lower() == 'true'

    # Sort the data
    sorted_df = merged_df.sort_values(
        by=[primary_sort, secondary_sort],
        ascending=[primary_asc, secondary_asc]
    )

     # Create simplified summary - just distributor code and count
    summary_df = sorted_df.groupby(['Distributor','Distributor Name']).size().reset_index(name='Total Data')

    return jsonify({
        "sorted_data": sorted_df.to_dict(orient='records'),
        "summary_data": summary_df.to_dict(orient='records')
    })

@app.route('/upload_HPCIQSALES_PBI', methods=['POST'])
def upload_files_HPC_IQ_Sales_pbi():
    if 'files1' not in request.files:
        return jsonify({"error": "No files provided"}), 400

    files = request.files.getlist('files1')
    if not files or len(files) == 0:
        return jsonify({"error": "Empty file list"}), 400

    merged_df1 = pd.DataFrame()

    for file in files:
        if file.filename == '':
            continue
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)
        try:
            # Read specific columns (adjust as per your actual column names or indices)
            df1 = pd.read_excel(filepath,usecols=[0,1,2,7,8,9,12,13,14,27,28,29,37,38,39,57])
            df1.drop(df1.tail(2).index,inplace=True)
            df1 = df1.fillna(0)
            df1['Distributor'] = df1['Distributor'].astype(float)

            merged_df1 = pd.concat([merged_df1, df1], ignore_index=True)
        except Exception as e:
            return jsonify({"error": f"Error reading {file.filename}: {str(e)}"}), 500
    

    # Sorting
    primary_sort = request.args.get('primary_sort', 'Distributor')
    secondary_sort = request.args.get('secondary_sort', 'Sales Route')
    primary_asc = request.args.get('primary_asc', 'true').lower() == 'true'
    secondary_asc = request.args.get('secondary_asc', 'true').lower() == 'true'

    sorted_df = merged_df1.sort_values(
        by=[primary_sort, secondary_sort],
        ascending=[primary_asc, secondary_asc]
    )

    # Summary
    summary_df = sorted_df.groupby(['Distributor', 'Distributor Name']).size().reset_index(name='Total Data')

    return jsonify({
        "sorted_data_PBI": sorted_df.to_dict(orient='records'),
        "summary_data_PBI": summary_df.to_dict(orient='records')
    })

@app.route('/export_summary_excel', methods=['POST'])
def export_summary_excel():
    data = request.get_json()
    records = data.get("records", [])
    report_type = data.get("report_type", "OSDP")

    if not records:
        return jsonify({"error": "No data to export"}), 400

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet(f"{report_type} Summary")

    # Define styles
    header_format = workbook.add_format({
        'bold': True, 'font_color': 'white', 'bg_color': '#4F81BD',
        'border': 1, 'align': 'center'
    })
    match_format = workbook.add_format({'bg_color': '#C6EFCE', 'border': 1})
    mismatch_format = workbook.add_format({'bg_color': '#F4CCCC', 'border': 1})
    text_format = workbook.add_format({'border': 1})

    # Headers
    headers = ['Distributor', 'Distributor Name', 'Status']
    for col, h in enumerate(headers):
        worksheet.write(0, col, h, header_format)
        worksheet.set_column(col, col, 20)

    # Rows
    for row_idx, row in enumerate(records, start=1):
        worksheet.write(row_idx, 0, row['Distributor'], text_format)
        worksheet.write(row_idx, 1, row['Distributor Name'], text_format)
        status_format = match_format if row['Status'] == 'Match' else mismatch_format
        worksheet.write(row_idx, 2, row['Status'], status_format)

    workbook.close()
    output.seek(0)
    return send_file(output, download_name="summary_report.xlsx", as_attachment=True)

@app.route('/export_result_excel', methods=['POST'])
def export_result_excel():
    data = request.get_json()
    records = data.get('records', [])
    mode = data.get('mode', 'current')
    business_type = data.get('businessType', 'N/A')
    report_type = data.get('reportType', 'N/A')
    creator = data.get('creator', 'Auto Generated')

    include_field_columns = mode == 'all' or any(row.get('Mismatch Type') == 'Value mismatch' for row in records)
    headers = ['Distributor', 'Distributor Name', 'Sales Route', 'Mismatch Type'] + (['Field', 'OSDP', 'PBI'] if include_field_columns else [])

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('Reconciliation')

    # Define formats
    title_format = workbook.add_format({
        'bold': True, 'font_size': 16, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#DDEBF7', 'font_color': '#1F4E78'
    })
    subtitle_format = workbook.add_format({
        'italic': True, 'font_size': 11, 'align': 'left', 'valign': 'vcenter', 'bg_color': '#F2F2F2'
    })
    meta_format = workbook.add_format({
        'bg_color': '#F2F2F2'
    })
    header_format = workbook.add_format({
        'bold': True, 'text_wrap': True, 'valign': 'middle', 'align': 'center', 'bg_color': '#B4C6E7', 'border': 1
    })
    center_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})
    left_format = workbook.add_format({'align': 'left', 'valign': 'vcenter', 'border': 1})
    value_mismatch_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#F8CBAD'})
    missing_osdp_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#FFE699'})
    missing_pbi_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#BDD7EE'})
    highlight_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#FFC7CE'})

    subtitle_value_format = workbook.add_format({
        'italic': True, 'font_size': 11, 'align': 'left', 'valign': 'vcenter', 'bg_color': '#F2F2F2'
    })

    # Detect if running on Render (you can also check other env vars if needed)
    is_render = os.getenv("RENDER", "").lower() == "true"

    # Adjust time accordingly
    current_time = datetime.utcnow() + timedelta(hours=8) if is_render else datetime.now()
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M')

    # Header rows
    worksheet.merge_range(0, 0, 0, len(headers) - 1, 'Mismatch Result Report', title_format)
    worksheet.write('A2', 'Created by:', subtitle_format)
    worksheet.write('D2', 'Business Type:', subtitle_format)
    worksheet.write('A3', 'Created on:', subtitle_format)
    worksheet.write('D3', 'Report Type:', subtitle_format)
    
    worksheet.merge_range('B2:C2', creator, subtitle_value_format)
    worksheet.merge_range('E2:G2', business_type, subtitle_value_format)
    worksheet.merge_range('B3:C3', formatted_time, subtitle_value_format)
    worksheet.merge_range('E3:G3', report_type, subtitle_value_format)


    worksheet.merge_range('A4:G4', '', meta_format)

    for col, header in enumerate(headers):
        worksheet.write(4, col, header, header_format)

    col_widths = [len(header) for header in headers]
    row_idx = 5

    for row in records:
        mismatch_type = row.get('Mismatch Type', '')
        if mismatch_type == 'Value mismatch' and 'Differences' in row:
            for field, values in row['Differences'].items():
                values_to_write = [row.get('Distributor', ''), row.get('Distributor Name', ''), row.get('Sales Route', ''), mismatch_type, field, values.get('OSDP', ''), values.get('PBI', '')]
                for col, val in enumerate(values_to_write):
                    fmt = (value_mismatch_format if col == 3 else
                           highlight_format if col in [5,6] and values.get('OSDP') != values.get('PBI') else
                           center_format if col in [0,2,5,6] else
                           left_format)
                    worksheet.write(row_idx, col, '-' if val in [None, ''] else val, fmt)
                    col_widths[col] = max(col_widths[col], len(str(val)))
                row_idx += 1
        elif mismatch_type == 'Value mismatch':
            values_to_write = [row.get('Distributor', ''), row.get('Distributor Name', ''), row.get('Sales Route', ''), mismatch_type, '', '', '']
            for col, val in enumerate(values_to_write):
                fmt = (value_mismatch_format if col == 3 else center_format if col in [0,2,5,6] else left_format)
                worksheet.write(row_idx, col, '-' if val in [None, ''] else val, fmt)
                col_widths[col] = max(col_widths[col], len(str(val)))
            row_idx += 1
        elif mode == 'all':
            values_to_write = [row.get('Distributor', ''), row.get('Distributor Name', ''), row.get('Sales Route', ''), mismatch_type, '', '', '']
            for col, val in enumerate(values_to_write):
                fmt = (missing_osdp_format if col == 3 and mismatch_type == 'Missing in OSDP' else
                       missing_pbi_format if col == 3 and mismatch_type == 'Missing in PBI' else
                       center_format if col in [0,2,5,6] else
                       left_format)
                worksheet.write(row_idx, col, '-' if val in [None, ''] else val, fmt)
                col_widths[col] = max(col_widths[col], len(str(val)))
            row_idx += 1
        else:
            values_to_write = [row.get('Distributor', ''), row.get('Distributor Name', ''), row.get('Sales Route', ''), mismatch_type]
            for col, val in enumerate(values_to_write):
                fmt = (missing_osdp_format if col == 3 and mismatch_type == 'Missing in OSDP' else
                       missing_pbi_format if col == 3 and mismatch_type == 'Missing in PBI' else
                       center_format if col in [0,2] else
                       left_format)
                worksheet.write(row_idx, col, '-' if val in [None, ''] else val, fmt)
                col_widths[col] = max(col_widths[col], len(str(val)))
            row_idx += 1

    for col, width in enumerate(col_widths):
        if include_field_columns and col in [5, 6]:
            worksheet.set_column(col, col, 12)
        else:
            worksheet.set_column(col, col, width + 2)

    worksheet.autofilter(4, 0, 4, len(headers) - 1)
    worksheet.freeze_panes(5, 0)
    worksheet.hide_gridlines(2)

    workbook.close()
    output.seek(0)

    return send_file(output, as_attachment=True, download_name=f'Reconciliation_{mode}.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# 🔧 Utility: Smart date parser to detect US vs EU formats
def smart_parse_date(series):
    parsed_us = pd.to_datetime(series, errors='coerce', dayfirst=False)
    parsed_eu = pd.to_datetime(series, errors='coerce', dayfirst=True)

    us_valid = parsed_us.notna().sum()
    eu_valid = parsed_eu.notna().sum()

    return parsed_eu if eu_valid > us_valid else parsed_us

# 📌 Route 1: Get columns for user selection
@app.route('/get_columns', methods=['POST'])
def get_columns():
    file = request.files['file']
    ext = file.filename.split('.')[-1]

    if ext == 'csv':
        df = pd.read_csv(file, nrows=0)
    else:
        df = pd.read_excel(file, nrows=0)

    return {'columns': df.columns.tolist()}


# 📌 Route 2: Convert selected date columns to user-specified format
@app.route('/convert_date', methods=['POST'])
def convert_date():
    file = request.files['file']
    columns = request.form.get('columns')
    date_format = request.form.get('format') or 'DD/MM/YYYY'

    # Convert custom format to Python strftime
    format_map = {
        'DD/MM/YYYY': '%d/%m/%Y',
        'DD/MM/YYYY HH:mm:ss': '%d/%m/%Y %H:%M:%S',
        'YYYY-MM-DD': '%Y-%m-%d',
    }
    strf_format = format_map.get(date_format, '%d/%m/%Y')

    # Load file
    if file.filename.endswith('.csv'):
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file)

    # Load selected columns from frontend
    try:
        cols = list(eval(columns)) if columns else []
    except Exception:
        cols = []

    # Convert each selected column
    for col in cols:
        if col in df.columns:
            try:
                parsed = smart_parse_date(df[col])
                df[col] = parsed.dt.strftime(strf_format)
            except Exception as e:
                print(f"Failed to parse column {col}: {e}")

    # Return converted Excel file
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, download_name='converted_dates.xlsx', as_attachment=True)

    # Setup credentials and connect to Google Sheet
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

# Load credentials from environment variable
creds_json = os.environ.get('GOOGLE_CREDS')
if creds_json:
    creds_dict = json.loads(creds_json)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
else:
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

creds_dict = json.loads(creds_json)
credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
gc = gspread.authorize(credentials)

SHEET_ID = '1ql1BfkiuRuU3A3mfOxEw_GoL2gP5ki7eQECHxyfvFwk'
worksheet = gc.open_by_key(SHEET_ID).worksheet('Summary')


@app.route('/export_to_sheets', methods=['POST'])
def export_to_sheets():
    data = request.get_json()

    year = str(data['year']).strip()
    month = str(data['month']).strip()
    business_type = data['businessType'].strip().lower()
    report_type = data['reportType'].strip().lower()
    records = data['records']
    pic = data['pic']
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sheet_data = worksheet.get_all_records()
    headers = worksheet.row_values(1)

    updates = []
    updated_rows = 0
    skipped_records = []

    for i, row in enumerate(sheet_data, start=2):  # i = actual sheet row (including header)
        row_year = str(row['Year']).strip()
        row_month = str(row['Month']).strip()
        row_type = row['Business Type'].strip().lower()
        row_report = row['Report Type'].strip().lower()
        row_code = str(row['Distributor Code']).strip()

        for record in records:
            rec_code = str(record['Distributor']).strip()

            if (
                row_year == year and
                row_month == month and
                row_type == business_type and
                row_report == report_type and
                row_code == rec_code
            ):
                # Build A1 notation ranges and collect values
                updates.append({
                    'range': f'{gspread.utils.rowcol_to_a1(i, headers.index("Report Status") + 1)}',
                    'values': [[record['Status']]]
                })
                updates.append({
                    'range': f'{gspread.utils.rowcol_to_a1(i, headers.index("PIC") + 1)}',
                    'values': [[pic]]
                })
                updates.append({
                    'range': f'{gspread.utils.rowcol_to_a1(i, headers.index("Timestamp") + 1)}',
                    'values': [[timestamp]]
                })
                updated_rows += 1
                break
        else:
            skipped_records.append(row_code)

    # Perform batch update to avoid 429 quota errors
    if updates:
        worksheet.batch_update(updates)

    return jsonify({
        "status": "success",
        "updated_rows": updated_rows,
        "skipped_distributors": skipped_records
    })


if __name__ == '__main__':
    app.run(debug=True)