from flask import Flask, request, jsonify
from flask_cors import CORS
from flask import Response
import os
import pandas as pd
import numpy as np
import glob
import json

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




if __name__ == '__main__':
    app.run(debug=True)