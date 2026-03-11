from flask import Flask, request, jsonify
import pandas as pd
import io
import re

app = Flask(__name__)

def extract_period_from_filename(name):
    match = re.findall(r"\d{8}", name)
    return match[0] if match else None

@app.route('/upload-balance-sheet', methods=['POST'])
def upload_balance_sheet():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    filename = file.filename

    content = file.read()

    df_raw = pd.read_excel(io.BytesIO(content), header=None)


    start_row = df_raw[
        df_raw.apply(lambda row: row.astype(str).str.contains("Mã số").any(), axis=1)
    ].index[0]


    df = pd.read_excel(io.BytesIO(content), header=start_row)

    # Đổi tên cột
    df = df.rename(columns={
        df.columns[0]: "Chi tiêu",
        df.columns[1]: "ma_cod",
        df.columns[2]: "Cuối kỳ",
        df.columns[3]: "Đầu kỳ"
    })


    df = df[df["ma_cod"].notna()]


    for col in ["Cuối kỳ", "Đầu kỳ"]:
        df[col] = df[col].astype(str).str.replace(",", "").str.replace(" ", "")
        df[col] = pd.to_numeric(df[col], errors="coerce")


    df["Period"] = extract_period_from_filename(filename)
    df["SourceName"] = filename
    df["ReportType"] = "Cân đối kế toán"

    df = df[["Period", "ma_cod", "Cuối kỳ", "Đầu kỳ", "Chi tiêu", "SourceName", "ReportType"]]

    return jsonify(df.to_dict(orient="records"))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
