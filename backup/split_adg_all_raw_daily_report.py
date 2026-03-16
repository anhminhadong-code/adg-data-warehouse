from pathlib import Path
import pandas as pd

input_file = r"C:\Projects\ADG\DWH\raw-data\ADG - ALL RAW DAILY  REPORT.xlsx"
out_dir = Path(r"C:\Projects\ADG\DWH\raw-data")
out_dir.mkdir(exist_ok=True)

sheet_map = {
    "mh": "stock_in.xlsx",
    "bh": "stock_out.xlsx",
    "ck": "stock_remaining.xlsx",
    "Danh sach kho": "storage_group_category.xlsx",
    "Phân loại": "storage_group.xlsx",
}

sheets = pd.read_excel(input_file, sheet_name=list(sheet_map.keys()))

for s, f in sheet_map.items():
    sheets[s].to_excel(out_dir / f, index=False)