import subprocess
import sys

# List of scripts to run in order
scripts = [
    'split_adg_all_raw_daily_report.py',
    # Dimensions
    'upsert_dim_measurement_unit.py',
    'upsert_dim_storage.py',
    'upsert_storage_group.py',
    'upsert_storage_group_category.py',
    'upsert_dim_supplier.py',
    'upsert_dim_customer.py',
    'upsert_dim_product.py',
    # Facts / operational
    'upsert_stock_in.py',
    'upsert_stock_out.py',
    'upsert_stock_remaining.py',
    'upsert_fact_order.py',
    'upsert_fact_stock_in.py',
    'upsert_fact_stock_move.py',
    'upsert_fact_stock_out.py',
]

# Get the Python executable path
python_exe = sys.executable

for script in scripts:
    print(f"Running {script}...")
    try:
        result = subprocess.run([python_exe, script], capture_output=True, text=True, cwd='.')
        print(result.stdout)
        if result.stderr:
            print(f"Stderr: {result.stderr}")
        if result.returncode != 0:
            print(f"Error: {script} exited with code {result.returncode}")
            break
        print(f"Completed {script}\n")
    except Exception as e:
        print(f"Failed to run {script}: {e}")
        break

print("Pipeline execution finished.")