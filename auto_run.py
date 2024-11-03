import os
import glob
import subprocess

def delete_csv_files():
    # Get a list of all CSV files in the current directory
    csv_files = glob.glob("*.csv")
    # Delete each CSV file
    for csv_file in csv_files:
        try:
            os.remove(csv_file)
            print(f"Deleted: {csv_file}")
        except Exception as e:
            print(f"Error deleting {csv_file}: {e}")

def run_script(script_name):
    # Run the specified Python script
    try:
        subprocess.run(["python", script_name], check=True)
        print(f"Successfully ran: {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")

if __name__ == "__main__":
    delete_csv_files()       # Step 1: Delete all CSV files
    run_script("MFCode.py")
    run_script("Holdings.py")
    run_script("Stkcode.py")
    run_script("commit_files")
    run_script("commit_files.py")  # Step 3: Run commit_files.py
