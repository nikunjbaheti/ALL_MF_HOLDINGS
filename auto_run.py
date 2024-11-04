import os
import glob
import subprocess

def delete_files():
    # Get a list of all CSV, LOG, and TXT files in the current directory
    files_to_delete = glob.glob("*.csv") + glob.glob("*.log") + glob.glob("*.txt")
    # Delete each file
    for file in files_to_delete:
        try:
            os.remove(file)
            print(f"Deleted: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")

def run_script(script_name):
    # Run the specified Python script
    try:
        subprocess.run(["python", script_name], check=True)
        print(f"Successfully ran: {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")

if __name__ == "__main__":
    delete_files()            # Step 1: Delete all CSV, LOG, and TXT files
    run_script("MFCode.py")
    run_script("Holdings.py")
    # run_script("Stkcode.py")
    run_script("commit_files.py")  # Step 3: Run commit_files.py
