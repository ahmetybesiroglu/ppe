# main.py

import subprocess
import sys
import os
from pathlib import Path

def main():
    # Check if .env file exists
    env_file = Path('.env')
    if not env_file.exists():
        print('Error: .env file not found. Please create a .env file with the necessary environment variables.')
        sys.exit(1)

    # Ensure data directory exists
    data_dir = Path('data')
    if not data_dir.exists():
        data_dir.mkdir(parents=True)
        print(f'Created data directory at {data_dir}')

    # Define the scripts to run in order
    scripts = [
        'src/01_data_retrieval_freshservice.py',
        'src/02_data_retrieval_airtable.py',
        'src/03_data_processing_freshservice.py',
        'src/04_data_processing_headcount.py',
        'src/05_data_standardization.py',
    ]

    for script in scripts:
        script_path = Path(script)
        if script_path.exists():
            print(f'\nRunning {script}\n' + '-'*50)
            result = subprocess.run([sys.executable, str(script_path)])
            if result.returncode != 0:
                print(f'\nError running {script}. Exiting.')
                sys.exit(result.returncode)
        else:
            print(f'\nScript {script} not found. Exiting.')
            sys.exit(1)

    print('\nAll scripts executed successfully.')

    # Now, launch the Streamlit app
    streamlit_script = 'src/06_laptop_matching.py'
    streamlit_script_path = Path(streamlit_script)
    if streamlit_script_path.exists():
        print('\nLaunching Streamlit app...\n' + '-'*50)
        # Use subprocess.Popen to run the Streamlit app without blocking
        result = subprocess.Popen(['streamlit', 'run', str(streamlit_script_path)])
        print('Streamlit app is running. You can access it in your web browser.')
        # Optionally, wait for the Streamlit app to exit
        result.wait()
    else:
        print(f'\nStreamlit app script {streamlit_script} not found. Exiting.')
        sys.exit(1)

if __name__ == "__main__":
    main()
