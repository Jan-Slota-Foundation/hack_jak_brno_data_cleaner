import pandas as pd
import os
from sys import argv
from typing import Dict
from fhir.resources.bundle import Bundle, BundleEntry
from supabase_client import FILES, download_all_files, upload_bundle_to_fhir_server, upload_cleaned_files
from cleaner import clean_all_dataframes
from exporter import create_fhir_resources
from mock_data_adder import process_files as add_mock_data
from postgres_client import upload_contacts_to_postgres

DIRECTORY = "current"

def load_dataframes(directory: str = DIRECTORY) -> Dict[str, pd.DataFrame]:
    """
    Load all CSV files from the specified directory into pandas DataFrames.
    Ensures files are downloaded first.
    """
    # Ensure files are present
    if not os.path.exists(directory) or not os.listdir(directory):
        print(f"Directory {directory} is empty or missing. Downloading files...")
        download_all_files(directory)

    dfs = {}
    for name in FILES:
        file_name = f"{name}.csv"
        file_path = os.path.join(directory, file_name)
        
        if os.path.exists(file_path):
            try:
                dfs[name] = pd.read_csv(file_path)
                print(f"Loaded {name} into DataFrame")
            except Exception as e:
                print(f"Error loading {name}: {e}")
        else:
            print(f"File {file_path} not found")
            
    return dfs

if __name__ == "__main__":
    if argv and len(argv) > 1:
        # Clean and push to supabase
        if argv[1] == "-c" or argv[1] == "--clean":
            dataframes = load_dataframes(DIRECTORY)
            cleaned_dfs = clean_all_dataframes(dataframes, output_dir=DIRECTORY)
            # View DF headers
            for name, df in cleaned_dfs.items():
                print(f"Cleaned DataFrame: {name}")
                print(df.head())
            
        elif argv[1] == "-s" or argv[1] == "--supabase":
            # Push to Supabase from DIRECTORY
            if os.path.exists(DIRECTORY):
                print(f"Uploading files from {DIRECTORY} to Supabase...")
                upload_cleaned_files(DIRECTORY)
            else:
                print(f"Directory {DIRECTORY} does not exist. Cannot push to Supabase.")

        elif argv[1] == "-f" or argv[1] == "--fhir":
            # Push to FHIR server (using data from DIRECTORY)
            print(f"Loading data from {DIRECTORY} for FHIR generation...")
            dataframes = load_dataframes(DIRECTORY)
            
            fhir_resources = create_fhir_resources(dataframes)
            upload_bundle_to_fhir_server(fhir_resources)

        elif argv[1] == "-d" or argv[1] == "--download":
            print(f"Downloading files to {DIRECTORY}...")
            download_all_files(DIRECTORY)
            
            dfs = load_dataframes(DIRECTORY)
            for name, df in dfs.items():
                print(f"\nDataFrame: {name}")
                print(df.head())

        elif argv[1] == "-g" or argv[1] == "--generate":
            print(f"Loading data from {DIRECTORY}...")
            dataframes = load_dataframes(DIRECTORY)
            
            fhir_resources = create_fhir_resources(dataframes)
            print(f"Generated {len(fhir_resources)} resources.")
            
            entries = [BundleEntry(resource=r) for r in fhir_resources]
            bundle = Bundle(type="collection", entry=entries)
            
            output_file = "generated_resources.json"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(bundle.model_dump_json(indent=2))
            
            print(f"Resources saved to {output_file}")

        elif argv[1] == "-m" or argv[1] == "--mock":
            print(f"Adding mock data to files in {DIRECTORY}...")
            add_mock_data(DIRECTORY)

        elif argv[1] == "-db" or argv[1] == "--database":
            print(f"Uploading contacts from {DIRECTORY} to PostgreSQL...")
            upload_contacts_to_postgres(DIRECTORY)

        elif argv[1] == "-h" or argv[1] == "--help":
            print("Usage: python3 main.py [OPTION]")
            print("Options:")
            print("  -c, --clean     Download, clean, and save files to 'current/' directory.")
            print("  -s, --supabase  Upload files from 'current/' to Supabase.")
            print("  -f, --fhir      Generate and upload FHIR resources to server.")
            print("  -d, --download  Download files from Supabase to 'current/' directory.")
            print("  -g, --generate  Generate FHIR resources locally from 'current/' and save to JSON.")
            print("  -m, --mock      Add mock contact data to files in 'current/' directory.")
            print("  -db, --database Upload contact info from 'current/' to PostgreSQL.")
            print("  -h, --help      Show this help message.")

    else:
        print("No arguments provided. Use -h or --help for usage information.")