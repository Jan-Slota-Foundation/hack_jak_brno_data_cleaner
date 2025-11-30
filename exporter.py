import pandas as pd
import os
import re
from typing import Dict, List
from supabase_client import FILES, download_all_files
from fhir.resources.organization import Organization
from fhir.resources.plandefinition import PlanDefinition
from fhir.resources.narrative import Narrative
from fhir.resources.reference import Reference
from fhir.resources.domainresource import DomainResource

def sanitize_id(text: str) -> str:
    """Generate a valid FHIR ID from a string."""
    # Replace non-alphanumeric characters with hyphens, remove leading/trailing hyphens
    sanitized = re.sub(r'[^a-zA-Z0-9\-\.]', '-', str(text)).lower()
    return re.sub(r'-+', '-', sanitized).strip('-')

def load_dataframes(directory: str = "docs") -> Dict[str, pd.DataFrame]:
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

def create_fhir_resources(dataframes: Dict[str, pd.DataFrame]) -> List[DomainResource]:
    resources = []
    
    for df_name, df in dataframes.items():
        # Use standardized column names from cleaner.py
        name_col = "oddeleni"
        process_col = "proces"
        desc_col = "popis_procesu"
        relation_col = "vazba_na_org_rad"
        
        if name_col not in df.columns:
            print(f"Skipping {df_name}: Standardized column '{name_col}' not found.")
            continue
            
        # Find the "parent" organization (first unique one in the dataframe)
        # Assuming the first row/group represents the main department for this file
        unique_orgs = df[name_col].unique()
        parent_org_name = unique_orgs[0] if len(unique_orgs) > 0 else None
        
        # Group by the department name
        for dept_name, group in df.groupby(name_col):
            if pd.isna(dept_name) or str(dept_name).strip() == "":
                continue

            org_id = sanitize_id(str(dept_name))
            
            # Aggregate description from other columns for the Organization Narrative
            description_parts = []
            # We can use the first row's relation info or aggregate it
            # Let's aggregate unique values from relation_col
            relations = group[relation_col].dropna().unique()
            for rel in relations:
                if str(rel).strip():
                    description_parts.append(f"<li>{rel}</li>")
            
            full_description_html = f"<div xmlns=\"http://www.w3.org/1999/xhtml\"><h3>{dept_name}</h3><ul>{''.join(description_parts)}</ul></div>"
            
            # We assign a temporary ID here to allow linking (e.g. partOf).
            # The client uploader will strip this ID and replace it with a UUID for the transaction,
            # allowing the server to assign the final ID.
            org = Organization(
                id=org_id,
                name=str(dept_name),
                active=True,
                text=Narrative(status="generated", div=full_description_html)
            )
            
            # Link to parent organization if this is not the parent
            # Logic: If this department name is different from the "main" one in the file, 
            # assume it's a sub-unit or related unit. 
            # However, the CSV structure usually implies the file IS about one main org.
            # But sometimes rows have different 'oddeleni'.
            # Let's keep the logic: if it's not the first one encountered, link it to the first one?
            # Or maybe we don't have enough info for hierarchy within a single file unless we infer it.
            # For now, let's stick to the previous logic:
            if parent_org_name and dept_name != parent_org_name:
                parent_id = sanitize_id(str(parent_org_name))
                org.partOf = Reference(reference=f"Organization/{parent_id}", display=str(parent_org_name))
            
            resources.append(org)
            
            # Create PlanDefinitions for Processes
            if process_col in df.columns:
                # Group by Process Name within the Organization
                for process_name, process_group in group.groupby(process_col):
                    if pd.isna(process_name) or str(process_name).strip() == "":
                        continue
                        
                    process_id = sanitize_id(f"{dept_name}-{process_name}")
                    
                    # Aggregate descriptions for the process
                    process_descriptions = []
                    for _, row in process_group.iterrows():
                        if desc_col in row and pd.notna(row[desc_col]) and str(row[desc_col]).strip():
                            process_descriptions.append(str(row[desc_col]))
                    
                    full_process_desc = "; ".join(process_descriptions)
                    
                    # Ensure we have a valid name and ID
                    sanitized_name = sanitize_id(str(process_name))
                    if not sanitized_name:
                        print(f"Skipping process with invalid name: '{process_name}' in DataFrame: '{df_name}'")
                        continue
                        
                    plan_def = PlanDefinition(
                        id=process_id,
                        status="active",
                        name=sanitized_name,
                        title=str(process_name),
                        publisher=str(dept_name),
                        description=full_process_desc
                    )
                    
                    resources.append(plan_def)
            
    return resources

if __name__ == "__main__":
    dataframes = load_dataframes()
    print(f"Loaded {len(dataframes)} DataFrames")
    
    resources = create_fhir_resources(dataframes)
    print(f"Created {len(resources)} FHIR resources")
    
    # Example: Print the first one
    if resources:
        print(resources[0].model_dump_json(indent=2))

