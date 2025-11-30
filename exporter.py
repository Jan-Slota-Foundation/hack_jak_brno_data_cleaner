import pandas as pd
import os
import re
import uuid
import requests
from typing import Dict, List
from dotenv import load_dotenv
from supabase_client import FILES, download_all_files
from fhir.resources.organization import Organization
from fhir.resources.plandefinition import PlanDefinition
from fhir.resources.narrative import Narrative
from fhir.resources.reference import Reference
from fhir.resources.domainresource import DomainResource
from fhir.resources.contactpoint import ContactPoint
from fhir.resources.bundle import Bundle, BundleEntry, BundleEntryRequest

load_dotenv()
FHIR_SERVER = os.environ.get("FHIR_SERVER")

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
        email_col = "email"
        phone_col = "telephone_number"
        
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
            
            # Extract contact info from the first row of the group
            telecoms = []
            first_row = group.iloc[0]
            if email_col in first_row and pd.notna(first_row[email_col]):
                telecoms.append(ContactPoint(system="email", value=str(first_row[email_col])))
            if phone_col in first_row and pd.notna(first_row[phone_col]):
                telecoms.append(ContactPoint(system="phone", value=str(first_row[phone_col])))

            # We assign a temporary ID here to allow linking (e.g. partOf).
            # The client uploader will strip this ID and replace it with a UUID for the transaction,
            # allowing the server to assign the final ID.
            org = Organization(
                id=org_id,
                name=str(dept_name),
                active=True,
                text=Narrative(status="generated", div=full_description_html),
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

def upload_bundle_to_fhir_server(resources: List[DomainResource]):
    """
    Uploads FHIR resources to a FHIR server using a Transaction Bundle.
    Uses POST (create) interaction with UUIDs for internal references.
    """
    if not FHIR_SERVER:
        print("FHIR_SERVER environment variable not set.")
        return

    # Limit to 10 resources to avoid overloading
    if len(resources) > 10:
        print(f"Limiting upload to first 10 resources (out of {len(resources)}).")
        resources = resources[:10]

    base_url = f"{FHIR_SERVER}"

    # Map original IDs to UUIDs for bundle references
    id_map = {r.id: str(uuid.uuid4()) for r in resources if r.id}

    entries = []
    for resource in resources:
        resource_type = resource.__resource_type__
        original_id = resource.id
        
        if not original_id:
            continue

        # Update references for Organization.partOf
        if isinstance(resource, Organization) and resource.partOf:
            ref = resource.partOf.reference
            if ref and ref.startswith("Organization/"):
                ref_id = ref.split("/")[1]
                if ref_id in id_map:
                    resource.partOf.reference = f"urn:uuid:{id_map[ref_id]}"

        # Remove the temporary ID so the server assigns a new one
        resource.id = None

        # Create a BundleEntry with a POST request (create)
        request = BundleEntryRequest(method="POST", url=resource_type)

        entry = BundleEntry(
            fullUrl=f"urn:uuid:{id_map[original_id]}",
            resource=resource,
            request=request,
        )
        entries.append(entry)

    # Create the Bundle
    bundle = Bundle(type="transaction", entry=entries)

    print(f"Uploading transaction bundle with {len(entries)} entries to {base_url}...")

    headers = {"Content-Type": "application/fhir+json"}
    try:
        response = requests.post(
            base_url, data=bundle.model_dump_json(), headers=headers
        )
        response.raise_for_status()
        print("Upload successful!")
        # print(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Failed to upload bundle: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"Server response: {e.response.text}")

