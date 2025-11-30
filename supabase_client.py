import requests
import os
import uuid
from typing import List
from dotenv import load_dotenv
from supabase import create_client, Client
from fhir.resources.organization import Organization
from fhir.resources.domainresource import DomainResource
from fhir.resources.bundle import Bundle, BundleEntry, BundleEntryRequest

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
fhir_server = os.environ.get("FHIR_SERVER")

BUCKET = "zhodnoceni_procesu"
FILES = ["CI", "EO", "IO", "OHTS", "OIAK", "OPV", "OPZ", "reditel", "UVV"]

if not url or not key:
    raise ValueError(
        "SUPABASE_URL and SUPABASE_KEY must be set in environment variables"
    )

supabase: Client = create_client(url, key)


def download_file(path: str) -> bytes:
    """Download a file from the Supabase storage bucket."""
    response = supabase.storage.from_(BUCKET).download(path)
    return response


def download_all_files(output_dir: str = "docs"):
    """Download all configured files to the specified directory."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for name in FILES:
        file_name = f"{name}.csv"
        print(f"Downloading {file_name}...")
        try:
            content = download_file(file_name)
            with open(os.path.join(output_dir, file_name), "wb") as f:
                f.write(content)
        except Exception as e:
            print(f"Failed to download {file_name}: {e}")


def upload_cleaned_files(input_dir: str = "cleaned"):
    """Upload all files from the cleaned directory to Supabase, overwriting existing ones."""
    if not os.path.exists(input_dir):
        print(f"Directory {input_dir} does not exist.")
        return

    for filename in os.listdir(input_dir):
        if not filename.endswith(".csv"):
            continue

        file_path = os.path.join(input_dir, filename)
        print(f"Uploading {filename}...")

        try:
            with open(file_path, "rb") as f:
                # Using upsert=True to overwrite
                supabase.storage.from_(BUCKET).upload(
                    path=filename, file=f, file_options={"upsert": "true"}
                )
            print(f"Successfully uploaded {filename}")
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")


def create_resource_on_server(resource: Organization):
    """Create an Organization resource on the FHIR server."""
    url = f"http://{fhir_server}/Organization"
    headers = {"Content-Type": "application/fhir+json"}
    response = requests.post(url, data=resource.model_dump_json(), headers=headers)
    response.raise_for_status()
    return response.json()


def upload_bundle_to_fhir_server(resources: List[DomainResource]):
    """
    Uploads FHIR resources to a FHIR server using a Transaction Bundle.
    Uses POST (create) interaction with UUIDs for internal references.
    """
    if not fhir_server:
        print("FHIR_SERVER environment variable not set.")
        return

    base_url = f"http://{fhir_server}"

    # Map original IDs to UUIDs for bundle references
    id_map = {r.id: str(uuid.uuid4()) for r in resources if r.id}

    entries = []
    for resource in resources:
        resource_type = resource.__resource_type__
        original_id = resource.id

        # Update references for Organization.partOf
        if isinstance(resource, Organization) and getattr(resource, "partOf", None):
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
