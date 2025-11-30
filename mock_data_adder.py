import pandas as pd
import os
import random
import string
from typing import Dict

try:
    from faker import Faker

    fake = Faker(["cs_CZ"])  # Use Czech locale if available
except ImportError:
    fake = None
    print("Faker library not found. Using simple random string generator.")


def generate_mock_email(name: str | None = None) -> str:
    """Generate a mock email address."""
    if fake:
        return f"{fake.user_name()}@fnbrno.cz"
    else:
        # Fallback if faker is not installed
        if name:
            # Create a simple email from the name
            clean_name = "".join(c for c in name if c.isalnum()).lower()
            domain = "fnbrno.cz"
            return f"{clean_name}@{domain}"
        else:
            random_str = "".join(random.choices(string.ascii_lowercase, k=8))
            return f"{random_str}@fnbrno.cz"


def generate_mock_phone() -> str:
    """Generate a mock Czech phone number."""
    # Generate 9 random digits
    if fake:
        digits = fake.numerify('#########')
    else:
        digits = "".join(random.choices(string.digits, k=9))
    
    return f"+420 {digits[:3]} {digits[3:6]} {digits[6:]}"


def add_mock_contacts_to_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'email' and 'telephone_number' columns with mock data. Remove old 'contact' column."""

    # Remove old contact column if present
    cols_to_drop = [col for col in df.columns if col.lower() == "contact"]
    if cols_to_drop:
        print(f"Removing old contact columns: {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)

    print("Overwriting/Adding email and telephone_number columns.")

    # We want to add the contact to the Organization.
    # The CSV structure is a bit complex (denormalized).
    # Usually, the 'oddeleni' (Department) is the Organization.
    # We should probably assign one contact email per Department.

    # Identify the department column
    # We can reuse the logic from cleaner.py or just look for common names
    dept_col = None
    possible_names = ["oddeleni", "Oddělení", "Útvar/oddělení", "Úsek/oddělení"]
    for col in possible_names:
        if col in df.columns:
            dept_col = col
            break

    if not dept_col:
        # If we can't find a department column, just add random emails to every row?
        # Or maybe just add a column 'contact' and fill it.
        print("Could not identify department column. Adding random data to all rows.")
        df["email"] = [generate_mock_email() for _ in range(len(df))]
        df["telephone_number"] = [generate_mock_phone() for _ in range(len(df))]
        return df

    # Generate a map of Department -> Email/Phone to ensure consistency
    unique_depts = df[dept_col].unique()
    dept_emails = {dept: generate_mock_email(str(dept)) for dept in unique_depts}
    dept_phones = {dept: generate_mock_phone() for dept in unique_depts}

    # Map the emails/phones to the new columns
    df["email"] = df[dept_col].map(dept_emails)
    df["telephone_number"] = df[dept_col].map(dept_phones)

    return df


def process_files(directory: str):
    """Process all CSV files in the directory."""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return

    for filename in os.listdir(directory):
        if not filename.endswith(".csv"):
            continue

        file_path = os.path.join(directory, filename)
        print(f"Processing {filename}...")

        try:
            df = pd.read_csv(file_path)
            original_cols = list(df.columns)

            df = add_mock_contacts_to_dataframe(df)

            df.to_csv(file_path, index=False)
            print(f"Updated mock contacts in {filename}")

        except Exception as e:
            print(f"Error processing {filename}: {e}")


if __name__ == "__main__":
    # Default to 'from_supabase' if it exists, else 'docs'
    target_dir = "from_supabase" if os.path.exists("from_supabase") else "docs"
    print(f"Adding mock data to files in {target_dir}...")
    process_files(target_dir)
