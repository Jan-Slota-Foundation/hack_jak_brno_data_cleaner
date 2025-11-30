import pandas as pd
import ftfy
import os
from typing import Dict, List

def fix_encoding(text):
    """Fix common encoding issues using ftfy and manual replacements."""
    if isinstance(text, str):
        # Fix general encoding issues
        text = ftfy.fix_text(text)
        
        # Replace non-breaking space (U+00A0) with regular space
        text = text.replace('\u00a0', ' ')
        
        # Replace en-dash (U+2013) with hyphen (U+002D)
        text = text.replace('\u2013', '-')
        
        # Replace em-dash (U+2014) with hyphen (U+002D)
        text = text.replace('\u2014', '-')

        # Remove pipe characters
        text = text.replace('|', ' ')
        
        return text.strip()
    return text

def clean_dataframe(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Standardize DataFrame columns to:
    1. oddeleni
    2. proces
    3. popis_procesu
    4. vazba_na_org_rad
    """
    
    # Define column mappings based on the filename or content
    # Map: {Standard Name: [Possible Source Names]}
    column_map = {
        "oddeleni": ["oddeleni", "Oddělení", "Útvar/oddělení", "Úsek/oddělení"],
        "proces": ["proces", "Název procesu", "Procesy", "Proces"],
        "popis_procesu": ["popis_procesu", "Popis procesu", "Popis procesu v organizačním řádu OPZ"],
        "vazba_na_org_rad": ["vazba_na_org_rad", "Vazba na Organizační řád IO"]
    }

    new_df = pd.DataFrame()

    # Preserve other columns (like email, telephone_number) if they exist
    preserved_cols = ["email", "telephone_number"]
    for col in preserved_cols:
        if col in df.columns:
            new_df[col] = df[col]

    # 1. Oddeleni (Department)
    found_oddeleni = False
    for source_col in column_map["oddeleni"]:
        if source_col in df.columns:
            new_df["oddeleni"] = df[source_col]
            found_oddeleni = True
            break
    if not found_oddeleni:
        # Fallback or error handling if needed
        new_df["oddeleni"] = None

    # 2. Proces (Process Name)
    found_proces = False
    for source_col in column_map["proces"]:
        if source_col in df.columns:
            new_df["proces"] = df[source_col]
            found_proces = True
            break
    if not found_proces:
        new_df["proces"] = None

    # 3. Popis procesu (Description)
    # Some files might have multiple description columns, we can merge them or pick the best one
    # For now, we pick the first match, but we can also append others if requested
    found_popis = False
    for source_col in column_map["popis_procesu"]:
        if source_col in df.columns:
            new_df["popis_procesu"] = df[source_col]
            found_popis = True
            break
    
    # Special handling for OPZ.csv which has "Popis na základě rozhovoru"
    if "Popis na základě rozhovoru" in df.columns:
        if "popis_procesu" in new_df.columns:
             new_df["popis_procesu"] = new_df["popis_procesu"].fillna("") + " " + df["Popis na základě rozhovoru"].fillna("")
        else:
             new_df["popis_procesu"] = df["Popis na základě rozhovoru"]
             found_popis = True

    if not found_popis:
        new_df["popis_procesu"] = None

    # 4. Vazba na org rad (Mandate/Relation)
    found_vazba = False
    for source_col in column_map["vazba_na_org_rad"]:
        if source_col in df.columns:
            new_df["vazba_na_org_rad"] = df[source_col]
            found_vazba = True
            break
    if not found_vazba:
        new_df["vazba_na_org_rad"] = None

    # Fill NaNs with empty strings for cleaner output
    new_df = new_df.fillna("")

    # Replace "?" with empty strings
    new_df = new_df.replace("?", "")

    # Remove rows where only 1 or fewer columns have non-empty values
    # We check against empty strings since we just filled NaNs
    # Convert to boolean (True if not empty), sum across columns (axis=1)
    # Note: We should probably only count the main columns for this check, not the preserved ones
    check_cols = ["oddeleni", "proces", "popis_procesu", "vazba_na_org_rad"]
    existing_check_cols = [c for c in check_cols if c in new_df.columns]
    
    non_empty_counts = (new_df[existing_check_cols] != "").sum(axis=1)
    new_df = new_df[non_empty_counts > 0] # Keep if at least one main column has data

    # Fix encoding issues in all string columns
    for col in new_df.columns:
        if new_df[col].dtype == object:  # Check if column is of object type (usually strings)
            new_df[col] = new_df[col].apply(fix_encoding)

    # Enforce column order
    desired_order = ["oddeleni", "proces", "popis_procesu", "vazba_na_org_rad", "email", "telephone_number"]
    
    # Add missing columns
    for col in desired_order:
        if col not in new_df.columns:
            new_df[col] = ""
            
    # Reorder
    new_df = new_df[desired_order]

    return new_df

def clean_all_dataframes(dataframes: Dict[str, pd.DataFrame], output_dir: str = "cleaned") -> Dict[str, pd.DataFrame]:
    """
    Clean all dataframes and save them to the specified output directory.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    cleaned_dfs = {}
    for name, df in dataframes.items():
        print(f"Cleaning {name}...")
        cleaned_df = clean_dataframe(df, name)
        cleaned_dfs[name] = cleaned_df
        
        # Save to CSV
        output_path = os.path.join(output_dir, f"{name}.csv")
        cleaned_df.to_csv(output_path, index=False)
        print(f"Saved cleaned {name} to {output_path}")
        
    return cleaned_dfs
