import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()

# Get DB connection details from environment variables
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")
DB_CHANNELBINDING = os.environ.get("DB_CHANNELBINDING", "disable")

# Alternatively, use a full connection string
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")

def get_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        if DB_CONNECTION_STRING:
            conn = psycopg2.connect(DB_CONNECTION_STRING)
        else:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                sslmode=DB_SSLMODE,
                channel_binding=DB_CHANNELBINDING
            )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def create_contacts_table_if_not_exists(conn):
    """Create a table for contacts if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS organization_contacts (
        id SERIAL PRIMARY KEY,
        department_name TEXT NOT NULL,
        email TEXT,
        phone_number TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(department_name)
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        conn.commit()
        print("Table 'organization_contacts' checked/created.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()

def upsert_contacts(conn, df: pd.DataFrame):
    """
    Insert or update contacts from the DataFrame into the database.
    Assumes DataFrame has columns: 'oddeleni', 'email', 'telephone_number'.
    """
    upsert_query = """
    INSERT INTO organization_contacts (department_name, email, phone_number, updated_at)
    VALUES (%s, %s, %s, NOW())
    ON CONFLICT (department_name) 
    DO UPDATE SET 
        email = EXCLUDED.email,
        phone_number = EXCLUDED.phone_number,
        updated_at = NOW();
    """
    
    inserted_count = 0
    try:
        with conn.cursor() as cur:
            # Filter for unique departments in this file to avoid duplicates within the batch
            # (though ON CONFLICT handles it, it's cleaner)
            unique_contacts = df[['oddeleni', 'email', 'telephone_number']].drop_duplicates(subset=['oddeleni'])
            
            for _, row in unique_contacts.iterrows():
                dept = row['oddeleni']
                email = row['email']
                phone = row['telephone_number']
                
                if pd.isna(dept) or not str(dept).strip():
                    continue
                    
                cur.execute(upsert_query, (dept, email, phone))
                inserted_count += 1
                
        conn.commit()
        print(f"Upserted {inserted_count} contacts.")
    except Exception as e:
        print(f"Error upserting contacts: {e}")
        conn.rollback()

def upload_contacts_to_postgres(directory: str = "current"):
    """Main function to upload contacts from CSVs in the directory to Postgres."""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return

    conn = get_connection()
    if not conn:
        print("Could not connect to database. Please check your .env variables.")
        return

    try:
        create_contacts_table_if_not_exists(conn)
        
        for filename in os.listdir(directory):
            if not filename.endswith(".csv"):
                continue
                
            file_path = os.path.join(directory, filename)
            print(f"Processing {filename}...")
            
            try:
                df = pd.read_csv(file_path)
                
                # Check if required columns exist
                required_cols = ['oddeleni', 'email', 'telephone_number']
                if all(col in df.columns for col in required_cols):
                    upsert_contacts(conn, df)
                else:
                    print(f"Skipping {filename}: Missing required columns {required_cols}")
                    
            except Exception as e:
                print(f"Error reading {filename}: {e}")
                
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    upload_contacts_to_postgres()
