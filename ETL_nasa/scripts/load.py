import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_to_supabase(csv_path):

    df = pd.read_csv(csv_path)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict("records")

    response = supabase.table("nasa_apod").insert(records).execute()

    print(f"Uploaded {len(records)} records to Supabase.")
    return response

if __name__ == "__main__":
    staged_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "staged"))
    latest_csv = sorted([f for f in os.listdir(staged_dir) if f.endswith(".csv")])[-1]
    load_to_supabase(os.path.join(staged_dir, latest_csv))