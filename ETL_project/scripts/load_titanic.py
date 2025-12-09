import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from create_table_titanic import create_table_if_not_exists  # Make sure this file exists!


# ------------------------------------------------------
# INITIALIZE SUPABASE CLIENT
# ------------------------------------------------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("supabase_url")
    key = os.getenv("supabase_key")

    if not url or not key:
        raise ValueError("Supabase URL or Key not found in environment variables.")

    return create_client(url, key)


# ------------------------------------------------------
# LOAD TITANIC DATA INTO SUPABASE IN BATCHES
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "titanic_data"):

    try:
        # Convert path to absolute if needed
        if not os.path.isabs(staged_path):
            staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

        print(f"📄 Looking for staged file at: {staged_path}")

        if not os.path.exists(staged_path):
            print(f" Staged file not found at {staged_path}")
            print("Run transform_titanic.py first.")
            return

        # Connect to Supabase
        supabase = get_supabase_client()

        df = pd.read_csv(staged_path)
        total_rows = len(df)
        batch_size = 20  # safe batch size

        print(f" Loading {total_rows} rows into '{table_name}'...\n")

        # Insert in batches
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size].copy()

            # Supabase cannot handle NaN → convert to None
            batch = batch.where(pd.notnull(batch), None)

            records = batch.to_dict("records")

            try:
                supabase.table(table_name).insert(records).execute()
                end = min(i + batch_size, total_rows)
                print(f"Inserted rows {i+1} — {end} of {total_rows}")
            except Exception as e:
                print(f"Error in batch {i//batch_size + 1}: {e}")
                continue

        print(f"\n🎉 Finished loading Titanic data into '{table_name}'!")

    except Exception as e:
        print(f" Error loading data: {e}")


# ------------------------------------------------------
# MAIN BLOCK
# ------------------------------------------------------
if __name__ == "__main__":
    print("🚀 Starting Titanic Load Step...\n")

    staged_csv_path = os.path.join("..", "data", "staged", "titanic_transformed.csv")

    create_table_if_not_exists()
    load_to_supabase(staged_csv_path)
