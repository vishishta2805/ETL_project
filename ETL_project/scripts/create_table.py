import os
from dotenv import load_dotenv
from supabase import create_client


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
# CREATE TABLE IF NOT EXISTS
# ------------------------------------------------------
def create_table_if_not_exists():
    try:
        supabase = get_supabase_client()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS iris_data (
            id BIGSERIAL PRIMARY KEY,
            sepal_length FLOAT,
            sepal_width FLOAT,
            petal_length FLOAT,
            petal_width FLOAT,
            species TEXT,
            sepal_ratio FLOAT,
            petal_ratio FLOAT,
            is_petal_long INTEGER
        );
        """

        try:
            supabase.rpc("execute_sql", {"query": create_table_sql}).execute()
            print("Table 'iris_data' created or already exists.")
        except Exception as e:
            print(f"RPC execute_sql failed: {e}")
            print("Make sure you created the SQL function in Supabase.")

    except Exception as e:
        print(f"Error creating table: {e}")
