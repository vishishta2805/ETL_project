import os
from dotenv import load_dotenv
from supabase import create_client

def get_supabase_client():
    load_dotenv()
    url = os.getenv("supabase_url")
    key = os.getenv("supabase_key")

    if not url or not key:
        raise ValueError("Supabase URL or Key not found.")
    return create_client(url, key)

def create_table_if_not_exists():
    try:
        supabase = get_supabase_client()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS titanic_data (
            id BIGSERIAL PRIMARY KEY,
            survived FLOAT,
            pclass FLOAT,
            sex TEXT,
            age FLOAT,
            sibsp FLOAT,
            parch FLOAT,
            fare FLOAT,
            embarked TEXT,
            embark_town TEXT,
            deck TEXT,
            family_size INTEGER,
            is_alone INTEGER,
            sex_binary INTEGER
        );
        """

        try:
            supabase.rpc("execute_sql", {"query": create_table_sql}).execute()
            print(" Table 'titanic_data' created or already exists.")
        except Exception as e:
            print(f" RPC execute_sql failed: {e}")
            print("Create the RPC function manually in Supabase.")

    except Exception as e:
        print(f" Error creating table: {e}")


if __name__ == "__main__":
    create_table_if_not_exists()
