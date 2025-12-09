import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Initialize supabase
load_dotenv()
SUPABASE = create_client(os.getenv("supabase_url"), os.getenv("supabase_key"))

def load_to_supabase():
    # Load cleaned csv
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} not found. Please run the transform script first.")

    df = pd.read_csv(csv_path)

    # Convert timestamps to strings
    df["time"] = pd.to_datetime(df["time"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime('%Y-%m-%d %H:%M:%S')

    batch_size = 20

    for i in range(0, len(df), batch_size):

        batch = df.iloc[i:i+batch_size].fillna("NULL").to_dict("records")

        values = [
            f"('{r['time']}', {r.get('temperature_C','NULL')}, "
            f"{r.get('humidity_percent','NULL')}, {r.get('wind_speed_kmph','NULL')}, "
            f"'{r.get('city','Hyderabad')}', '{r['extracted_at']}')"
            for r in batch
        ]

        insert_sql = f"""
        INSERT INTO weather_data 
        (time, temperature_c, humidity, wind_speed_kmph, city, extracted_at)
        VALUES {', '.join(values)};
        """

        # Remote Procedure Call: Run SQL inside Supabase
        SUPABASE.rpc("execute_sql", {"sql": insert_sql}).execute()

        print(f"Inserted batch {i//batch_size + 1} with {len(batch)} records.")
        time.sleep(0.5)  # avoid rate limiting

    print("Data loading completed.")

if __name__ == "__main__":
    load_to_supabase()
