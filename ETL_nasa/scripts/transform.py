import json
import pandas as pd
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
STAGED_DIR = Path(__file__).resolve().parent.parent / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

def transform_apod(raw_file):
    with open(raw_file, "r") as f:
        data = json.load(f)

    # Case 1: API returned a single object
    if isinstance(data, dict):
        records = [data]

    # Case 2: API returned a list of objects
    elif isinstance(data, list):
        records = data

    else:
        raise ValueError("Unexpected JSON format from NASA API")

    df = pd.DataFrame([
        {
            "date": item.get("date"),
            "title": item.get("title"),
            "explanation": item.get("explanation"),
            "media_type": item.get("media_type"),
            "url": item.get("url"),
            "hdurl": item.get("hdurl")
        }
        for item in records
    ])

    # Use the date of the first record for file name
    output_path = STAGED_DIR / f"apod_cleaned_{df.iloc[0]['date']}.csv"
    df.to_csv(output_path, index=False)

    print(f"🔄 Transformed → {output_path}")
    return output_path

if __name__ == "__main__":
    latest_raw = sorted(RAW_DIR.glob("*.json"))[-1]
    transform_apod(latest_raw)
