import os
import pandas as pd
import seaborn as sns


def transform_data(raw_path):
    # Base directory setup
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, 'data', 'staged')
    os.makedirs(staged_dir, exist_ok=True)

    # Load raw dataset
    df = pd.read_csv(raw_path)

    # -----------------------------
    # 1. HANDLE MISSING VALUES
    # -----------------------------
    df["age"] = df["age"].fillna(df["age"].median())
    df["fare"] = df["fare"].fillna(df["fare"].median())
    df["embarked"] = df["embarked"].fillna(df["embarked"].mode()[0])
    df["embark_town"] = df["embark_town"].fillna(df["embark_town"].mode()[0])
    df["deck"] = df["deck"].fillna("Unknown")

    # -----------------------------
    # 2. FEATURE ENGINEERING
    # -----------------------------
    df["family_size"] = df["sibsp"] + df["parch"] + 1
    df["is_alone"] = (df["family_size"] == 1).astype(int)
    df["sex_binary"] = (df["sex"] == "male").astype(int)

    # -----------------------------
    # 3. DROP ALL COLUMNS NOT USED IN SUPABASE
    # -----------------------------
    columns_to_keep = [
        "survived",
        "pclass",
        "sex",
        "age",
        "sibsp",
        "parch",
        "fare",
        "embarked",
        "embark_town",
        "deck",
        "family_size",
        "is_alone",
        "sex_binary"
    ]

    df = df[columns_to_keep]

    # -----------------------------
    # 4. SAVE TRANSFORMED FILE
    # -----------------------------
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f" Titanic transformed data saved to: {staged_path}")
    return staged_path


if __name__ == "__main__":
    # Extract file must already exist
    from extract_titanic import extract_data
    raw_path = extract_data()
    transform_data(raw_path)
