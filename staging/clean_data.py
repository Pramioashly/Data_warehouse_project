# staging/clean_data.py
import os
import pandas as pd

input_dir = './data'
output_dir = "./cleaned"
os.makedirs(output_dir, exist_ok=True)

for file in os.listdir(input_dir):
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join(input_dir, file))
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        df.dropna(how="all", inplace=True)  # remove empty rows
        df.to_csv(os.path.join(output_dir, file), index=False)
        print(f"Cleaned: {file}")
