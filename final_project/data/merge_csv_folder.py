import os
import pandas as pd
import argparse


def merge_csvs(input_folder: str, output_file: str) -> None:
    csv_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith('.csv')]

    if not csv_files:
        raise ValueError(f"No CSV files found in the folder: {input_folder}")

    dataframes = [pd.read_csv(csv_file) for csv_file in csv_files]
    merged_df = pd.concat(dataframes, ignore_index=True)

    merged_df.to_csv(output_file, index=False)
    print(f"Merged {len(csv_files)} CSV files into {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Merge all CSV files in a folder (from Kafka) into one CSV file")
    parser.add_argument("--input_folder", required=True, help="Path to the folder containing CSV files.")
    parser.add_argument("--output_file", required=True, help="Path to save the merged CSV file.")

    args = parser.parse_args()

    merge_csvs(args.input_folder, args.output_file)
