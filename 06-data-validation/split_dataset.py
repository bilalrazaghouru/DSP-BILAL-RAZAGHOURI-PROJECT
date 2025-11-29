import pandas as pd
import os
import argparse

def split_dataset(dataset_path, output_folder, num_files):
    """Split dataset into multiple files"""
    
    # Read dataset
    df = pd.read_csv(dataset_path)
    total_rows = len(df)
    rows_per_file = total_rows // num_files
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    # Split and save
    for i in range(num_files):
        start_idx = i * rows_per_file
        end_idx = start_idx + rows_per_file if i < num_files - 1 else total_rows
        
        chunk = df.iloc[start_idx:end_idx]
        filename = f"candidates_batch_{i+1:03d}.csv"
        filepath = os.path.join(output_folder, filename)
        
        chunk.to_csv(filepath, index=False)
        print(f"✅ Created {filename} with {len(chunk)} rows")
    
    print(f"\n✅ Successfully split {total_rows} rows into {num_files} files")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Path to dataset")
    parser.add_argument("--output", required=True, help="Output folder path")
    parser.add_argument("--num-files", type=int, default=50, help="Number of files to create")
    
    args = parser.parse_args()
    split_dataset(args.dataset, args.output, args.num_files)
