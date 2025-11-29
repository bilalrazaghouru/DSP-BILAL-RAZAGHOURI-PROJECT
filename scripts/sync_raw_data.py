#!/usr/bin/env python3
"""Sync raw CSV files into Airflow data folder used by DAGs.

Usage: python scripts/sync_raw_data.py
"""
from pathlib import Path
import shutil
import sys


def main():
    repo = Path(__file__).resolve().parents[1]
    data_locations = [
        repo / 'data' / 'raw-data',
        repo / 'data' / 'raw-data' / 'data' / 'raw-data'
    ]
    dest = repo / '05-airflow-dags' / 'data' / 'raw-data'
    dest.mkdir(parents=True, exist_ok=True)

    moved = 0
    for src in data_locations:
        if src.exists() and src.is_dir():
            for csv in src.glob('*.csv'):
                dst_file = dest / csv.name
                if dst_file.exists():
                    print(f"Skipping existing {dst_file}")
                    continue
                print(f"Copying {csv} -> {dst_file}")
                shutil.copy2(csv, dst_file)
                moved += 1
    if moved == 0:
        print('No new files copied; check directories')
    else:
        print(f'{moved} files copied into {dest}')


if __name__ == '__main__':
    main()
