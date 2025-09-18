import logging
import json
import pandas as pd
import os

from .config import *

def load_progress(file_path=PROGRESS_FILE):
    """
    Loads previous progress from a JSON file.
    Returns (so_to, so_thua, phuong_xa_index).
    """
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                progress = json.load(f)
                return progress.get('so_to', 1), progress.get('so_thua', 1), progress.get('phuong_xa_index', 0)
        except (json.JSONDecodeError, FileNotFoundError):
             print("Progress file is corrupted or empty. Starting from scratch.")
             return 1, 1, 20194
    else:
        logging.info("No previous progress file found. Starting from scratch.")
        return 1, 1, 20194

def save_progress(file_path, soTo, soThua, phuongXa_index):
    """
    Saves current progress to a JSON file.
    """
    progress = {
        'so_to': soTo,
        'so_thua': soThua,
        'phuong_xa_index': phuongXa_index
    }
    output_dir = os.path.dirname(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(file_path, 'w') as f:
        json.dump(progress, f, indent=4)

def load_codes(file_path=CODE_FILE_PATH):
    """
    Loads codes from an Excel file.
    """
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        if 'Tỉnh / Thành Phố' in df.columns and LOCATION in df['Tỉnh / Thành Phố'].values:
            codes = df[df['Tỉnh / Thành Phố'] == LOCATION]['Mã'].tolist()
            logging.info(f"Loaded {len(codes)} codes for {LOCATION}.")
            return codes
        else:
            logging.warning(f"Location '{LOCATION}' not found in the Excel file.")
            return []
    except Exception as e:
        logging.error(f"Failed to load codes from {file_path}. Error: {e}")
        return []

def dump_records_to_jsonl(records, output_file=OUTPUT_FILE):
    """
    Appends a list of records to a JSONL file.
    """
    if not records:
        return

    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_file, 'a', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')