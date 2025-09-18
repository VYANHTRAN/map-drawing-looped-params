import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from src import config
from src.config import *
from src.utils import *
from src.requests_manager import RequestManager

class ScrapingPipeline():
    def __init__(self):
        self.codes = load_codes(CODE_FILE_PATH)
        self.progress = load_progress(PROGRESS_FILE)
        self.request_manager = RequestManager()
        self.related_data_cache = {
            "daqh": {},
            "phankhu": {},
            "kientruc": {}
        }
        # This dictionary maps the key in the general data (e.g., 'MaDuAnQH')
        # to the prefix we'll use for the enriched fields (e.g., 'DuAnQH').
        self.related_indices = {
            "daqh": ("MaDuAnQH", "DuAnQH"),
            "phankhu": ("MaQHPhanKhu", "QHPhanKhu"),
            "kientruc": ("MaKVKT", "KVKT")
        }

    def _fetch_and_process_batch(self, batch_tasks):
        """
        Fetches general data for a batch of tasks concurrently and enriches them.
        """
        if not batch_tasks:
            return []

        # 1. Fetch general data concurrently
        fetched_records = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_task = {
                executor.submit(self.request_manager.fetch_general_data, soTo, soThua, phuongXa): (phuongXa, soTo, soThua)
                for phuongXa, soTo, soThua in batch_tasks
            }
            for future in as_completed(future_to_task):
                record = future.result()
                if record:
                    fetched_records.append(record)

        if not fetched_records:
            return []
            
        # 2. Collect all unique, uncached related info codes
        new_related_codes = {"daqh": set(), "phankhu": set(), "kientruc": set()}
        for record in fetched_records:
            for info_type, (code_key, _) in self.related_indices.items():
                code = record.get(code_key)
                if code and code not in self.related_data_cache[info_type]:
                    new_related_codes[info_type].add(code)

        # 3. Fetch all new related info concurrently and store in cache
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_code = {
                executor.submit(self.request_manager.fetch_related_info, info_type, code): (info_type, code)
                for info_type, codes in new_related_codes.items() for code in codes
            }
            for future in as_completed(future_to_code):
                info_type, code = future_to_code[future]
                related_info = future.result()
                if related_info:
                    self.related_data_cache[info_type][code] = related_info

        # 4. ENRICHMENT STEP: Merge related data into the original records
        enriched_records = []
        for record in fetched_records:
            # This loop modifies the 'record' dictionary in-place by adding new keys
            for info_type, (code_key, record_key_prefix) in self.related_indices.items():
                code = record.get(code_key)
                if code and code in self.related_data_cache[info_type]:
                    related_info = self.related_data_cache[info_type][code]
                    if isinstance(related_info, dict):
                        for k, v in related_info.items():
                            # This line adds the related data to the main record dictionary
                            record[f"{record_key_prefix}_{k}"] = v
                    elif isinstance(related_info, list) and related_info:
                        if isinstance(related_info[0], dict):
                            for k, v in related_info[0].items():
                                record[f"{record_key_prefix}_{k}"] = v
            # The fully merged record is added to our final list
            enriched_records.append(record)
        
        return enriched_records

    def run(self):
        soTo_start, soThua_start, phuongXa_index_start = self.progress

        try:
            for idx in range(phuongXa_index_start, len(self.codes)):
                if config.STOP_SCRAPE:
                    print("Stop signal received. Halting pipeline.")
                    break

                code = self.codes[idx]
                
                soTo = soTo_start if idx == phuongXa_index_start else 1
                soThua = soThua_start if idx == phuongXa_index_start and soTo == soTo_start else 1

                print(f"Processing phuongXa: {code} ({idx + 1}/{len(self.codes)}). Starting at soTo {soTo}, soThua {soThua}")

                for soto_loop in tqdm(range(soTo, MAX_SOTO + 1), desc=f"phuongXa {code}"):
                    if config.STOP_SCRAPE:
                        break
                    
                    start_thua = soThua if soto_loop == soTo else 1

                    batch_tasks = []
                    for sothua_loop in tqdm(range(start_thua, MAX_SOTHUA + 1), desc=f"soTo {soto_loop}"):
                        if config.STOP_SCRAPE:
                            break
                        
                        batch_tasks.append((code, soto_loop, sothua_loop))

                        if len(batch_tasks) >= BATCH_SIZE:
                            records = self._fetch_and_process_batch(batch_tasks)
                            if records:
                                dump_records_to_jsonl(records, OUTPUT_FILE)
                                print(f"Saved {len(records)} records. Last processed: {code}, soTo {soto_loop}, soThua {sothua_loop}")
                            
                            batch_tasks = []
                            save_progress(PROGRESS_FILE, soto_loop, sothua_loop + 1, idx)
                    
                    if not config.STOP_SCRAPE and batch_tasks:
                        records = self._fetch_and_process_batch(batch_tasks)
                        if records:
                            dump_records_to_jsonl(records, OUTPUT_FILE)
                            print(f"Saved {len(records)} records. Last processed: {code}, soTo {soto_loop}, soThua {MAX_SOTHUA}")

                    save_progress(PROGRESS_FILE, soto_loop + 1, 1, idx)
                    soThua = 1
                
                soTo_start, soThua_start = 1, 1

            if not config.STOP_SCRAPE:
                print("Finished scraping all ward codes!")

        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Exiting.")

def main():
    """Runs the scraping pipeline."""
    pipeline = ScrapingPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()