import os

# -- Location and API Configuration --
LOCATION = "Thành phố Đà Nẵng"

API_URL = {
    'thua': 'https://thongtinquyhoachxaydung.danang.gov.vn/api/ranh-gioi-qh/tim-theo-to-thua',
    'daqh': 'https://thongtinquyhoachxaydung.danang.gov.vn/api/duanqh/tim-theo-ma',
    'phankhu': 'https://thongtinquyhoachxaydung.danang.gov.vn/api/duanqh/tim-theo-ma-phan-khu',
    'kientruc': 'https://thongtinquyhoachxaydung.danang.gov.vn/api/kien-truc/tim-theo-ma'
}

# -- Domain File -- 
CODE_FILE_PATH = 'domains/phuongXa.xlsx'

# -- Output Directory --
OUTPUT_DIR = 'output'
OUTPUT_FILE = f"{OUTPUT_DIR}/planning_data.jsonl"
PROGRESS_FILE = f"{OUTPUT_DIR}/progress.json"

# -- Scraping Parameters --
MAX_SOTO = 400 
MAX_SOTHUA = 1000
BATCH_SIZE = 100 

# -- Global Request Settings --
STOP_SCRAPE = False