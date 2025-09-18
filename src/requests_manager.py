import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
from urllib.parse import urlparse

from . import config
from .config import *


class RequestManager:
    """
    Manages sending requests using the `requests` library with
    a persistent session for performance and robustness.
    """

    def __init__(self, retries=3, backoff_factor=1, timeout=20):
        self.timeout = timeout
        self.ua = UserAgent()
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _generate_browser_headers(self, url):
        """
        Generate headers that mimic a real Chrome browser.
        """
        user_agent = self.ua.random
        chrome_version = "100"
        if "Chrome/" in user_agent:
            try:
                chrome_version = user_agent.split("Chrome/")[1].split(".")[0]
            except IndexError:
                pass
        sec_ch_ua = (
            f'"Not)A;Brand";v="8", "Chromium";v="{chrome_version}", '
            f'"Google Chrome";v="{chrome_version}"'
        )
        parsed_url = urlparse(url)
        origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
        referer = f"{origin}/"
        return {
            "User-Agent": user_agent, "Accept": "application/json, text/plain, */*",
            "Referer": referer, "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
            "Accept-Encoding": "gzip, deflate, br", "Connection": "keep-alive",
            "Sec-Ch-Ua": sec_ch_ua, "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin", "Origin": origin
        }

    def fetch_api(self, url, params=None):
        """
        Fetch data from the API with retries, timeout, and critical error handling.
        """
        if config.STOP_SCRAPE:
            return None

        headers = self._generate_browser_headers(url)

        try:
            response = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
            
            if response.status_code >= 400:
                print(f"Critical Error: Received status code {response.status_code} from {url}. Halting pipeline.")
                config.STOP_SCRAPE = True
                return None
            
            if not response.text.strip() or response.text.strip() == "[]":
                return None
            
            return response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def fetch_general_data(self, soTo, soThua, phuongXa):
        """
        Fetch general data for a specific land plot (thua).
        """
        params = {"soTo": soTo, "soThua": soThua, "phuongXa": phuongXa}
        data = self.fetch_api(API_URL["thua"], params=params)
        return data[0] if data else None

    def fetch_related_info(self, info_type, code):
        """
        Fetch related project data (e.g., daqh, phankhu, kientruc).
        """
        if info_type not in API_URL:
            raise ValueError(f"Invalid info_type: {info_type}. Must be one of {list(API_URL.keys())}.")

        url = API_URL[info_type]
        params = {"code": code}
        return self.fetch_api(url, params=params)