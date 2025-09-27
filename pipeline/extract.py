# pipeline/extract.py
import requests
import time

"""
This module is responsible for connecting to the Open Brewery DB API and
retrieving brewery data page by page. It implements pagination with retries
and exponential backoff to ensure resilience against temporary network or API
errors.
"""

API_BASE = "https://api.openbrewerydb.org/v1"
PER_PAGE = 200        # Maximum page size supported by the API
TIMEOUT = 30          # Request timeout in seconds

def fetch_page(page: int, retries: int = 3, backoff_factor: float = 1.5):
    """
    Fetches a single page of breweries and returns it as a list of dictionaries.
    Includes automatic retries with exponential backoff in case of transient errors.
    """
    for attempt in range(retries):
        try:
            url = f"{API_BASE}/breweries"
            params = {"page": page, "per_page": PER_PAGE}
            resp = requests.get(url, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = backoff_factor ** attempt
                print(f"[WARN] Falha ao buscar página {page}, tentativa {attempt+1}/{retries}. Retentando em {wait:.1f}s...")
                time.sleep(wait)
            else:
                print(f"[ERROR] Falha definitiva ao buscar página {page}: {e}")
                raise

def paginate_breweries(max_pages: int = 1000, sleep_secs: float = 0.1):
    """
    Iterates through all brewery pages from the API until no more results
    are returned or the maximum page limit is reached. Yields one page at a time.
    """
    for page in range(1, max_pages + 1):
        rows = fetch_page(page)
        if not rows:
            break
        yield rows
        time.sleep(sleep_secs)  # pequena pausa para não sobrecarregar a API
