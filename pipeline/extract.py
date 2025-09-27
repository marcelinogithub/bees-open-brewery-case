# pipeline/extract.py
import requests
import time

# Este módulo é responsável por se conectar à Open Brewery DB e buscar dados
# de cervejarias página por página, com tentativas de retry em caso de falha.

API_BASE = "https://api.openbrewerydb.org/v1"
PER_PAGE = 200        # limite máximo suportado pela API
TIMEOUT = 30          # tempo máximo de espera em segundos

def fetch_page(page: int, retries: int = 3, backoff_factor: float = 1.5):
    """
    Busca uma única página de breweries e retorna como lista de dicts.
    Faz retries automáticos com backoff exponencial em caso de falhas temporárias.
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
    Faz chamadas consecutivas à API, página por página,
    até acabar os resultados ou atingir o limite de páginas.
    """
    for page in range(1, max_pages + 1):
        rows = fetch_page(page)
        if not rows:
            break
        yield rows
        time.sleep(sleep_secs)  # pequena pausa para não sobrecarregar a API
