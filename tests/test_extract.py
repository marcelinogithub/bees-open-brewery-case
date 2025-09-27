# tests/test_extract.py
from pipeline.extract import paginate_breweries

def test_paginate_breweries_returns_data():
    pages = list(paginate_breweries(max_pages=1))
    assert pages, "Nenhuma página retornada"
    assert isinstance(pages[0], list) and pages[0], "Primeira página vazia"
    first = pages[0][0]
    assert isinstance(first, dict)
    assert "name" in first
