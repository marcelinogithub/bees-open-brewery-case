# tests/conftest.py
import os, sys

# caminho da pasta "inner" onde estão pipeline/ e tests/
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(THIS_DIR, ".."))
# ex.: ...\bees-open-brewery-case\bees-open-brewery-case
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
