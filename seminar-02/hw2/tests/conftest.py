import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / 'app'))
sys.path.insert(0, str(root / 'verify'))
