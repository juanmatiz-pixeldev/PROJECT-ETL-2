"""Copia la evidencia de un lote (manifiestos, resultados GX, suites, eventos) a docs/evidence/<batch_id>/.
Uso: python scripts/export_evidence.py <batch_id>"""
import shutil
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
b = sys.argv[1]
dest = root / "docs" / "evidence" / b
for src in [root / "data/raw/primary" / b / "manifest.json", root / "data/raw/api" / b / "manifest.json",
            root / "data/prepared" / b / "manifest.json", root / "data/logs" / b / "events.jsonl"]:
    if src.exists():
        t = dest / f"{src.parent.parent.name}_{src.parent.name}_{src.name}".replace("data_", "")
        t.parent.mkdir(parents=True, exist_ok=True); shutil.copy(src, t)
vd = root / "data/validation" / b
if vd.exists():
    shutil.copytree(vd, dest / "validation", dirs_exist_ok=True)
print("evidencia ->", dest)
