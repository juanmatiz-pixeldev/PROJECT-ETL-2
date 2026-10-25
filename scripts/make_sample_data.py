"""Genera un CSV SINTETICO con el esquema del RNT (14 columnas) SOLO para pruebas de humo / CI.
No sustituye al dataset real de datos.gov.co (ver README). Uso:
    python scripts/make_sample_data.py data/source/rnt_sample.csv --rows 5000
"""
import argparse
import random
from pathlib import Path

import pandas as pd

MUNICIPIOS = [  # (codigo_dep, dep, codigo_mun, mun) -- codigos DIVIPOLA sin cero inicial, como en el CSV real
    (5, "ANTIOQUIA", 5001, "MEDELLIN"), (11, "BOGOTA D.C.", 11001, "BOGOTA D.C."),
    (76, "VALLE DEL CAUCA", 76001, "CALI"), (13, "BOLIVAR", 13001, "CARTAGENA"),
    (8, "ATLANTICO", 8001, "BARRANQUILLA"), (47, "MAGDALENA", 47001, "SANTA MARTA"),
    (68, "SANTANDER", 68001, "BUCARAMANGA"), (63, "QUINDIO", 63001, "ARMENIA"),
]
CATS = [("ESTABLECIMIENTOS DE ALOJAMIENTO", "HOTEL"), ("ESTABLECIMIENTOS DE ALOJAMIENTO", "HOSTAL"),
        ("AGENCIAS DE VIAJES", "AGENCIA OPERADORA"), ("ESTABLECIMIENTOS DE GASTRONOMIA", "RESTAURANTE")]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    rows = []
    for i in range(a.rows):
        dep, dname, mun, mname = rnd.choice(MUNICIPIOS)
        cat, sub = rnd.choice(CATS)
        rnt = 10000 + (i % 1200)
        rows.append({
            "CODIGO_RNT": rnt, "RAZON_SOCIAL_ESTABLECIMIENTO": None if i % 700 == 0 else f"ESTABLECIMIENTO {rnt}",
            "NIT": f"9{rnt:08d}-1", "CODIGO_DEPARTAMENTO": dep, "DEPARTAMENTO": dname,
            "CODIGO_MUNICIPIO": mun, "MUNICIPIO": mname, "CATEGORIA": cat, "SUB_CATEGORIA": sub,
            "ESTADO_RNT": "ACTIVO", "NUMERO_DE_HABITACIONES": rnd.randint(0, 80),
            "NUMERO_DE_CAMAS": rnd.randint(0, 200), "NUMERO_DE_EMPLEADOS": rnd.randint(0, 40),
            "AÑO": rnd.choice(range(2019, 2027)),
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["CODIGO_RNT", "CODIGO_MUNICIPIO", "SUB_CATEGORIA", "AÑO"])
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out, index=False)
    print(f"{len(df)} filas sinteticas -> {a.out}")


if __name__ == "__main__":
    main()
