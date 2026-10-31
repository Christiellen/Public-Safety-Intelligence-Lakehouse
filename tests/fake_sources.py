"""Small Bronze tables that reproduce the layout and the known problems of each real source.

Municipalities: 8, including two homonyms ("Bom Jesus" in PI and RS) and one municipality created
after the IDHM census (5101837). Problems reproduced on purpose:
- SENATRAN: old spelling handled by an alias (MOJI MIRIM), a typo matched by fuzzy matching
  (SOROCAVA), thousands separator ("1.234"), float text ("60000.0"), a name too different to match,
  a municipality that does not exist, and "MUNICIPIO NAO INFORMADO";
- SINESP: events published per municipality and per state only, victims vs occurrences vs kg,
  a Federal Police row with every value null, a municipality that does not exist, a bad date;
- IBGE: "..." for a missing value; Ipeadata: a 6-digit municipality code and a state-level row.
"""

import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

MUNICIPALITIES = [
    ("3530805", "Mogi Mirim", "SP", 92_000),
    ("3545803", "Santa Bárbara d'Oeste", "SP", 195_000),
    ("3550308", "São Paulo", "SP", 11_900_000),
    ("3509502", "Campinas", "SP", 1_180_000),
    ("3552205", "Sorocaba", "SP", 730_000),
    ("2201903", "Bom Jesus", "PI", 25_000),
    ("4302303", "Bom Jesus", "RS", 11_800),
    ("5101837", "Boa Esperança do Norte", "MT", 7_000),
]
# CVLI victims per month and municipality (SINESP)
CVLI_PER_MONTH = {
    "Mogi Mirim": 1,
    "Santa Bárbara d'Oeste": 0,
    "São Paulo": 60,
    "Campinas": 9,
    "Sorocaba": 30,
    "Bom Jesus": 1,
}
NOT_INFORMED_CVLI_PER_MONTH = 2  # per state, victims without informed municipality
VEHICLE_CRIMES_PER_MONTH = 100 + 50  # per state: vehicle robbery + theft (state police)
COCAINE_KG_PER_MONTH = 12.5  # per state
MONTHS = [f"2025-{m:02d}-01 00:00:00" for m in range(1, 13)] + [f"2026-{m:02d}-01 00:00:00" for m in range(1, 10)]


def write_reference_bronze(ns: dict) -> None:
    """IBGE population and GDP and Ipeadata IDHM, written as notebooks 01 and 04 would (and logged)."""
    p2s, write, log = ns["pandas_to_spark_str"], ns["write_table"], ns["log_change"]
    labels = {code: f"{name} - {uf}" for code, name, uf, _ in MUNICIPALITIES}

    population, gdp, idhm = [], [], []
    for code, _, _, pop in MUNICIPALITIES:
        for year, factor in [(2021, 0.97), (2024, 0.995), (2025, 1.0)]:
            population.append(
                {"Município (Código)": code, "Município": labels[code], "Ano": str(year), "Valor": str(int(pop * factor))}
            )
        gdp.append({"Município (Código)": code, "Ano": "2023", "Valor": str(pop * 45)})
        if code == "5101837":
            continue  # created after the 2010 census
        for series, value in [("ADH_IDHM", 0.75), ("ADH_IDHM_E", 0.7), ("ADH_IDHM_L", 0.8), ("ADH_IDHM_R", 0.72)]:
            idhm.append({"SERCODIGO": series, "VALDATA": "2010-01-01T00:00:00-02:00",
                         "VALVALOR": value - (0.15 if code == "2201903" else 0), "NIVNOME": "Municípios",
                         "TERCODIGO": code[:6] if code == "3509502" else code})
    population.append({"Município (Código)": "5101837", "Município": "Nome Antigo - MT", "Ano": "2020", "Valor": "..."})
    idhm.append({"SERCODIGO": "ADH_IDHM", "VALDATA": "2010-01-01", "VALVALOR": 0.78, "NIVNOME": "Estados",
                 "TERCODIGO": "35"})
    for name, rows in [("bronze_ibge_population", population), ("bronze_ibge_gdp", gdp), ("bronze_ipea_idhm", idhm)]:
        write(p2s(pd.DataFrame(rows)), "bronze", name)
        log(name, ["all"], "fake", len(rows))


def sinesp_rows(year: str, extra_victims: int = 0) -> pd.DataFrame:
    """Rows of the SINESP file of one year. `extra_victims` adds homicide victims to São Paulo in the
    last month of the year file, to simulate a revised file."""
    def row(uf, municipality, event, date, agency="Estadual", total=None, victims=None, kg=None, weapon=None):
        return {"uf": uf, "municipio": municipality, "evento": event, "data_referencia": date, "agente": None,
                "arma": weapon, "faixa_etaria": None, "feminino": "0", "masculino": "0", "nao_informado": "0",
                "total_vitima": victims, "total": total, "total_peso": kg, "abrangencia": agency}

    months = [m for m in MONTHS if m.startswith(year)]
    rows = []
    for date in months:
        for name, victims in CVLI_PER_MONTH.items():
            if name == "São Paulo" and date == months[-1]:
                victims += extra_victims
            for uf in ["PI", "RS"] if name == "Bom Jesus" else ["SP"]:
                rows.append(row(uf, name, "Homicídio doloso", date, victims=str(victims)))
                rows.append(row(uf, name, "Feminicídio", date, victims="0"))
                rows.append(row(uf, name, "Mandado de prisão cumprido", date, total="3"))
        rows.append(row("MT", "Boa Esperança do Norte", "Homicídio doloso", date, victims="0"))
        for uf in ["SP", "PI", "RS", "MT"]:
            rows.append(row(uf, "NÃO INFORMADO", "Homicídio doloso", date, victims=str(NOT_INFORMED_CVLI_PER_MONTH)))
            rows.append(row(uf, None, "Roubo de veículo", date, total="100"))
            rows.append(row(uf, None, "Furto de veículo", date, total="50"))
            rows.append(row(uf, None, "Furto de veículo", date, agency="Polícia Federal"))  # all null
            rows.append(row(uf, None, "Apreensão de Cocaína", date, kg=str(COCAINE_KG_PER_MONTH)))
            for weapon in ["Pistola", "Revólver", "Fuzil"]:
                rows.append(row(uf, None, "Arma de Fogo Apreendida", date, total="4", weapon=weapon))
        rows.append(row("SP", "Cidade Errada", "Homicídio doloso", date, victims="1"))
    if year == "2025":
        rows.append(row("SP", "Campinas", "Homicídio doloso", "lixo", victims="5"))  # invalid date
    return pd.DataFrame(rows)


FLEET = [
    ("SAO PAULO", "MOJI MIRIM", "60000.0"),          # alias + float text
    ("SAO PAULO", "SANTA BARBARA D'OESTE", "1.234"),  # thousands separator
    ("SAO PAULO", "SAO PAULO", "9000000"),
    ("SAO PAULO", "CAMPINAS", "900000"),
    ("SAO PAULO", "SOROCAVA", "500000"),              # fuzzy (distance 1)
    ("SP", "MUNICIPIO NAO INFORMADO", "7"),
    ("SP", "SAO PAULO DO NORTE DISTANTE", "7"),       # too far: unmatched
    ("PIAUI", "BOM JESUS", "9000"),
    ("RIO GRANDE DO SUL", "BOM JESUS", "8000"),
    ("PARANA", "CIDADE INEXISTENTE", "10"),
]


def senatran_sheet(fleet=FLEET) -> pd.DataFrame:
    """SENATRAN layout: title rows above the header and a copy of the header inside the table."""
    header = ("UF", "MUNICIPIO", "TOTAL")
    rows = [("FROTA POR MUNICIPIO E TIPO", None, None), (None, None, None), header, *fleet[:5], header, *fleet[5:]]
    return pd.DataFrame(rows)


def xlsx(frame: pd.DataFrame, header: bool = True) -> bytes:
    buffer = io.BytesIO()
    frame.to_excel(buffer, index=False, header=header, engine="openpyxl")
    return buffer.getvalue()


class FakeWeb:
    """Stands in for http_get: serves the SINESP and SENATRAN spreadsheets from memory, so the real
    Bronze notebooks 02 and 03 run. Tests change the files to simulate new publications."""

    def __init__(self, ns: dict):
        self.ns, self.files, self.requests = ns, {}, []
        self.reset()

    def reset(self):
        for year in ["2025", "2026"]:
            self.publish_sinesp(year)
        self.publish_senatran(self.ns["SENATRAN_FLEET_URL"])

    def publish_sinesp(self, year: str, extra_victims: int = 0):
        self.files[self.ns["SINESP_URL_TEMPLATE"].format(year=year)] = xlsx(sinesp_rows(year, extra_victims))

    def publish_senatran(self, url: str, fleet=FLEET):
        self.files[url] = xlsx(senatran_sheet(fleet), header=False)

    def get(self, url, *args, **kwargs):
        self.requests.append(url)
        if url not in self.files:
            raise ConnectionError(f"Not published: {url}")
        return SimpleNamespace(content=self.files[url], status_code=200)


# Mesh: 3x3 grid of 0.5 degree cells (diagonal cells touch only at a corner, so they are NOT
# rook neighbors), one code that is not in the dimension (9999999) and an ocean island.
MESH_LAYOUT = {
    (0, 0): "3530805",
    (1, 0): "3545803",
    (2, 0): "3550308",
    (0, 1): "3509502",
    (1, 1): "3552205",
    (2, 1): "2201903",
    (0, 2): "4302303",
    (2, 2): "9999999",
}
ISLAND = "5101837"


def write_mesh(ns: dict, folder: Path) -> None:
    import geopandas as gpd
    from shapely.geometry import box

    rows = [
        {"CD_MUN": code, "geometry": box(-47 + x * 0.5, -23 + y * 0.5, -46.5 + x * 0.5, -22.5 + y * 0.5)}
        for (x, y), code in MESH_LAYOUT.items()
    ]
    rows.append({"CD_MUN": ISLAND, "geometry": box(-30, -4, -29.9, -3.9)})
    shp_dir = folder / "mesh"
    shp_dir.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(rows, crs="EPSG:4674").to_file(shp_dir / "BR_Municipios_2099.shp")
    zip_path = folder / "BR_Municipios_2099.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for file in shp_dir.glob("BR_Municipios_2099.*"):
            archive.write(file, file.name)
    spark, F = ns["spark"], ns["F"]
    manifest = spark.createDataFrame(
        [(2099, zip_path.name, str(zip_path), "test", 0.1)],
        "mesh_year int, file_name string, volume_path string, source_url string, size_mb double",
    )
    ns["write_table"](manifest.withColumn("_ingested_at", F.current_timestamp()), "bronze", "bronze_ibge_mesh_files")
    ns["log_change"]("bronze_ibge_mesh", [2099], "fake")
