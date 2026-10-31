"""Neighborhood graph and spatial statistics (notebooks 12-13) on a synthetic 3x3 grid mesh."""

import pytest
from fake_sources import ISLAND, MESH_LAYOUT

LISA_VALUES = {"High-High", "Low-Low", "High-Low", "Low-High", "Not significant", "No neighbors", "No data"}


@pytest.fixture(scope="module")
def read(spatial):
    spark, tbl = spatial["spark"], spatial["tbl"]
    return lambda layer, name: spark.table(tbl(layer, name)).toPandas()


def expected_rook_pairs():
    """Cells that share an edge (not only a corner), excluding codes outside the dimension."""
    cells = {xy: code for xy, code in MESH_LAYOUT.items() if code != "9999999"}
    pairs = set()
    for (x, y), code in cells.items():
        for dx, dy in [(1, 0), (0, 1)]:
            other = cells.get((x + dx, y + dy))
            if other:
                pairs.add(tuple(sorted((code, other))))
    return pairs


def test_adjacency_is_rook_contiguity(read):
    adjacency = read("silver", "municipality_adjacency")
    found = {tuple(sorted(p)) for p in zip(adjacency.municipality_code_a, adjacency.municipality_code_b, strict=True)}
    assert found == expected_rook_pairs()
    assert (adjacency.municipality_code_a < adjacency.municipality_code_b).all()  # stored once per pair


def test_border_length_in_km(read):
    adjacency = read("silver", "municipality_adjacency")
    # 0.5 degree edges at about 22 degrees south: about 51 km (north-south) to 56 km (east-west)
    assert adjacency.border_km.between(50, 57).all()


def test_island_has_no_neighbors(read):
    metrics = read("gold", "municipality_spatial_metrics").set_index("municipality_code")
    assert metrics.loc[ISLAND, "neighbor_count"] == 0
    assert metrics.loc[ISLAND, "lisa_cluster"] == "No neighbors"


def test_metrics_cover_every_municipality(read):
    metrics = read("gold", "municipality_spatial_metrics")
    analytics = read("gold", "municipality_analytics")
    assert set(metrics.municipality_code) == set(analytics.municipality_code)
    assert set(metrics.lisa_cluster) <= LISA_VALUES
    assert (metrics.degree == metrics.neighbor_count).all()
    assert metrics.global_moran_i.nunique() == 1


def test_communities_partition_the_municipalities(read):
    communities = read("gold", "graph_communities")
    metrics = read("gold", "municipality_spatial_metrics")
    assert communities.municipalities.sum() == len(metrics)
    assert set(communities.community_id) == set(metrics.community_id)


def test_borders_table_matches_adjacency(read):
    borders = read("gold", "municipality_borders")
    assert len(borders) == len(expected_rook_pairs())
