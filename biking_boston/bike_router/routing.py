from scoring import calculate_edge_safety
import osmnx as ox
import networkx as nx


def apply_safety_scores(G):

    for u, v, k, data in G.edges(keys=True, data=True):

        data["safety_weight"] = (
            calculate_edge_safety(data)
        )

    return G


def safest_route(
    G,
    start_lat,
    start_lon,
    end_lat,
    end_lon
):

    orig = ox.nearest_nodes(
        G,
        X=start_lon,
        Y=start_lat
    )

    dest = ox.nearest_nodes(
        G,
        X=end_lon,
        Y=end_lat
    )

    route = nx.shortest_path(
        G,
        orig,
        dest,
        weight="safety_weight"
    )

    return route