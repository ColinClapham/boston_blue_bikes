import osmnx as ox

def load_bike_graph(place="Boston, Massachusetts, USA"):
    """
    Download bike network graph
    """

    places = [
        "Boston, Massachusetts, USA",
        "Somerville, Massachusetts, USA",
        "Cambridge, Massachusetts, USA",
        "Brookline, Massachusetts, USA"
    ]

    G = ox.graph_from_place(
        places,
        network_type="bike",
        simplify=True
    )

    return G