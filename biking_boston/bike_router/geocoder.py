import osmnx as ox

def geocode_address(address):
    """
    Convert address to (lat, lon)
    """
    lat, lon = ox.geocode(address)

    return lat, lon