import osmnx as ox

from geocoder import geocode_address
from extract_boston_street_data import load_bike_graph
from routing import safest_route
from scoring import calculate_edge_safety
import geopandas as gpd
from explain import (
    get_alternative_routes,
    compare_routes,
    explain_best_route
)

# Load graph
G = load_bike_graph()
# ox.save_graphml(
#     G,
#     "boston_bike.graphml"
# )
#
# G = ox.load_graphml(
#     "boston_bike.graphml"
# )

# Apply safety weights
for u, v, k, data in G.edges(keys=True, data=True):

    safety = calculate_edge_safety(data)

    data["safety_weight"] = safety

    # Fastest
    data["length_weight"] = data.get(
        "length",
        1
    )

    # Balanced
    data["balanced_weight"] = (
        safety * 0.7
        +
        data.get("length", 1) * 0.3
    )

    # Family Safe
    family_weight = safety

    highway = data.get("highway")

    if highway in [
        "primary",
        "secondary"
    ]:
        family_weight *= 4

    data["family_weight"] = family_weight

# Convert addresses
start_lat, start_lon = geocode_address(
    "17 Rose Street, Somerville MA"
)

end_lat, end_lon = geocode_address(
    "Fenway Park, Boston MA"
)

# Compute route
route = safest_route(
    G,
    start_lat,
    start_lon,
    end_lat,
    end_lon
)

# Convert route edges into GeoDataFrame
route_gdf = ox.routing.route_to_gdf(
    G,
    route
)

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

# Get alternative routes (uses node IDs)
alt_routes = get_alternative_routes(
    G,
    orig,
    dest,
    k=5,
    weight="safety_weight"
)

# Compare routes
comparison_df = compare_routes(G, alt_routes)

# Generate explanation
explanation = explain_best_route(comparison_df)

print("\n=== ROUTE EXPLANATION ===\n")
print(explanation)

print("\n=== ROUTE COMPARISON ===\n")
print(comparison_df)

# Save to GeoJSON
route_gdf.to_file(
    "safe_route.geojson",
    driver="GeoJSON"
)

print("Saved route to safe_route.geojson")
