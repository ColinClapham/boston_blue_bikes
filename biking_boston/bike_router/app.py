import streamlit as st
import folium
from streamlit_folium import st_folium
from directions import generate_turn_by_turn

import osmnx as ox

from geocoder import geocode_address
from routing import safest_route
from scoring import calculate_edge_safety
from explain import (
    get_alternative_routes,
    compare_routes,
    explain_best_route
)

from weights import get_weight_name

# -----------------------------------
# LOAD GRAPH
# -----------------------------------

@st.cache_resource
def load_graph():

    G = ox.load_graphml(
        "boston_bike.graphml"
    )

    for u, v, k, data in G.edges(keys=True, data=True):

        safety = calculate_edge_safety(data)

        # Safest route
        data["safety_weight"] = safety

        # Fastest route
        data["length_weight"] = data.get(
            "length",
            1
        )

        # Balanced route
        data["balanced_weight"] = (
                safety
                +
                data.get("length", 1) * 0.01
        )

        # Family-safe route
        family_weight = safety

        highway = data.get("highway")

        if highway in [
            "primary",
            "secondary"
        ]:
            family_weight *= 4

        data["family_weight"] = family_weight

    return G


G = load_graph()

# -----------------------------------
# UI
# -----------------------------------

st.title("Boston Bike Router")

start_address = st.text_input(
    "Start Address",
    "600 Atlantic Avenue, Boston MA"
)

end_address = st.text_input(
    "End Address",
    "Fenway Park, Boston MA"
)

route_mode = st.selectbox(
    "Route Mode",
    [
        "Safest",
        "Fastest",
        "Balanced",
        "Family Safe"
    ]
)

# -----------------------------------
# ROUTE BUTTON
# -----------------------------------

if st.button(f"Find Route"):

    # Geocode
    start_lat, start_lon = geocode_address(start_address)

    end_lat, end_lon = geocode_address(end_address)

    # Get selected routing weight
    weight_name = get_weight_name(
        route_mode
    )

    # Route
    route = safest_route(
        G,
        start_lat,
        start_lon,
        end_lat,
        end_lon,
        weight=weight_name
    )

    # Route GeoDataFrame
    route_gdf = ox.routing.route_to_gdf(
        G,
        route
    )

    # Add Directions
    directions = generate_turn_by_turn(
        route_gdf
    )

    # Explanation
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

    alt_routes = get_alternative_routes(
        G,
        orig,
        dest,
        k=5,
        weight=weight_name
    )

    comparison_df = compare_routes(
        G,
        alt_routes
    )

    explanation = explain_best_route(
        comparison_df
    )

    # -----------------------------------
    # MAP
    # -----------------------------------

    center_lat = (
        start_lat + end_lat
    ) / 2

    center_lon = (
        start_lon + end_lon
    ) / 2

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13
    )

    # Add route
    folium.GeoJson(
        route_gdf.to_json(),
        name=f"{route_mode} Route"
    ).add_to(m)

    # Add markers
    folium.Marker(
        [start_lat, start_lon],
        tooltip="Start"
    ).add_to(m)

    folium.Marker(
        [end_lat, end_lon],
        tooltip="End"
    ).add_to(m)

    st_folium(
        m,
        width=1000,
        height=600
    )

    # -----------------------------------
    # EXPLANATION
    # -----------------------------------

    st.subheader("Why This Route Was Chosen")

    st.write(explanation)

    st.subheader("Route Comparison")

    st.subheader("Turn-by-Turn Directions")

    for i, d in enumerate(directions, start=1):
        st.write(f"{i}. {d}")

    st.dataframe(comparison_df)

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Distance",
        f"{comparison_df.iloc[0]['total_length_m'] / 1000:.1f} km"
    )

    col2.metric(
        "Bike Lane %",
        f"{comparison_df.iloc[0]['bike_lane_pct']:.0f}%"
    )

    col3.metric(
        "Safety Score",
        f"{comparison_df.iloc[0]['avg_safety_weight']:.1f}"
    )