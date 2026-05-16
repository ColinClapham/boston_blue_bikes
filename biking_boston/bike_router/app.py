import streamlit as st
import folium
from streamlit_folium import st_folium

import osmnx as ox

from geocoder import geocode_address
from routing import safest_route
from scoring import calculate_edge_safety
from explain import (
    get_alternative_routes,
    compare_routes,
    explain_best_route
)

# -----------------------------------
# LOAD GRAPH
# -----------------------------------

@st.cache_resource
def load_graph():

    G = ox.load_graphml(
        "boston_bike.graphml"
    )

    for u, v, k, data in G.edges(keys=True, data=True):

        data["safety_weight"] = (
            calculate_edge_safety(data)
        )

    return G


G = load_graph()

# -----------------------------------
# UI
# -----------------------------------

st.title("Boston Safe Bike Router")

start_address = st.text_input(
    "Start Address",
    "17 Rose Street, Somerville MA"
)

end_address = st.text_input(
    "End Address",
    "Fenway Park, Boston MA"
)

# -----------------------------------
# ROUTE BUTTON
# -----------------------------------

if st.button("Find Safest Route"):

    # Geocode
    start_lat, start_lon = geocode_address(start_address)

    end_lat, end_lon = geocode_address(end_address)

    # Route
    route = safest_route(
        G,
        start_lat,
        start_lon,
        end_lat,
        end_lon
    )

    # Route GeoDataFrame
    route_gdf = ox.routing.route_to_gdf(
        G,
        route
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
        weight="safety_weight"
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
        name="Safe Route"
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

    st.dataframe(comparison_df)