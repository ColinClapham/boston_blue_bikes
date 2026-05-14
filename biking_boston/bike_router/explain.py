import networkx as nx
import osmnx as ox
import pandas as pd

def get_alternative_routes(G, orig, dest, k=3, weight="safety_weight"):
    return ox.routing.k_shortest_paths(
        G,
        orig,
        dest,
        k=k,
        weight=weight
    )


def summarize_route(G, route):

    gdf = ox.routing.route_to_gdf(G, route)

    # Normalize columns
    for col in [
        "cycleway",
        "bicycle",
        "highway",
        "maxspeed"
    ]:
        if col not in gdf.columns:
            gdf[col] = None

    # Bike friendliness
    bike_friendly = (
        gdf["cycleway"].notna()
        |
        gdf["bicycle"].isin(
            ["designated", "yes"]
        )
    )

    # Arterial roads
    arterial_roads = gdf["highway"].isin(
        ["primary", "secondary"]
    )

    # High speed roads
    high_speed = (
        gdf["maxspeed"]
        .astype(str)
        .str.contains(
            "30|35|40|45",
            na=False
        )
    )

    summary = {
        "total_length_m":
            gdf["length"].sum(),

        "avg_safety_weight":
            gdf["safety_weight"].mean(),

        "bike_lane_pct":
            bike_friendly.mean() * 100,

        "arterial_pct":
            arterial_roads.mean() * 100,

        "high_speed_pct":
            high_speed.mean() * 100,

        "num_segments":
            len(gdf)
    }

    return summary


def compare_routes(G, routes):
    summaries = []

    for i, route in enumerate(routes):
        summary = summarize_route(G, route)
        summary["route_id"] = i
        summaries.append(summary)

    return pd.DataFrame(summaries)


def explain_best_route(comparison_df):

    best = comparison_df.sort_values(
        "avg_safety_weight"
    ).iloc[0]

    explanation = []

    explanation.append(
        f"This route was chosen because it has the lowest safety cost "
        f"({best['avg_safety_weight']:.2f})."
    )

    explanation.append(
        f"It uses bike-friendly infrastructure on ~{best['bike_lane_pct']:.0f}% of segments."
    )

    explanation.append(
        f"Total segments: {int(best['num_segments'])}."
    )

    if best["high_speed_pct"] > 20:
        explanation.append(
            "Some sections still include higher-speed roads, "
            "but they are minimized compared to alternatives."
        )
    else:
        explanation.append(
            "It avoids most high-speed road segments."
        )

    return "\n".join(explanation)


def explain_route(G, orig, dest, chosen_route):

    alt_routes = get_alternative_routes(
        G,
        orig,
        dest
    )

    comparison = compare_routes(G, alt_routes)

    explanation = explain_best_route(comparison)

    return explanation, comparison


def compare_against_best(best, others):
    deltas = []

    for i, r in others.iterrows():
        deltas.append({
            "route": i,
            "extra_length_pct": (
                r["total_length_m"] / best["total_length_m"] - 1
            ) * 100,
            "less_bike_lanes_pct": (
                best["bike_lane_pct"] - r["bike_lane_pct"]
            )
        })

    return pd.DataFrame(deltas)