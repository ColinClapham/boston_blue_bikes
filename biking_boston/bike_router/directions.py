import pandas as pd

def generate_turn_by_turn(route_gdf):

    directions = []

    previous_street = None

    for _, row in route_gdf.iterrows():

        street = row.get("name")

        if isinstance(street, list):
            street = street[0]

        if not street:
            street = "Unnamed Road"

        length_m = row.get("length", 0)

        # Only emit instruction when street changes
        if street != previous_street:

            directions.append(
                f"Continue on {street} "
                f"for {length_m:.0f} meters"
            )

            previous_street = street

    return directions