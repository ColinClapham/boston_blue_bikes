def get_weight_name(route_mode):

    if route_mode == "Safest":
        return "safety_weight"

    elif route_mode == "Fastest":
        return "length_weight"

    elif route_mode == "Balanced":
        return "balanced_weight"

    elif route_mode == "Family Safe":
        return "family_weight"

    return "safety_weight"