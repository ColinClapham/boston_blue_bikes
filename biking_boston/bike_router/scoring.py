def calculate_edge_safety(data):

    score = data.get("length", 1)

    highway = data.get("highway")
    cycleway = data.get("cycleway")
    bicycle = data.get("bicycle")
    maxspeed = data.get("maxspeed")

    # ---- Bike infrastructure signals ----
    if cycleway in ["track", "separate"]:
        score *= 0.3

    elif cycleway == "lane":
        score *= 0.6

    # fallback signal (VERY important in OSM data)
    elif bicycle == "designated":
        score *= 0.5

    elif bicycle == "yes":
        score *= 1.0

    elif bicycle == "no":
        score *= 3.0

    # ---- Road hierarchy ----
    if highway in ["motorway", "trunk"]:
        score *= 3.0

    elif highway in ["primary"]:
        score *= 2.0

    elif highway in ["secondary"]:
        score *= 1.5

    # ---- Speed penalty ----
    try:
        if maxspeed:
            speed = int(str(maxspeed).split()[0])
            if speed >= 35:
                score *= 1.5
    except:
        pass

    return score