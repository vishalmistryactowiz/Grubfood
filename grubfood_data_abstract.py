import os
import gzip
import json
from grubfood_model import restaurants


def load_json(input_src_folder):
    json_data = []

    for name in os.listdir(input_src_folder):
        if name.endswith(".gz"):
            full_path = os.path.join(input_src_folder, name)
            with gzip.open(full_path, "rt", encoding="utf-8") as f:
                json_data.append(json.load(f))
    return json_data


def process(data):
    grubfood_dict = {}
    base = data.get("merchant") or data.get("data", {}).get("merchant") or {}

    restaurant_id = (
        base.get("ID")
        or base.get("id")
        or data.get("restaurant_id")
        or data.get("id")
    )

    if not restaurant_id:
        return None

    grubfood_dict["res_ID"] = str(restaurant_id)
    grubfood_dict["res_name"] = base.get("name")
    grubfood_dict["cuisine"] = base.get("cuisine")
    grubfood_dict["logo"] = base.get("photoHref")
    grubfood_dict["timeZone"] = base.get("timeZone")
    grubfood_dict["ETA"] = base.get("ETA")
    grubfood_dict["Distance"] = base.get("distanceInKm")
    grubfood_dict["Rating"] = base.get("rating")
    grubfood_dict["DeliverBy"] = base.get("deliverBy")
    grubfood_dict["Radius"] = base.get("radius")

    time_data = base.get("openingHours", {})
    grubfood_dict["Timing"] = {
        "displayedHours": time_data.get("displayedHours"),
        "SunDay": time_data.get("sun"),
        "Monday": time_data.get("mon"),
        "Tuesday": time_data.get("tue"),
        "Wednesday": time_data.get("wed"),
        "Thursday": time_data.get("thu"),
        "Friday": time_data.get("fri"),
        "Saturday": time_data.get("sat")
    }

    menu = base.get("menu", {})
    categories = menu.get("categories", [])

    all_categories = []

    if isinstance(categories, list):
        for category in categories:

            # 🔥 FIX 3: Flexible category ID
            category_id = category.get("ID") or category.get("id")

            if not category_id:
                continue

            category_data = {
                "category_name": category.get("name"),
                "category_id": str(category_id),
                "items": []
            }

            items = category.get("items", [])
            if isinstance(items, list):
                for item in items:

                    # 🔥 FIX 4: Flexible item ID
                    item_id = item.get("ID") or item.get("id")
                    if not item_id:
                        continue

                    price_data = item.get("priceV2") or {}

                    item_data = {
                        "items_id": str(item_id),
                        "items_Name": item.get("name"),
                        "price": price_data.get("amountDisplay"),
                        "available": item.get("available"),
                        "image_url": item.get("imgHref"),
                        "description": item.get("description")
                    }

                    category_data["items"].append(item_data)

            all_categories.append(category_data)

    grubfood_dict["Categories"] = all_categories

    return restaurants.model_validate(grubfood_dict)