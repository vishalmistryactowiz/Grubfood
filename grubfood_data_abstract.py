import json
import gzip
import os

def load_json_data(data):
    all_data = []
    for file_name in os.listdir(data):
        if file_name.endswith('.gz'):
            base = os.path.join(data, file_name)
            with gzip.open(base, 'rt', encoding='utf-8') as file:
                all_data.append(json.load(file))
    return all_data

def process(extract_data):
    grabfood_json_data = []
    for data in extract_data:
        merchant = data.get("merchant") or data.get("data", {}).get("merchant") or {} 
        result = {}
        result['restaurant_id'] = (
            merchant.get("ID") or
            merchant.get("id") or
            merchant.get("merchantID") or
            merchant.get("merchant_id") or
            merchant.get("restaurantID") or
            merchant.get("uuid")
        )

        if not result['restaurant_id']:
            continue
        result["restaurant_name"] = merchant.get("name")
        result["cuisine"] = merchant.get("cuisine")
        result["timezone"] = merchant.get("timeZone")
        result["ETA"] = merchant.get("ETA")
        result["Rating"] = merchant.get("rating")
        result["vote"] = merchant.get("voteCount")
        result["deliverBy"] = merchant.get("deliverBy")
        result["distance_range"] = merchant.get("radius")

        result["timing"] = merchant.get("openingHours")
        result["tips"] = merchant.get("sofConfiguration", {}).get("tips")
        
        # Menu information 
        categories = merchant.get("menu", {}).get("categories", [])
        result["Menu"] = []

        for category in categories:
            category_id = category.get("ID") or category.get("id")

            category_dict = {
                "category_name": category.get("name"),
                "category_id": str(category_id) if category_id else None,
                "Items": []
            }

            for item in category.get("items", []):
                item_id = item.get("ID") or item.get("id")
                if not item_id:
                    continue

                price_data = item.get("priceV2", {})
                amount = price_data.get("amountDisplay")

                try:
                    price_value = float(str(amount).replace(",", "")) if amount else None
                except:
                    price_value = None

                item_dict = {
                    "item_id": str(item_id),
                    "item_name": item.get("name"),
                    "price": price_value,
                    "available": 1 if item.get("available") else 0,
                    "IMG": item.get("imgHref"),
                    "description": item.get("description")
                }

                category_dict["Items"].append(item_dict)

            result["Menu"].append(category_dict)

        grabfood_json_data.append(result)
        
    return grabfood_json_data