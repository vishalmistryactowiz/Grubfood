import json
import gzip
import os

def load_json(data):
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

        restaurant_id = (
            merchant.get("ID")
            or merchant.get("id")
            or data.get("restaurant_id")
            or data.get("id")
        )

        if not restaurant_id:
            continue
        
        restaurant_information = {}
        restaurant_information['restaurant_id'] = str(restaurant_id)
        restaurant_information["restaurant_name"] = merchant.get("name")
        restaurant_information["cuisine"] = merchant.get("cuisine")
        restaurant_information["timezone"] = merchant.get("timeZone")
        restaurant_information["ETA"] = merchant.get("ETA")
        restaurant_information["Rating"] = merchant.get("rating")
        restaurant_information["vote"] = merchant.get("voteCount")
        restaurant_information["deliverBy"] = merchant.get("deliverBy")
        restaurant_information["distance_range"] = merchant.get("radius")
        
        estimated_fee = merchant.get("estimatedDeliveryFee", {})
        currency = estimated_fee.get("currency", {})
        restaurant_information["Currency"] = currency.get("symbol")

        restaurant_information["timing"] = merchant.get("openingHours")
        restaurant_information["tips"] = merchant.get("sofConfiguration", {}).get("tips")
        
        #Menu information 
        categories = merchant.get("menu", {}).get("categories", [])
        restaurant_information["Menu"] = []

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

            restaurant_information["Menu"].append(category_dict)

        grabfood_json_data.append(restaurant_information)
        
    return grabfood_json_data
        
        
    