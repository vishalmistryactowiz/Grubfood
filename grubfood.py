from datetime import datetime
import gzip
import time
import json
import os
import mysql.connector
from pydantic import BaseModel
start = time.time()
base_path = r"C:\Users\vishal.mistry\Downloads\grab_food_pages\grab_food_pages"
connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="actowiz",
        database="grub"
    )
class Item(BaseModel):
    items_id: str 
    items_Name: str | None = None
    price: str | None = None
    available: bool | None = None
    image_url: str | None = None
    description: str | None = None


class Category(BaseModel):
    category_name: str | None = None
    category_id: str | None = None
    items: list[Item]


class grub(BaseModel):
    res_ID: str
    res_name: str | None = None
    cuisine: str | None = None
    logo: str | None = None
    timeZone: str | None = None
    ETA: int | None = None
    Distance: float | None = None
    Rating: float | None = None
    DeliverBy: str | None = None
    Radius: int | None = None
    Timing: dict[str, str | None]
    Categories: list[Category]

# Load File Block
def load_json(input_src_folder):
    json_data = []

    for name in os.listdir(input_src_folder):
        if name.endswith(".gz"):   # only gz files
            full_path = os.path.join(input_src_folder, name)

            with gzip.open(full_path, "rt", encoding="utf-8") as f:
                json_data.append(json.load(f))
    return json_data
# extract Data Block
def process(data):
    e_dict = {}
    base = data.get("merchant") or {}
    if not base.get("ID"):
        return None
    # Basic Info
    e_dict["res_ID"] = base.get("ID")
    e_dict["res_name"] = base.get("name")
    e_dict["cuisine"] = base.get("cuisine")
    e_dict["logo"] = base.get("photoHref")
    e_dict["timeZone"] = base.get("timeZone")
    e_dict["ETA"] = base.get("ETA")
    e_dict["Distance"] = base.get("distanceInKm")
    e_dict["Rating"] = base.get("rating")
    e_dict["DeliverBy"] =base.get("deliverBy")
    e_dict["Radius"] = base.get("radius")
    # Opening Hours
    time_data = base.get("openingHours", {})
    e_dict["Timing"] = {
        "displayedHours": time_data.get("displayedHours"),
        "SunDay": time_data.get("sun"),
        "Monday": time_data.get("mon"),
        "Tuesday": time_data.get("tue"),
        "Wednesday": time_data.get("wed"),
        "Thursday": time_data.get("thu"),
        "Friday": time_data.get("fri"),
        "Saturday": time_data.get("sat")
    }
    # MENU SECTION
    menu = base.get("menu", {})
    categories = menu.get("categories", [])

    all_categories = []

    if isinstance(categories, list):
        for category in categories:
            category_name = category.get("name")
            cate_id = category.get("ID")
            items = category.get("items", [])
            category_data = {
                "category_name": category_name,
                "category_id" : cate_id,
                "items": []
            }
            if isinstance(items, list):
                for item in items:
                    price_data = item.get("priceV2") or {}
                    item_data = {
                        "items_id": item.get("ID"),
                        "items_Name": item.get("name"),
                        "price": price_data.get("amountDisplay"),
                        'available' : item.get("available"),
                        "image_url": item.get("imgHref"),
                        "description" :item.get("description")
                    }
                    category_data["items"].append(item_data)
            all_categories.append(category_data)
    e_dict["Categories"] = all_categories

    return grub.model_validate(e_dict)
# write Data in json file Block
def write_jsondata(all_data):
    current_date = datetime.now().strftime("%d-%m-%Y")
    filename = f"GRUBFOOD_{current_date}.json"

    # Convert Pydantic models to dict
    json_ready_data = [item.model_dump() for item in all_data]

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_ready_data, f, indent=4, ensure_ascii=False)
# Create Table Block
def create_three_tables(connection):
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurants (
        restaurant_id VARCHAR(50) PRIMARY KEY,
        restaurant_name VARCHAR(255),
        cuisine VARCHAR(100),
        logo TEXT,
        timezone VARCHAR(100),
        eta INT,
        distance DECIMAL(6,3),
        rating DECIMAL(2,1),
        deliver_by VARCHAR(50),
        radius INT,
        displayed_hours VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS categories (
    category_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    category_name VARCHAR(100),
    PRIMARY KEY (restaurant_id, category_id),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id)
        ON DELETE CASCADE
);""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS menu_items (
    item_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    category_id VARCHAR(50),
    item_name VARCHAR(255),
    description TEXT,
    price DECIMAL(8,2),
    available BOOLEAN,
    image_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (restaurant_id, item_id),
    FOREIGN KEY (restaurant_id) REFERENCES restaurants(restaurant_id)
        ON DELETE CASCADE,
    FOREIGN KEY (restaurant_id, category_id)
        REFERENCES categories(restaurant_id, category_id)
        ON DELETE CASCADE
);""")
    connection.commit()
    cursor.close()
# Insert Query Block
def insert_restaurant_data(connection, data):
    cursor = connection.cursor()

    cursor.execute("""
    INSERT IGNORE INTO restaurants
    (restaurant_id, restaurant_name, cuisine, logo, timezone, eta, distance, rating, deliver_by, radius, displayed_hours)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data.res_ID,
        data.res_name,
        data.cuisine,
        data.logo,
        data.timeZone,
        data.ETA,
        data.Distance,
        data.Rating,
        data.DeliverBy,
        data.Radius,
        data.Timing.get("displayedHours")
    ))

    inserted_items = set()

    for category in data.Categories:
        if not category.category_id:
            continue

        cursor.execute("""
        INSERT IGNORE INTO categories (restaurant_id, category_id, category_name)
        VALUES (%s, %s, %s)
        """, (
            data.res_ID,
            category.category_id,
            category.category_name
        ))

        for item in category.items:
            if item.items_id in inserted_items:
                continue

            inserted_items.add(item.items_id)

            # Safe price conversion
            price = 0
            if item.price:
                price = float(''.join(c for c in item.price if c.isdigit() or c == '.'))

            cursor.execute("""
            INSERT IGNORE INTO menu_items
            (item_id, restaurant_id, category_id, item_name, description, price, available, image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item.items_id,
                data.res_ID,
                category.category_id,
                item.items_Name,
                item.description,
                price,
                item.available,
                item.image_url
            ))

    connection.commit()
    cursor.close()

s =load_json(base_path)# function call

processed_data = []
for item in s:
    result = process(item)
    if result is not None:
        processed_data.append(result)

write_jsondata(processed_data)# function call
create_three_tables(connection) # function call
for restaurant in processed_data:
    insert_restaurant_data(connection, restaurant) # function call

connection.close()
end = time.time()
print(end-start)
                 