import json
import mysql.connector


def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="actowiz",
        database="grabfood"
    )

def batch_insert(data, batch_size=1000):

    if not data:
        print("No data to insert")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # RESTAURANTS TABLE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurants (
        restaurant_id VARCHAR(50) PRIMARY KEY,
        restaurant_name VARCHAR(255),
        cuisine VARCHAR(255),
        timezone VARCHAR(100),
        eta VARCHAR(100),
        rating FLOAT,
        vote INT,
        deliver_by VARCHAR(100),
        distance_range VARCHAR(100),
        timing JSON,
        tips TEXT
    )
    """)

    # MENU TABLE 
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS menu_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        restaurant_id VARCHAR(50),
        category_name VARCHAR(255),
        category_id VARCHAR(100),
        item_id VARCHAR(100) UNIQUE,
        item_name VARCHAR(255),
        price FLOAT,
        available TINYINT,
        IMG TEXT,
        description TEXT
    )
    """)
    conn.commit()

    restaurant_query = """
    INSERT INTO restaurants
    (restaurant_id, restaurant_name, cuisine, timezone, eta, rating, vote, deliver_by, distance_range, timing, tips)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        restaurant_name=VALUES(restaurant_name),
        rating=VALUES(rating)
    """

    menu_query = """
    INSERT IGNORE INTO menu_items
    (restaurant_id, category_name, category_id, item_id, item_name, price, available, IMG, description)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    total_restaurants = 0
    total_menu_items = 0

    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        restaurant_values = []
        menu_values = []
        for item in batch:
            restaurant_values.append((
                item.get("restaurant_id"),
                item.get("restaurant_name"),
                item.get("cuisine"),
                item.get("timezone"),
                item.get("ETA"),
                item.get("Rating"),
                item.get("vote"),
                item.get("deliverBy"),
                item.get("distance_range"),
                json.dumps(item.get("timing")) if item.get("timing") else None,
                item.get("tips")
            ))

            for category in item.get("Menu", []):
                for menu_item in category.get("Items", []):
                    menu_values.append((
                        item.get("restaurant_id"),
                        category.get("category_name"),
                        category.get("category_id"),
                        menu_item.get("item_id"),
                        menu_item.get("item_name"),
                        menu_item.get("price"),
                        menu_item.get("available"),
                        menu_item.get("IMG"),
                        menu_item.get("description")
                    ))

        # Insert Restaurants
        cursor.executemany(restaurant_query, restaurant_values)
        batch_restaurants = cursor.rowcount
        total_restaurants += batch_restaurants

        # Insert Menu
        batch_menu = 0
        if menu_values:
            cursor.executemany(menu_query, menu_values)
            batch_menu = cursor.rowcount
            total_menu_items += batch_menu

        conn.commit()

    print("Total Restaurants Batch:", total_restaurants)
    print("Total Menu Items Batch:", total_menu_items)

    cursor.close()
    conn.close()