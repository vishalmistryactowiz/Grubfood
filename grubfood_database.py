# Create Table Block
def create_two_tables(connection):
    cursor = connection.cursor()

    # Restaurants table
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
            displayed_hours VARCHAR(50)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS menu_items (
            item_id VARCHAR(50) UNIQUE,
            restaurant_id VARCHAR(50),
            category_id VARCHAR(50),
            category_name VARCHAR(100),
            item_name VARCHAR(255),
            description TEXT,
            price DECIMAL(8,2),
            available BOOLEAN,
            image_url TEXT
        );
    """)

    connection.commit()
    cursor.close()


# Insert Query Block
def insert_restaurant_data(connection, data):
    cursor = connection.cursor()

    # Insert restaurant
    cursor.execute("""
        INSERT IGNORE INTO restaurants
        (restaurant_id, restaurant_name, cuisine, logo, timezone,
         eta, distance, rating, deliver_by, radius, displayed_hours)
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
                (item_id, restaurant_id, category_id, category_name,
                 item_name, description, price, available, image_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item.items_id,
                data.res_ID,
                category.category_id,
                category.category_name,
                item.items_Name,
                item.description,
                price,
                item.available,
                item.image_url
            ))

    connection.commit()
    cursor.close()