import time
import mysql.connector

from grubfood_data_abstract import load_json, process
from grubfood_database import create_two_tables, insert_restaurant_data

start = time.time()

base_path = r"C:\Users\vishal.mistry\Downloads\grab_food_pages\grab_food_pages"

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="actowiz",
    database="grubfood"
)

json_data_loaded = load_json(base_path)

processed_data = []
for data in json_data_loaded:
    result = process(data)
    if result:
        processed_data.append(result)

create_two_tables(connection)

for restaurant in processed_data:
    insert_restaurant_data(connection, restaurant)

end = time.time()
print(end-start)
connection.close()
