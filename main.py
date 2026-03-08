from grubfood_data_abstract import load_json_data, process
from grubfood_model import restaurant
from grubfood_database import batch_insert

file_path = r"C:\Users\vishal.mistry\Downloads\grab_food_pages\grab_food_pages"

all_data = load_json_data(file_path)
parsed = process(all_data)

validation_check = [restaurant(**item).model_dump() for item in parsed]

batch_insert(validation_check, batch_size=500)