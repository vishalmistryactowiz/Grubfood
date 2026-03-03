from grubfood_data_abstract import load_json, process
from grubfood_model import restaurant
from grubfood_database import batch_insert

base_path = r"C:\Users\vishal.mistry\Downloads\grab_food_pages\grab_food_pages"

raw_data = load_json(base_path)
parsed = process(raw_data)

validated = [restaurant(**item).model_dump() for item in parsed]

batch_insert(validated, batch_size=500)