
from pydantic import BaseModel
from typing import List, Dict, Any

class menu(BaseModel):
    item_id: str | None
    item_name: str | None
    price: float | None
    available: int | None
    IMG: str | None
    description: str | None
    
class category(BaseModel):
    category_name: str | None
    category_id: str | None
    Items: List[menu] | None

class restaurant(BaseModel):
    restaurant_id: str | None
    restaurant_name: str | None
    cuisine: str | None
    timezone: str | None
    ETA: int | None
    Rating: float | None
    vote: int | None
    deliverBy: str | None
    distance_range: int | None
    Currency: str | None
    timing: Dict[str, Any] | None
    tips: str | None
    Menu: List[category] | None
