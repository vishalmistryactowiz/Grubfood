from pydantic import BaseModel

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


class restaurants(BaseModel):
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