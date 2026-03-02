from pydantic import BaseModel
from typing import List, Optional, Any


class Offer(BaseModel):
    Title: Optional[str]
    SubTitle: Optional[str]

class RestaurantDetails(BaseModel):
    Restaurant_Name: Optional[str]
    Restaurant_ID: Optional[str]
    Branch_Name: Optional[str]
    Cuisine: Optional[str]
    Restaurant_Image: Optional[str]

    Tip: List[Any]
    Timezone: Optional[str]
    ETA: Optional[int]
    DeliveryOptions: List[str]
    Rating: Optional[float]
    Is_Open: Optional[bool]

    Currency_Code: Optional[str]
    Currency_Symbol: Optional[str]

    Offers: List[Offer]
    Timing_Everyday: Optional[str]


class MenuItem(BaseModel):
    Restaurant_ID: Optional[str]
    Category_Name: Optional[str]
    Item_ID: str | None
    Item_Name: str | None
    Item_Description: str | None

    Item_Price: float | None
    Item_Discounted_Price: float | None

    Item_Image_URL: List[str] | None
    Item_Thumbnail_URL: List[Optional[str]]

    Item_Available: bool
    Is_Top_Seller: bool


class Rest(BaseModel):
    Restaurant_Details: RestaurantDetails
    Menu_Items: List[MenuItem]

