import json
from datetime import datetime
from pydantic import BaseModel,Field,ValidationError
from typing import *

from pydantic.v1.class_validators import Validator

input_data = "D:\\Siddharth\\grabfood\\grabfood.json"

f_name = "GRABFOOD_MENU"
today = datetime.today()
todays_date = datetime.strftime(today, "%Y_%m_%d")
file_name = f"{f_name}_{todays_date}.json"

def load_input_json(input_data):
    with open(input_data, "r", encoding="utf-8") as file:
        python_data = json.load(file)
        raw_json_dict = dict(python_data)
        return raw_json_dict

class Restraunt(BaseModel):
    Restaurant_ID:str
    Restaurant_Name:str
    Branch_Name:str
    Cuisine: str
    Restaurant_Image: Optional[str]
    Tip:List[str]
    Timezone: Optional[str]
    ETA: int
    DeliveryOptions:List[str]
    Rating:Optional[int]=None
    Currency_Code:str
    Currency_Symbol:str
    Is_Open: Optional[bool]
    Offers:List[Dict[str,str]]
    Timing_Everyday: Optional[str]


class Menu(BaseModel):
    Restaurant_ID:str
    Category_Name:str
    Item_ID:str
    Item_Name:str
    Item_Description:str
    Item_Price:Optional[float]=None
    Item_Discounted_Price:Optional[float]=None
    Item_Image_URL:List[str]
    Item_Thumbnail_URL:List[str]
    Item_Available:Optional[bool]=None
    Is_Top_Seller:Optional[bool]=None

class Restaurant_Model(BaseModel):
    Restaurant_Details:Restraunt
    Menu_Items:List[Menu]

    pass

def parse_json_(raw_json_dict):
    Rest_Data={}
    merchant_data = raw_json_dict.get("merchant", {})

    restaurant_details = {
        "Restaurant_ID": merchant_data.get("ID"),
        "Restaurant_Name": merchant_data.get("name"),
        "Branch_Name":merchant_data.get("branchName"),
        "Cuisine": merchant_data.get("cuisine"),
        "Restaurant_Image": merchant_data.get("photoHref"),

        "Tip": [merchant_data.get("sofConfiguration",{}).get("tips")] ,

        "Timezone": merchant_data.get("timeZone"),
        "ETA": merchant_data.get("ETA"),
        "DeliveryOptions":merchant_data.get("deliveryOptions",[]),
        "Rating":merchant_data.get("rating"),
        "Is_Open": merchant_data.get("openingHours", {}).get("open"),
        "Currency_Code":merchant_data.get("currency",{}).get("code"),
        "Currency_Symbol":merchant_data.get("currency",{}).get("symbol"),
        "Offers":[],
        "Timing_Everyday": merchant_data.get("openingHours",{}).get("displayedHours")
    }

    for offers in merchant_data.get("offerCarousel").get("offerHighlights", []):
        off = {
            "Title": offers.get("highlight").get("title"),
            "SubTitle": offers.get("highlight").get("subtitle")
        }
        restaurant_details["Offers"].append(off)
    categories = merchant_data.get("menu", {}).get("categories", [])
    products_list = []

    for category in categories:
        category_name = category.get("name")
        items = category.get("items", [])

        for item in items:

            temp_product_dict = {
                "Restaurant_ID": merchant_data.get("ID"),
                "Category_Name": category_name,
                "Item_ID": item.get("ID"),
                "Item_Name": item.get("name"),
                "Item_Description": item.get("description"),
                "Item_Price": float(item.get("priceInMinorUnit", 0)) / 100,
                "Item_Discounted_Price": float(item.get("discountedPriceInMin", 0)) / 100,
                "Item_Image_URL": [item.get("imgHref") or (item.get("images")[0] if item.get("images") else None)],
                "Item_Thumbnail_URL": [item.get("thumbImages")[0] if item.get("thumbImages") else None],
                "Item_Available": item.get("available"),
                "Is_Top_Seller": True if item.get("topSeller") else False,
            }

            if all([
                temp_product_dict["Item_Name"],
                temp_product_dict["Item_Description"],
                temp_product_dict["Item_Price"],
                temp_product_dict["Item_Image_URL"]
            ]):
                products_list.append(temp_product_dict)

    Rest_Data["Restaurant_Details"]=restaurant_details
    Rest_Data["Menu_Items"]=products_list

    try:
        validation=Restaurant_Model(**Rest_Data)
        return Rest_Data
    except ValidationError as e:
        print(e)

def export_structured_data_func(struct_json_data):
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(json.dumps(struct_json_data, indent=4))

raw_json_dict = load_input_json(input_data)
validated_json_data = parse_json_(raw_json_dict)
export_structured_data_func(validated_json_data)

print("GrabFood structured JSON created successfully!")