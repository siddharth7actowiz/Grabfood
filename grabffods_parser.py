import os
import gzip
import zipfile
import json
from datetime import datetime
from pydantic import *
from typing import List,Dict,Optional


input_src_folder=r"D:/Siddharth/Grabfood_unzip_load/grab_food_pages/"
f_name = "GRABFOOD_MENU"
today = datetime.today()
todays_date = datetime.strftime(today, "%Y_%m_%d")
file_name = f"{f_name}_{todays_date}.json"

def load_json(input_src_folder):
    json_data = []

    for name in os.listdir("D:/Siddharth/Grabfood_unzip_load/grab_food_pages"):
        if name.endswith(".gz"):

            full_path = os.path.join(input_src_folder, name)

            with gzip.open(full_path, "rt", encoding="utf-8") as f:
                json_data.append(json.load(f))

    return json_data

class Offer(BaseModel):
    Title: Optional[str]
    SubTitle: Optional[str]


class RestaurantDetails(BaseModel):
    Restaurant_Name: Optional[str]
    Restaurant_ID: Optional[str]
    Branch_Name: Optional[str]
    Cuisine: Optional[str]
    Restaurant_Image: Optional[str]

    Tip: List[str]
    Timezone: Optional[str]
    ETA: Optional[int]
    DeliveryOptions: List
    Rating: Optional[float]
    Is_Open: Optional[bool]

    Currency_Code: Optional[str]
    Currency_Symbol: Optional[str]

    Offers: List[Offer]
    Timing_Everyday: Optional[str]


class MenuItem(BaseModel):
    Restaurant_ID: Optional[str]
    Category_Name: Optional[str]
    Item_ID: Optional[str]
    Item_Name: Optional[str]
    Item_Description: Optional[str]

    Item_Price: Optional[float]
    Item_Discounted_Price: Optional[float]

    Item_Image_URL: List[Optional[str]]
    Item_Thumbnail_URL: List[Optional[str]]

    Item_Available: bool
    Is_Top_Seller: bool


class Rest(BaseModel):
    Restaurant_Details: RestaurantDetails
    Menu_Items: List[MenuItem]


def parser(validated_json):
    all_data=[]
    for raw_json_dict in validated_json:
        print(raw_json_dict)
        Rest_Data = {}
        merchant_data = raw_json_dict.get("merchant") or {}

        restaurant_details = {

            "Restaurant_Name": merchant_data.get("name"),
            "Restaurant_ID": merchant_data.get("ID"),
            "Branch_Name": merchant_data.get("branchName"),
            "Cuisine": merchant_data.get("cuisine"),
            "Restaurant_Image": merchant_data.get("photoHref"),

            "Tip": [merchant_data.get("sofConfiguration", {}).get("tips")],

            "Timezone": merchant_data.get("timeZone"),
            "ETA": merchant_data.get("ETA"),
            "DeliveryOptions": merchant_data.get("deliveryOptions", []),
            "Rating": merchant_data.get("rating"),
            "Is_Open": merchant_data.get("openingHours", {}).get("open"),
            "Currency_Code": merchant_data.get("currency", {}).get("code"),
            "Currency_Symbol": merchant_data.get("currency", {}).get("symbol"),
            "Offers": [],
            "Timing_Everyday": merchant_data.get("openingHours", {}).get("displayedHours")
        }

        offer_carousel = merchant_data.get("offerCarousel") or {}

        for offers in offer_carousel.get("offerHighlights", []):
            highlight = offers.get("highlight") or {}

            off = {
                "Title": highlight.get("title"),
                "SubTitle": highlight.get("subtitle")
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
                    "Item_Available":True if item.get("available") else False ,
                    "Is_Top_Seller": True if item.get("topSeller") else False,
                }

                if all([
                    temp_product_dict["Item_Name"],
                    temp_product_dict["Item_Description"],
                    temp_product_dict["Item_Price"],
                    temp_product_dict["Item_Image_URL"]
                ]):
                    products_list.append(temp_product_dict)

        Rest_Data["Restaurant_Details"] = restaurant_details
        Rest_Data["Menu_Items"] = products_list
        try:
            validated_rest = Rest(**Rest_Data)
            all_data.append(Rest_Data)  
            return all_data
        except ValidationError as e:
            print(e)


def export_structured_data_func(struct_json_data):
    with open(file_name, "w", encoding="utf-8") as file:
        file.write(json.dumps(struct_json_data, indent=4))



#Function Calls
validated_json=load_json(input_src_folder)
struct_json_data=parser(validated_json)
export_structured_data_func(struct_json_data)
