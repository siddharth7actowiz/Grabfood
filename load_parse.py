import os
import gzip
import zipfile
import json
from modelvalidation import *


def load_json(zip_path):
    json_data = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for file_name in z.namelist():
            if file_name.endswith(".gz"):
                with z.open(file_name) as gz_file:
                     with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                        json_data.append(json.load(f))

    return json_data


def parser(validated_json):
    all_data=[]
    for raw_json_dict in validated_json:
        # print(raw_json_dict)
        Rest_Data = {}
        merchant_data = raw_json_dict.get("merchant")
        if not merchant_data:
            continue
        tips = merchant_data.get("sofConfiguration", {}).get("tips")
                
        restaurant_details = {

            "Restaurant_Name": merchant_data.get("name"),
            "Restaurant_ID": merchant_data.get("ID"),
            "Branch_Name": merchant_data.get("branchName"),
            "Cuisine": merchant_data.get("cuisine"),
            "Restaurant_Image": merchant_data.get("photoHref"),
            "Tip": tips if tips else [],
            "Timezone": merchant_data.get("timeZone"),
            "ETA": merchant_data.get("ETA"),
            "DeliveryOptions": merchant_data.get("deliveryOptions", []),
            "Rating": merchant_data.get("rating") ,
            "Is_Open": True if merchant_data.get("openingHours", {}).get("open") else False,
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
                img_href = item.get("imgHref")
                images = item.get("images") or []

                if img_href:
                    image_list = [img_href]
                elif images:
                    image_list = [images[0]]
                else:
                    image_list = []

                temp_product_dict = {
                    "Restaurant_ID": merchant_data.get("ID"),
                    "Category_Name": category_name,
                    "Item_ID": item.get("ID"),
                    "Item_Name": item.get("name"),
                    "Item_Description": item.get("description"),
                    "Item_Price": float(item.get("priceInMinorUnit", 0)) / 100,
                    "Item_Discounted_Price": float(item.get("discountedPriceInMin", 0)) / 100,
                    "Item_Image_URL": image_list,
                    "Item_Thumbnail_URL": [item.get("thumbImages")[0] if item.get("thumbImages") else None],
                    "Item_Available":True if item.get("available") else False ,
                    "Is_Top_Seller": True if item.get("topSeller") else False,
                }


                products_list.append(temp_product_dict)

        Rest_Data["Restaurant_Details"] = restaurant_details
        Rest_Data["Menu_Items"] = products_list
        Rest(**Rest_Data)

        all_data.append(Rest_Data)
    return all_data



