import json
import mysql.connector

# JSON file path
json_file = "D:\\Siddharth\\grabfood\\GRABFOOD_MENU_2026_02_25.json"

# Load JSON
def load_validated_json(path):
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)

json_data = load_validated_json(json_file)

# Database connection
con = mysql.connector.connect(
    user="root",
    password="actowiz",
    host="localhost",
    port=3306,
    database="grabfood"
)

cursor = con.cursor()


cursor.execute("""
CREATE TABLE IF NOT EXISTS RESTAURANT(
    iid INT AUTO_INCREMENT PRIMARY KEY,
    Restaurant_ID VARCHAR(100) UNIQUE,
    Restaurant_Name VARCHAR(200),
    Branch_Name VARCHAR(50),
    Cuisine VARCHAR(50),
    Tip JSON,
    Timezone VARCHAR(50),
    ETA INT,
    DeliveryOptions JSON,
    Rating INT,
    Is_Open BOOLEAN,
    Currency_Code VARCHAR(50),
    Currency_Symbol VARCHAR(50),
    Offers JSON,
    Timing_Everyday VARCHAR(100)
)
""")


cursor.execute("""
CREATE TABLE IF NOT EXISTS MENU_ITEMS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    Restaurant_ID VARCHAR(100) ,
    Category_Name VARCHAR(255),
    Item_Id VARCHAR(100),
    Item_Name VARCHAR(255),
    Item_Description TEXT,
    Item_Price FLOAT,
    Item_Discounted_Price FLOAT,
    Item_Image_URL JSON,
    Item_Thumbnail_URL JSON,
    Item_Available BOOLEAN,
    Is_Top_Seller BOOLEAN,
    FOREIGN KEY (Restaurant_ID)
        REFERENCES RESTAURANT(Restaurant_ID) #make rest id unique in rest table
       
)
""")


insert_rest_values = """
INSERT INTO RESTAURANT (
    Restaurant_ID,
    Restaurant_Name,
    Branch_Name,
    Cuisine,
    Tip,
    Timezone,
    ETA,
    DeliveryOptions,
    Rating,
    Is_Open,
    Currency_Code,
    Currency_Symbol,
    Offers,
    Timing_Everyday
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

rest_details = json_data.get("Restaurant_Details", {})

values_rest_table = (
    rest_details.get("Restaurant_ID"),
    rest_details.get("Restaurant_Name"),
    rest_details.get("Branch_Name"),
    rest_details.get("Cuisine"),
    json.dumps(rest_details.get("Tip")),
    rest_details.get("Timezone"),
    rest_details.get("ETA"),
    json.dumps(rest_details.get("DeliveryOptions")),
    rest_details.get("Rating"),
    rest_details.get("Is_Open"),
    rest_details.get("Currency_Code"),
    rest_details.get("Currency_Symbol"),
    json.dumps(rest_details.get("Offers")),
    rest_details.get("Timing_Everyday"),
)



cursor.execute(insert_rest_values, values_rest_table)


menu_items = json_data.get("Menu_Items", [])

insert_menu_sql = """
INSERT INTO MENU_ITEMS (
    Restaurant_ID,
    Category_Name,
    Item_Id,
    Item_Name,
    Item_Description,
    Item_Price,
    Item_Discounted_Price,
    Item_Image_URL,
    Item_Thumbnail_URL,
    Item_Available,
    Is_Top_Seller
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

menu_values = []

for item in menu_items:
    values = (
        item.get("Restaurant_ID"),
        item.get("Category_Name"),
        item.get("Item_ID"),
        item.get("Item_Name"),
        item.get("Item_Description"),
        item.get("Item_Price"),
        item.get("Item_Discounted_Price"),
        json.dumps(item.get("Item_Image_URL")),
        json.dumps(item.get("Item_Thumbnail_URL")),
        item.get("Item_Available"),
        item.get("Is_Top_Seller"),
    )

    menu_values.append(values)

# Bulk insert with using execute many
cursor.executemany(insert_menu_sql, menu_values)

con.commit()
cursor.close()
con.close()