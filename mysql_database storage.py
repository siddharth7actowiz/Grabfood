import json
import time
import mysql.connector

# JSON file path
json_file = "D://Siddharth//Grabfood_unzip_load//GRABFOOD_MENU_2026_02_26.json"

st_time=time.time()
#Batch Function for batch processing
def batch_insert(cursor, insert_query, values_list, batch_size=200):
    total = len(values_list)
    count = 0

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = values_list[start:end]

        cursor.executemany(insert_query, batch)
        count += 1
    con.commit()
    return count
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
CREATE TABLE IF NOT EXISTS RESTAURANT_TABLE(
    iD INT AUTO_INCREMENT PRIMARY KEY,
    Restaurant_ID VARCHAR(50)  UNIQUE,
    Restaurant_Name VARCHAR(255) ,
    Branch_Name VARCHAR(100),
    Cuisine TEXT,
    Tip JSON,
    Timezone VARCHAR(50),
    ETA SMALLINT,  
    DeliveryOptions JSON,
    Rating DECIMAL(3,2),  
    Is_Open BOOLEAN,
    Currency_Code CHAR(3), 
    Currency_Symbol VARCHAR(10),
    Offers JSON,
    Timing_Everyday VARCHAR(150),
    INDEX idx_restaurant_id (Restaurant_ID)
);
""")

cursor.execute(
    '''
    CREATE TABLE IF NOT EXISTS MENU_ITEMS_TABLE(
    id INT AUTO_INCREMENT PRIMARY KEY,
    Restaurant_ID VARCHAR(50) ,
    Category_Name VARCHAR(255),
    Item_Id VARCHAR(50) NOT NULL,
    Item_Name VARCHAR(255) NOT NULL,
    Item_Description TEXT,
    Item_Price DECIMAL(10,2),
    Item_Discounted_Price DECIMAL(10,2),
    Item_Image_URL JSON,
    Item_Thumbnail_URL JSON,
    Item_Available BOOLEAN,
    Is_Top_Seller BOOLEAN,
    CONSTRAINT fk_restaurant
        FOREIGN KEY (Restaurant_ID)
        REFERENCES RESTAURANT_TABLE(Restaurant_ID)
        ON DELETE CASCADE
);
    
    
    '''
)

#Restranunt table insertinto query
insert_rest_values = """
INSERT INTO RESTAURANT_TABLE (
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
#menu table insertinto query
insert_menu_sql = """
 INSERT INTO MENU_ITEMS_TABLE (
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
#restaurant table values
rest_values = []

#menu table values
menu_values = []

for data in json_data:
    rest_details = data.get("Restaurant_Details", {})
    menu_items = data.get("Menu_Items", [])

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
    rest_values.append(values_rest_table)

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


 try:
        print("Starting Restaurant Batch Insert...")
        rest_batches = batch_insert(cursor, insert_rest_query, restaurant_values)

        print("Starting Menu Batch Insert...")
        menu_batches = batch_insert(cursor, insert_menu_query, menu_values)

        con.commit()

        print("Transaction Successful :white_check_mark:")
        print(f"Restaurant batches: {rest_batches}")
        print(f"Menu batches: {menu_batches}")

except Exception as e:
        con.rollback()
        print("Transaction Failed :x:")
        print("Error:", e)

finally:
        cursor.close()
        con.close()

    end_time = datetime.now()
    print("Execution Time:", end_time - start_time)

