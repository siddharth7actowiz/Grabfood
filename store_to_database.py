import json
import time
import mysql.connector
from typing import *


#Batch Function
def batch_insert(cursor, con,insert_query: str, values: List[Tuple], BATCH_SIZE: int =100):

    total_records = len(values)
    batch_count = 0

    for start in range(0, total_records, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_records)
        batch = values[start:end]

        try:
            cursor.executemany(insert_query, batch)
            con.commit()
            batch_count += 1
            print(f"Inserted batch {batch_count} ({start} → {end})")
        except Exception as e:
            print(f"Batch failed ({start} → {end})")
            print("Error:", e)

    return batch_count


#create tble
def create_tables(cursor):

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS RESTAURANT_TABLE(
        iD INT AUTO_INCREMENT PRIMARY KEY,
        Restaurant_ID VARCHAR(50) UNIQUE,
        Restaurant_Name VARCHAR(255),
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS MENU_ITEMS_TABLE(
        id INT AUTO_INCREMENT PRIMARY KEY,
        Restaurant_ID VARCHAR(50),
        Category_Name VARCHAR(255),
        Item_Id VARCHAR(500) UNIQUE,
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
    """)


# getting values
def prepare_values(json_data):

    rest_values = []
    menu_values = []

    for data in json_data:

        rest_details = data.get("Restaurant_Details", {})
        menu_items = data.get("Menu_Items", [])

        rest_values.append((
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
        ))

        for item in menu_items:
            menu_values.append((
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
            ))

    return rest_values, menu_values


# main insert function
def insert_into_database(json_data):

    start_time = time.time()

    con = mysql.connector.connect(
        user="root",
        password="actowiz",
        host="localhost",
        port=3306,
        database="grabfood"
    )

    cursor = con.cursor()

    try:
        create_tables(cursor)

        rest_values, menu_values = prepare_values(json_data)

        insert_rest_query = """
        INSERT INTO RESTAURANT_TABLE (
            Restaurant_ID, Restaurant_Name, Branch_Name, Cuisine,
            Tip, Timezone, ETA, DeliveryOptions, Rating,
            Is_Open, Currency_Code, Currency_Symbol,
            Offers, Timing_Everyday
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Restaurant_Name = VALUES(Restaurant_Name)
        """

        insert_menu_query = """
        INSERT  INTO  MENU_ITEMS_TABLE (
            Restaurant_ID, Category_Name, Item_Id, Item_Name,
            Item_Description, Item_Price, Item_Discounted_Price,
            Item_Image_URL, Item_Thumbnail_URL,
            Item_Available, Is_Top_Seller
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE Item_Id = VALUES(Item_Id)
        """

        # Restaurant Data
         rest_batches = batch_insert(cursor, con,insert_rest_query, rest_values)
        # Menu Items Data
         menu_batches = batch_insert(cursor,con, insert_menu_query, menu_values)


        print("Restaurant batches:", rest_batches)
        print("Menu batches:", menu_batches)
        print("Total Time:", time.time() - start_time)

    except Exception as e:
        con.rollback()
        print("Transaction Failed")
        print("Error:", e)

    finally:
        cursor.close()

        con.close()

