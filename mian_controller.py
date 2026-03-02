from load_parse import load_json,parser
from store_to_database import insert_into_database

zip_path=r"D:\Siddharth\Grabfood_unzip_load\grab_food_pages.zip"
def main():
    json_data = load_json(zip_path)
    validated_data = parser(json_data)
    insert_into_database(validated_data)


main()


