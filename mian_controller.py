from load_parse import load_json,parser
from store_to_database import insert_into_database
from config import ZIP_PATH


def main():
    json_data = load_json(ZIP_PATH)
    validated_data = parser(json_data)
    insert_into_database(validated_data)

# Main Function Call
main()



