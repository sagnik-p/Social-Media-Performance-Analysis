import os
from astrapy import DataAPIClient
from dotenv import load_dotenv
import csv

load_dotenv()
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
ASTRA_DB_URL = os.getenv("ASTRA_DB_URL")

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_URL)
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
collection = db.get_collection(COLLECTION_NAME)
print(f"Connected to Astra DB, collections are: {db.list_collection_names()}")


def read_and_insert_from_file(file_path):
    """
    Reads data from a CSV file and inserts it into the Astra DB collection.

    Args:
        file_path (str): Path to the CSV file containing the data.
    """
    try:
        with open(file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                # Validate and prepare data
                document = {
                    "post_id": row["post_id"],
                    "day_of_posting": row["day_of_posting"],
                    "date_of_posting": row["date_of_posting"],
                    "time_of_posting": row["time_of_posting"],
                    "post_type": row["post_type"],
                    "likes": int(row["likes"]),
                    "comments": int(row["comments"]),
                    "shares": int(row["shares"]),
                    "repost": int(row["repost"]),
                    "gender": row["gender"],
                    "hashtags": row["hashtags"].split(",") if row["hashtags"].strip() else []
                }

                # Insert into the collection
                collection.insert_one(document)

            print("Data successfully inserted into the database.")
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except KeyError as e:
        print(f"Error: Missing required column in the file: {e}")
    except ValueError as e:
        print(f"Error: Invalid data format: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


read_and_insert_from_file("dataset.csv")