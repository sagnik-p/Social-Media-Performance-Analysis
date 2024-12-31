import os
from astrapy import DataAPIClient
from dotenv import load_dotenv


load_dotenv()
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
ASTRA_DB_URL = os.getenv("ASTRA_DB_URL")

client = DataAPIClient(ASTRA_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_URL)
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
if COLLECTION_NAME not in db.list_collection_names():
    db.create_collection(COLLECTION_NAME)

print(f"Connected to Astra DB, collections are: {db.list_collection_names()}")
