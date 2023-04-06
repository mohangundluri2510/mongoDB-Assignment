import pymongo
import subprocess

from path_collection import collection_names_list, json_file_path_list


# Creating client
client = pymongo.MongoClient("mongodb://localhost:27017/")

db_name = "assignmentDB"
db = client[db_name]

# Getting all collections
comments_collection = db.get_collection("comments_collection")
movies_collection = db.get_collection("movies_collection")
theaters_collection = db.get_collection("theaters_collection")
users_collection = db.get_collection("users_collection")


# creating a collection if not exits
def create_collection_if_not_exits(colletion_name):
    try:
        db.create_collection(colletion_name)
    except:
        print(f"Collection {colletion_name} is present in the database {db_name}")


# Creating collection if not exists in database by using above function create_collection_if_not_exits
for collection in collection_names_list:
    create_collection_if_not_exits(collection)


# Loading the data to the collections using subprocess
def import_documets_from_files(db_name, collection_name, file_path):
    subprocess.run(["mongoimport", "--db", db_name, "--collection", collection_name,'--file', file_path])


# To avoid loading again and again I have used flag new_changes, even though the documents not load in to collections
# but it takes some time to run the command;
new_changes = False

if new_changes:
    # Loading all documents into the collections
    for i in range(len(json_file_path_list)):
        collection_name = collection_names_list[i]
        filepath = json_file_path_list[i]
        import_documets_from_files(db_name=db_name, collection_name=collection_name, file_path=filepath)

