import pymongo
import subprocess
from datetime import datetime
from dateutil import parser
# Creating client
client = pymongo.MongoClient("mongodb://localhost:27017/")

db_name = "assignmentDB"
db = client[db_name]


# Collection names which need to created and needed
collection_names_list = ["comments_collection", "movies_collection", "theaters_collection", "users_collection"]


# file paths
comments_filepath = "/Users/mohangundluri/Desktop/mongoDB_Assignment/sample_mflix/comments.json"
movies_filepath = "/Users/mohangundluri/Desktop/mongoDB_Assignment/sample_mflix/movies.json"
theaters_filepath = "/Users/mohangundluri/Desktop/mongoDB_Assignment/sample_mflix/theaters.json"
users_filepath = "/Users/mohangundluri/Desktop/mongoDB_Assignment/sample_mflix/users.json"

# file paths added to list for the looping to add data inm the files to collections
json_file_path_list = [comments_filepath, movies_filepath, theaters_filepath, users_filepath]

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


for collection in collection_names_list:
    create_collection_if_not_exits(collection)


# Loading the data to the collections.
def import_documets_from_files(db_name, collection_name, file_path):
    subprocess.run(["mongoimport", "--db", db_name, "--collection", collection_name,'--file', file_path])

new_changes = False
if new_changes:
    # Loading all documents into the collections
    for i in range(len(json_file_path_list)):
        collection_name = collection_names_list[i]
        filepath = json_file_path_list[i]
        import_documets_from_files(db_name=db_name, collection_name=collection_name, file_path=filepath)


# Function to insert any new records into data we imported
def insert_record(col_no):

    if col_no == 1:
        record = {
            "name": input("Enter name: "),
            "email": input("Enter email: "),
            "moviesno": {"$oid": input("Enter movie no: ")},
            "text": input("Enter text: "),
            "date": datetime.datetime.utcnow()}
        comments_collection.insert_one(record)

    if col_no == 2:
        record ={
            "plot": input("Enter plot: "),
            "genres": [input("Enter generes: ") for i in range(int(input("Enter no of generes: ")))],
            "runtime": int(input("Enter the runtime: ")),
            "cast": [input("Enter actor: ") for i in range(int(input("Enter no of actors: ")))],
            "num_mflix_comments": int(input("Enter no of comments: ")),
            "title": input("Enter tittle: "),
            "fullplot": input("Enter full plot: "),
            "countries": [input("Enter country: ") for i in range(int(input("Enter no of countries: ")))],
            "released": parser.parse(input("Enter the date: ")),
            "directors": [input("Enter director: ") for i in range(int(input("Enter no of directors: ")))],
            "rated": input("Enter whether rated or unrated: "),
            "awards": {"wins": int(input("Enter no of awards won: ")),
                       "nominations": int(input("Enter no of nominatinos: ")),
                       "text": input("Enter text about awards: ")},
            "lastupdated": datetime.utcnow(),
            "year": int(input("Enter the year: ")), "imdb": {"rating": float(input("Enter the imdb rating: ")),
                                                             "votes": int(input("Enter no of votes in imdb: ")),
                                                             "id": int(input("Enter the id in imdb: "))},
            "type": input("Enter type: "),

            "tomatoes": {"viewer": {"rating": int(input("Enter the tomatoes rating: ")),
                                    "numReviews": int(input("Enter the no of reviews in tomato: ")),
                                    "meter": int(input("Enter the meter: "))}},
            "lastUpdated": datetime.utcnow()
        }

        movies_collection.insert_one(record)


# Fid top `N` movies - with the highest IMDB rating
def find_top_n_movies_with_the_highest_IMDB_rating():
    pipeline = [
        {"$match": {"imdb.rating": {"$ne": ""}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": int(input("Enter the value of N: "))},
        {"$project": {"_id": 0, "title": 1, "imbd.rating": 1}}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` movies - with the highest IMDB rating in a given year
def find_top_n_movies_with_the_highest_IMDB_rating_in_year(year):
    pipeline = [
        {"$match": {"$and": [{"imdb.rating": {"$ne": ""}}, {"year": year}]}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": int(input("Enter the value of N: "))},
        {"$project": {"_id": 0, "title": 1, "imbd.rating": 1, "year": 1}}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` movies - with highsst IMDB rating with number of votes > 1000
def find_top_n_movies_with_the_highest_IMDB_rating_and_votes_greater_than_1000():
    pipeline = [
        {"$match": {"$and": [{"imdb.rating": {"$ne": ""}}, {"imdb.votes": {"$gte": 1000}}]}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": int(input("Enter the value of N: "))},
        {"$project": {"_id": 0, "title": 1, "imbd.rating": 1, "imdb.votes": 1}}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` movies -with title matching a given pattern sorted by highest tomatoes ratings
def find_top_n_movies_with_title_matching_pattern_sorted_by_highest_tomatoes_ratings():
    pipeline = [
        {"$match": {"title": {"$regex": input("Enter the regex pattern: "), "$options": "i"}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": int(input("Enter the value of N: "))},
        {"$project": {"_id": 0, "title": 1, "rating": "$tomatoes.viewer.rating"}}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` directors -who created the maximum number of movies
def find_top_n_directors_with_maximum_no_of_movies():
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "noOfMovies": {"$sum": 1}}},
        {"$sort": {"noOfMovies": -1}},
        {"$limit": int(input("Enter the value of N: "))}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` directors -who created the maximum number of movies in a given year
def find_top_n_directors_who_created_maximum_no_of_movies_in_an_year(year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "noOfMovies": {"$sum": 1}}},
        {"$sort": {"noOfMovies": -1}},
        {"$limit": int(input("Enter the value of N: "))}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` directors -who created the maximum number of movies for a given genre
def find_top_n_directors_who_created_maximum_no_of_movies_in_given_genre(genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "noOfMovies": {"$sum": 1}}},
        {"$sort": {"noOfMovies": -1}},
        {"$limit": int(input("Enter the value of N: "))}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` actors - who starred in the maximum number of movies
def find_top_n_actors_with_maximum_no_of_movies():
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "noOfMovies": {"$sum": 1}}},
        {"$sort": {"noOfMovies": -1}},
        {"$limit": int(input("Enter the value of N: "))}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` actors - who starred in the maximum number of movies in a given year
def find_top_n_actors_with_maximum_no_of_movies_in_given_year(year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "noOfMovies": {"$sum": 1}}},
        {"$sort": {"noOfMovies": -1}},
        {"$limit": int(input("Enter the value of N: "))}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` actors -who starred in the maximum number of movies for a given genre
def find_top_n_actors_with_maximum_no_of_movies_in_give_genre(genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "noOfMovies": {"$sum": 1}}},
        {"$sort": {"noOfMovies": -1}},
        {"$limit": int(input("Enter the value of N: "))}
    ]
    print(list(movies_collection.aggregate(pipeline=pipeline)))


# Find top `N` movies for each genre with the highest IMDB rating
def top_n_movies_for__every_genre():
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres"}}
    ]
    for document in list(movies_collection.aggregate(pipeline=pipeline)):
        genre = document['_id']
        print("Genre: " + genre)
        pipeline = [
            {"$unwind": "$genres"},
            {"$match": {"genres": genre}},
            {"$sort": {"imdb.rating": -1}},
            {"$match": {"imdb.rating": {"$ne": ""}}},
            {"$project": {"_id": 0, "title": 1, "rating": "$imdb.rating"}},
            {"$limit": int(input("Enter the value of N: "))}
        ]
        print(list(movies_collection.aggregate(pipeline=pipeline)))



# Find top 10 users who made the maximum number of comments
def top10_users_with_max_number_of_comments():
    pipeline = [
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "User": "$_id", "count": 1}}
    ]
    return comments_collection.aggregate(pipeline=pipeline)


# Find top 10 movies with most comments
def top10_movies_With_most_comment():
    pipeline = [
        {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    documents = comments_collection.aggregate(pipeline=pipeline)
    for document in documents:
        print(f"Title {movies_collection.find_one({'_id': document['_id']})['title']} Number of comments {document['count']}")


# Given a year find the total number of comments created each month in that year
def month_wise_comment(year):
    pipeline = [
        {"$project": {"year": {"$year": "$date"}, "month": {"$month": "$date"}}},
        {"$match": {"year": year}},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$project": {"month": "$_id", "count": 1, "_id": 0}},
        {"$sort": {"month": 1}}
    ]
    print(list(comments_collection.aggregate(pipeline=pipeline)))


# Top 10 cities with the maximum number of theatres
def top10_cities_most_theaters():
    pipeline = [
        {"$group": {"_id": "$location.address.city", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": -1}},
        {"$limit": 10}
    ]
    print(list(theaters_collection.aggregate(pipeline=pipeline)))


# top 10 theatres nearby given coordinates
def top10_theaters_near(coordinates):
    theaters_collection.create_index([("location.geo", "2dsphere")])
    query = {
        "location.geo": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": coordinates
                }
            }
        }
    }
    top10_theater_near_documents_list = theaters_collection.find(query).limit(10)
    print(top10_theater_near_documents_list)

# Running all the functions
top10_users_with_max_number_of_comments()
top10_movies_With_most_comment()
year = int(input("Enter the year\n"))
month_wise_comment(year=year)


find_top_n_movies_with_the_highest_IMDB_rating()
year = int(input("Enter the year\n"))
find_top_n_movies_with_the_highest_IMDB_rating_in_year(year)
find_top_n_movies_with_the_highest_IMDB_rating_and_votes_greater_than_1000()
find_top_n_movies_with_title_matching_pattern_sorted_by_highest_tomatoes_ratings()
find_top_n_directors_with_maximum_no_of_movies()
year = int(input("Enter the year\n"))
find_top_n_directors_who_created_maximum_no_of_movies_in_an_year(year)
genre = input("Enter gener\n")
find_top_n_directors_who_created_maximum_no_of_movies_in_given_genre(genre)
find_top_n_actors_with_maximum_no_of_movies()
year = int(input("Enter the year\n"))
find_top_n_actors_with_maximum_no_of_movies_in_given_year(year)
year = int(input("Enter the year\n"))
find_top_n_actors_with_maximum_no_of_movies_in_give_genre(genre)
top_n_movies_for__every_genre()


top10_cities_most_theaters()
coordinates = input("coordinates")
top10_theaters_near(coordinates=coordinates)


client.close()