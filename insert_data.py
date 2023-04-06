from database import comments_collection, movies_collection, theaters_collection, users_collection
from path_collection import collection_names_list


# some sample documents to insert into the collections
comments = [
    {"_id": "comment1", "text": "This movie was great!", "user_id": "user1", "movie_id": "movie1"},
    {"_id": "comment2", "text": "I didn't like this movie at all.", "user_id": "user2", "movie_id": "movie1"},
    {"_id": "comment3", "text": "This theater had terrible service.", "user_id": "user3", "theater_id": "theater1"}
]

movies = [
    {"_id": "movie1", "title": "The Matrix", "year": 1999, "genre": "Action"},
    {"_id": "movie2", "title": "Inception", "year": 2010, "genre": "Science Fiction"},
    {"_id": "movie3", "title": "The Godfather", "year": 1972, "genre": "Crime"}
]

theaters = [
    {"_id": "theater1", "name": "AMC", "location": "New York"},
    {"_id": "theater2", "name": "Cinemark", "location": "Los Angeles"},
    {"_id": "theater3", "name": "Regal", "location": "Chicago"}
]

users = [
    {"_id": "user1", "name": "Alice", "email": "alice@example.com"},
    {"_id": "user2", "name": "Bob", "email": "bob@example.com"},
    {"_id": "user3", "name": "Charlie", "email": "charlie@example.com"}
]


# Getting collection name from the user to load documents--
collection_name = input(f"Enter the collection name from {collection_names_list} or use "
                        f"{list(range(1, len(collection_names_list)+1))}to load the documents..\n")
# Insert the documents into the collections
if collection_name == "comments_collection":
    comments_collection.insert_many(comments)
elif collection_name == movies_collection:
    movies_collection.insert_many(movies)
elif collection_name == "theaters_collection":
    theaters_collection.insert_many(theaters)
elif collection_name == "users_collection":
    users_collection.insert_many(users)
else:
    print('No collection in the database')

