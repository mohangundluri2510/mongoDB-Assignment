from database import movies_collection


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
