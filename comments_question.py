from database import comments_collection, movies_collection


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


# Find top 10 users who made the maximum number of comments
def top10_users_with_max_number_of_comments():
    pipeline = [
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
        {"$project": {"_id": 0, "User": "$_id", "count": 1}}
    ]
    return comments_collection.aggregate(pipeline=pipeline)
