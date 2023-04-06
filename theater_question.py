from database import theaters_collection


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
