# MongoDb Peer learning
<hr>

## Aswat Bisht

### **In Datbase.py**
Used pymongo library to connect with mongodb and used subprocess library to run the mongoDb shell commands, created a db called **mydb** using the client **myclient** and created collections.


### **In comments.py**
Imported required collections from **database.py**, created a class **Comments** <br>
Functions in the class
* **addComment** This adds the one document to the commentsCollection
* **top10UserWithMaxComment** prints top 10 users with the max comment used aggregate function on **commentsCollection**
* **top10MoviesWithMaxComment** Prints top 10 movies with max comment used aggregate function on **commentsCollection** to get 10 documets 
* **monthWiseComment** Prints month wise comments aggregate function on **commentsCollection** to get documets and sorted by column month in ascending order

### **In movies.py**
Imported required collections from **database.py**, created a class **Movies** 
Functions in the class
**addMovies** Inserting document to the moviesCollection

**topNMoviesWith** 
* If choice is 1 prints the top n movies with the highest IMDB rating
* If choice is 2 prints the top n with the highest IMDB rating in a given year
* If choice is 3 prints the top n  with highest IMDB rating with number of votes > 1000
* If choice is 4  prints the top n with title matching a given pattern sorted by highest tomatoes ratings

**topNDirector**
* choice is 1 prints top n directors who created the maximum number of movies
* choice is 2 prints top n directors who created the maximum number of movies in a given year
* choice is 3 prints top n directors who created the maximum number of movies for a given genre

**topNActors**
* choice is 1 prints top n actors who starred in the maximum number of movies
* choice is 2 prints top n actors who starred in the maximum number of movies in a given year
* choice is 3 prints top n actors who starred in the maximum number of movies for a given genre

**topNMoviesForAGenre** Prints the top n movies for each genre with the highest IMDB rating


### **theaters.py**
Imported required collections from **database.py**, created a class **theaters** 
* **addMovies** This adds the one document to the theaterCollection
* **top10CitiesMostTheaters** prints top 10 cities with the most theaters
* **top10theatersNear** prints top 10 theaters near the given coordinates.

<hr>

## Shikhar Agarwal`s
### 1. Load
* He is using mongoimport and subprocess to load data to the file

### 2. Comments

* **query1()** function executes an aggregation query on the comments collection to group the comments by the commenter's name, sort them in descending order based on the count of comments, and limit the result to the top 10 commenters. The function then prints the result. <br>
* **query2()** function executes an aggregation query on the comments collection to group the comments by the movie_id field, sort them in descending order based on the count of comments, and limit the result to the top 10 movies. The function then prints the result.<br>
* **query3()** the function executes an aggregation query on the comments collection to group the comments by the month in which they were made and count the number of comments made in each month. The function then prints the result.<br>

### 3. Theaters

* **query11():** This function performs an aggregation query on the "theaters" collection to group the data by the city field in the location.address subdocument, count the number of documents in each group, sort the result in descending order of the count, and return only the top 10 results.<br>
* **query12():** This function performs a geospatial aggregation query using the $geoNear operator to find the documents in the "theaters" collection that are closest to a specified location, which is represented by a longitude-latitude pair.<br>

### 4. Movies

* **query11(n):** This function takes an integer n as input and returns the top n movies with highest IMDb ratings. It queries the MongoDB database myDB and sorts the movies by their imdb.rating field in descending order.
* **query12(year):** This function takes an integer year as input and returns the highest IMDb rating for movies released in that year. It queries the MongoDB database myDB and groups the movies by their year field, then finds the maximum imdb.rating for each group.​- query13(): This function returns the highest IMDb rating for movies with more than 10,000 votes. It queries the MongoDB database myDB and filters the movies by their imdb.rating and imdb.votes fields, then finds the maximum imdb.rating.
* **query14(pattern):** This function takes a string pattern as input and returns the top 10 movies whose titles match the regular expression pattern, sorted by their tomatoes.viewer.rating field in descending order. It queries the MongoDB database myDB and uses the $regex operator to match the title field with the input pattern, and then sorts the results by the tomatoes.viewer.rating field.
* **query21():** This function returns the number of movies directed by each director in the database. It queries the MongoDB database myDB, groups the movies by their directors field using $unwind and $group operators, and sorts the results by the count of movies for each director.
* **query22(year):** This function takes an integer year as input and returns the director who directed the most movies in that year. It queries the MongoDB database myDB, filters the movies by their year field, groups the movies by their directors field using $unwind and $group operators, and finds the director with the highest count of movies.
* **query23(genre):** This function takes a string genre as input and returns the director who directed the most movies in that genre. It queries the MongoDB database myDB, filters the movies by their genres field, groups the movies by their directors field using $unwind and $group operators, and finds the director with the highest count of movies.
* **query31(n):** This function takes an integer n as input and returns the top n most frequently appearing actors in the database. It queries the MongoDB database myDB, groups the movies by their cast field using $unwind and $group operators, and sorts the results by the count of movies for each actor.
* **query32(year,n):** This function takes an integer year and an integer n as input and returns the top n most frequently appearing actors in movies released in that year. It queries the MongoDB database myDB, filters the movies by their year field, groups the movies by their cast field using $unwind and $group operators, and sorts the results by the count of movies for each actor.
* **query33(genre,n):** This function takes a string genre and an integer n as input and returns the top n most frequently appearing actors in movies of that genre. It queries the MongoDB database myDB, filters the movies by their genres field, groups the movies by their cast field using $unwind and $group operators.
