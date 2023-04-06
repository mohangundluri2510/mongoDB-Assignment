from comments_question import *
from movie_question import *
from theater_question import *
from database import  client

def main():
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

main()
client.close()