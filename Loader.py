from neo4j import GraphDatabase
from datetime import datetime
import re

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "bartek"))
numbers_of_ratings = 17770
path_for_training_sets = r"C:\Users\Bartek\Desktop\Magisterka\nf_prize_dataset\download\training_set\training_set"
path_for_movies = r"C:\Users\Bartek\Desktop\Magisterka\/nf_prize_dataset\download\movie_titles.txt"


def add_movies(tx, movie):
    parts_of_line_movie = movie.split(',')

    if "NULL" in parts_of_line_movie[1]:
        year_of_production = parts_of_line_movie[1]
    else:
        year_of_production = int(parts_of_line_movie[1])

    tx.run("MERGE (m:Movie {id: $movie_data_1, year: $movie_data_2, title: $movie_data_3})",
           movie_data_1=int(parts_of_line_movie[0]), movie_data_2=year_of_production, movie_data_3=parts_of_line_movie[2].strip("\n"))


def add_movie_and_person(tx, id_movie, rate):
    parts_of_line_rate = rate.split(",")
    # print(id_movie)
    # print(rate)
    tx.run("MATCH (m:Movie {id: $id_movie})"
           "MERGE (p:Person {id: $rate_data_0, name: $rate_data_0_str})"           
           "MERGE (p)-[:RATE {rate: $rate_data_1, yearOfRate: $rate_data_2}]->(m)",
           rate_data_0=int(parts_of_line_rate[0].strip("\n")), rate_data_0_str = parts_of_line_rate[0].strip("\n"), rate_data_1=int(parts_of_line_rate[1].strip("\n")), rate_data_2=datetime.strptime(parts_of_line_rate[2].strip("\n"), '%Y-%m-%d').date(), id_movie=id_movie)


def title_read(tx, path):
    file = open(path, "r")

    for line in file:
        add_movies(tx, line)

    file.close()


def rate_read(tx, file_name_number):
    file_name_number2 = "17767"
    file2 = open(path_for_training_sets+"\mv_00" + file_name_number + ".txt", "r")
    id_movie = int(re.search(r'\d+', file2.readline()).group())

    for line in file2:
        add_movie_and_person(tx,id_movie,line)

    file2.close()


with driver.session() as session:
      session.write_transaction(title_read, path_for_movies)
      for val in range(1, numbers_of_ratings+1):
        session.write_transaction(rate_read, str(val).zfill(5))
      session.close()

