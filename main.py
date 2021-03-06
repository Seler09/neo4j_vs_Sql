import argparse
from datetime import datetime

import graph
import temp_parser
import rel
from datetime import datetime


def parse_args():
    p = argparse.ArgumentParser(description='Process some integers.')
    p.add_argument(
        '--db-uri', default='bolt://localhost:7687',
        help='Database uri'
    )
    p.add_argument(
        '--username', default='neo4j',
        help='Database username'
    )
    p.add_argument(
        '--password', default='bartek',
        help='Database password'
    )
    p.add_argument(
        '--dataset-path', default='../dataset',
        help='Path to dataset'
    )
    p.add_argument(
        '--max-ratings', type=int,
        help='Max number of ratings per movie'
    )
    p.add_argument(
        '--db-type', default='neo4j',
        help='Type of database (neo4j or postgresql)'
    )
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    # connectionSql = rel.try_conn()
    # with temp_parser.open_movies(args.dataset_path) as f:
    #     for line in f:
    #         movie = temp_parser.parse_movie(line)
    #         print(f'Creating movie in DB: {movie!r}')
    #         rel.create_movie_sql(connectionSql, movie)
    #         ratings = temp_parser.get_ratings(movie['id'], args.dataset_path)
    #        # user_id = 0
    #         if args.max_ratings:
    #             ratings = ratings[:args.max_ratings]
    #         for r in ratings:
    #             rel.create_rating_sql(connectionSql, movie, r) #, user_id
    # connectionSql.close()
    driver = graph.get_db(
        args.db_uri, args.username, args.password
    )
    with temp_parser.open_movies(args.dataset_path) as f:
        for line in f:

            start = datetime.now()
            movie = temp_parser.parse_movie(line)
            print(f'Creating movie in DB: {movie!r}')
            with driver.session() as db:
                graph.create_movie(db, movie)
                ratings = temp_parser.get_ratings(movie['id'], args.dataset_path)
                if args.max_ratings:
                    ratings = ratings[:args.max_ratings]
                for r in ratings:
                    graph.create_user_rating(db, movie, r)
                time = datetime.now() - start
                print(f'Done in {time.total_seconds()}s')

