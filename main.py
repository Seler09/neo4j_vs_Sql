import argparse

import graph
import parser


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
        '--password', default='neo4j',
        help='Database password'
    )
    p.add_argument(
        '--dataset-path', default='../dataset',
        help='Path to dataset'
    )
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    driver = graph.get_db(
        args.db_uri, args.username, args.password
    )
    with driver.session() as db, parser.open_movies(args.dataset_path) as f:
        for line in f:
            movie = parser.parse_movie(line)
            print(f'Creating movie in DB: {movie!r}')
            graph.create_movie(db, movie)
            ratings = parser.get_ratings(movie['id'], args.dataset_path)
            for r in ratings:
                graph.create_user_rating(db, movie, r)
