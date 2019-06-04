import os


def parse_rating(line):
    line = line.rstrip('\n')
    user_id, value, date = line.split(',')
    try:
        date = date
    except ValueError:
        date = None
    return {
        'user_id': int(user_id),
        'value': int(value),
        'date': date
    }


def get_ratings(movie_id, path):
    str_movie_id = str(movie_id).zfill(7)
    file_name = f'mv_{str_movie_id}.txt'
    file_path = os.path.join(path, 'training_set', file_name)
    with open(file_path, encoding='latin-1') as f:
        f.readline()  # Skip first line
        return [parse_rating(line) for line in f]


def parse_movie(line):
    line = line.rstrip('\n')
    id, year, title = line.split(',', maxsplit=2)
    try:
        year = int(year)
    except ValueError:
        year = 0
    return {
        'id': int(id),
        'year': year,
        'title': title,
    }


def open_movies(dataset_path):
    return open(
        os.path.join(dataset_path, 'movie_titles.txt'),
        encoding='latin-1'
    )
