import psycopg2


def try_conn():

    commands = (
        """
          CREATE TABLE IF NOT EXISTS Movie (
            id int PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            date VARCHAR(255)
        )
        """
    ,
        """
        CREATE TABLE IF NOT EXISTS Person (
        id int PRIMARY KEY,
        name VARCHAR(255) NOT NULL
        )
        """
    ,
        """
        CREATE TABLE IF NOT EXISTS Rate (
        id serial PRIMARY KEY,
        id_Movie INTEGER REFERENCES Movie (id),
        id_Person INTEGER REFERENCES Person (id),        
        rate VARCHAR(255) NOT NULL,
        date VARCHAR(255)
        )
        """
    )

    try:
        connection = psycopg2.connect(host = "127.0.0.1",
                                      database = "SQL",
                                      user = "postgres",
                                      password = "bartek",
                                      port = "5433")
    except:
        print("Unable to connect to the database")

    cursor = connection.cursor()

    for command in commands:
        print(command)
        cursor.execute(command)

    cursor.close()
    # commit the changes
    connection.commit()
    return connection


def create_movie_sql(connection, movie):
    cursor = connection.cursor()
    cursor.execute('INSERT INTO Movie (id, title, date) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',
                   (movie['id'], movie['title'], movie['year']))
    cursor.close()
    connection.commit()


def create_rating_sql(connection, movie, rating): #, user_id
    cursor = connection.cursor()
    cursor.execute('INSERT INTO Person (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                   (rating['user_id'], rating['user_id']))
    cursor.close()
    connection.commit()

    cursor = connection.cursor()
    cursor.execute('INSERT INTO Rate (id_Movie, id_Person, rate, date) VALUES (%s, %s, %s, %s)',
                   (movie['id'], rating['user_id'], rating['value'], rating['date']))
    cursor.close()
    connection.commit()


