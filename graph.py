from neo4j import GraphDatabase


def get_db(uri, username, password):
    return GraphDatabase.driver(uri, auth=(username, password))


def create_movie(db, movie):
    query = """
        CREATE (m:Movie {id: $id, year: $year, title: $title})
    """
    db.run(query, **movie)


def create_user_rating(db, movie, rating):
    query = "MERGE (u:User {id: $user_id})"
    db.run(query, user_id=rating['user_id'])

    query = """
        MATCH (m:Movie)
        WHERE m.id = $id
        CREATE (u)-[r:Rating {value: $value, date: date($date)}]->(m)
    """
    db.run(query, id=movie['id'], **rating)
