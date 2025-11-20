from mysql.connector import connect
from typing import List, Tuple, Set

# ------------------------------
# Database connection (example)
# ------------------------------
# If you want to run this file standalone, you can create a connection like this:
# Otherwise, `mydb` can be passed into the functions externally.
mydb = connect(
    host="localhost",
    user="root",
    password="password",
    database="music_db"
)


# ------------------------------
# Populating the database
# ------------------------------

def clear_database(mydb):
    """
    Deletes all data from the database tables in the correct order to respect foreign keys.
    """
    cursor = mydb.cursor()
    tables = ['Rating', 'SongGenre', 'Song', 'Album', 'User', 'Genre', 'Artist']
    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
    mydb.commit()


def load_single_songs(mydb, single_songs: List[Tuple[str, Tuple[str, ...], str, str]]) -> Set[Tuple[str, str]]:
    cursor = mydb.cursor()
    inserted_songs = set()

    for title, artists, release_date, genre_name in single_songs:
        cursor.execute("SELECT genre_id FROM Genre WHERE name=%s", (genre_name,))
        result = cursor.fetchone()
        if result:
            genre_id = result[0]
        else:
            cursor.execute("INSERT INTO Genre (name) VALUES (%s)", (genre_name,))
            mydb.commit()
            genre_id = cursor.lastrowid

        for artist_name in artists:
            cursor.execute("SELECT artist_id FROM Artist WHERE name=%s", (artist_name,))
            res = cursor.fetchone()
            if res:
                artist_id = res[0]
            else:
                cursor.execute("INSERT INTO Artist (name) VALUES (%s)", (artist_name,))
                mydb.commit()
                artist_id = cursor.lastrowid

            cursor.execute(
                "INSERT IGNORE INTO Song (title, artist_id, album_id, release_date) VALUES (%s,%s,NULL,%s)",
                (title, artist_id, release_date)
            )
            mydb.commit()
            cursor.execute("SELECT song_id FROM Song WHERE title=%s AND artist_id=%s", (title, artist_id))
            song_id = cursor.fetchone()[0]

            cursor.execute(
                "INSERT IGNORE INTO SongGenre (song_id, genre_id) VALUES (%s,%s)",
                (song_id, genre_id)
            )
            mydb.commit()
            inserted_songs.add((title, artist_name))

    return inserted_songs


def load_albums(mydb, albums: List[Tuple[str, str, str, List[str]]]) -> Set[Tuple[str, str]]:
    cursor = mydb.cursor()
    inserted_albums = set()

    for album_name, artist_name, release_date, songs in albums:
        cursor.execute("SELECT artist_id FROM Artist WHERE name=%s", (artist_name,))
        res = cursor.fetchone()
        if res:
            artist_id = res[0]
        else:
            cursor.execute("INSERT INTO Artist (name) VALUES (%s)", (artist_name,))
            mydb.commit()
            artist_id = cursor.lastrowid

        genre_name = 'Unknown'
        cursor.execute("SELECT genre_id FROM Genre WHERE name=%s", (genre_name,))
        res = cursor.fetchone()
        if res:
            genre_id = res[0]
        else:
            cursor.execute("INSERT INTO Genre (name) VALUES (%s)", (genre_name,))
            mydb.commit()
            genre_id = cursor.lastrowid

        cursor.execute(
            "INSERT IGNORE INTO Album (name, artist_id, release_date, genre_id) VALUES (%s,%s,%s,%s)",
            (album_name, artist_id, release_date, genre_id)
        )
        mydb.commit()
        cursor.execute("SELECT album_id FROM Album WHERE name=%s AND artist_id=%s", (album_name, artist_id))
        album_id = cursor.fetchone()[0]

        for song_title in songs:
            cursor.execute(
                "INSERT IGNORE INTO Song (title, artist_id, album_id, release_date) VALUES (%s,%s,%s,%s)",
                (song_title, artist_id, album_id, release_date)
            )
            mydb.commit()
            cursor.execute("SELECT song_id FROM Song WHERE title=%s AND artist_id=%s", (song_title, artist_id))
            song_id = cursor.fetchone()[0]

            cursor.execute(
                "INSERT IGNORE INTO SongGenre (song_id, genre_id) VALUES (%s,%s)",
                (song_id, genre_id)
            )
            mydb.commit()

        inserted_albums.add((album_name, artist_name))

    return inserted_albums


def load_users(mydb, users: List[str]) -> Set[str]:
    cursor = mydb.cursor()
    inserted_users = set()
    for username in users:
        cursor.execute("INSERT IGNORE INTO User (username) VALUES (%s)", (username,))
        mydb.commit()
        inserted_users.add(username)
    return inserted_users


def load_song_ratings(mydb, song_ratings: List[Tuple[str, Tuple[str, str], int, str]]) -> Set[Tuple[str, str, str]]:
    cursor = mydb.cursor()
    inserted_ratings = set()

    for username, (song_title, artist_name), rating, rating_date in song_ratings:
        cursor.execute("SELECT user_id FROM User WHERE username=%s", (username,))
        user_id = cursor.fetchone()[0]

        cursor.execute(
            "SELECT s.song_id FROM Song s JOIN Artist a ON s.artist_id = a.artist_id "
            "WHERE s.title=%s AND a.name=%s",
            (song_title, artist_name)
        )
        song_id = cursor.fetchone()[0]

        cursor.execute(
            "INSERT IGNORE INTO Rating (user_id, song_id, rating, rating_date) VALUES (%s,%s,%s,%s)",
            (user_id, song_id, rating, rating_date)
        )
        mydb.commit()
        inserted_ratings.add((username, song_title, artist_name))

    return inserted_ratings


# ------------------------------
# Query functions
# ------------------------------

def get_most_prolific_individual_artists(mydb, n: int, year_range: Tuple[int, int]) -> List[Tuple[str, int]]:
    cursor = mydb.cursor()
    start_year, end_year = year_range
    cursor.execute(
        "SELECT a.name, COUNT(s.song_id) "
        "FROM Artist a JOIN Song s ON a.artist_id = s.artist_id "
        "WHERE a.name NOT LIKE '% %' AND YEAR(s.release_date) BETWEEN %s AND %s "
        "GROUP BY a.artist_id ORDER BY COUNT(s.song_id) DESC LIMIT %s",
        (start_year, end_year, n)
    )
    return cursor.fetchall()


def get_artists_last_single_in_year(mydb, year: int) -> Set[str]:
    cursor = mydb.cursor()
    cursor.execute(
        "SELECT DISTINCT a.name FROM Artist a "
        "JOIN Song s ON a.artist_id = s.artist_id "
        "WHERE s.album_id IS NULL AND YEAR(s.release_date) = %s",
        (year,)
    )
    return {row[0] for row in cursor.fetchall()}


def get_top_song_genres(mydb, n: int) -> List[Tuple[str, int]]:
    cursor = mydb.cursor()
    cursor.execute(
        "SELECT g.name, COUNT(sg.song_id) "
        "FROM Genre g JOIN SongGenre sg ON g.genre_id = sg.genre_id "
        "GROUP BY g.genre_id ORDER BY COUNT(sg.song_id) DESC LIMIT %s",
        (n,)
    )
    return cursor.fetchall()


def get_album_and_single_artists(mydb) -> Set[str]:
    cursor = mydb.cursor()
    # MySQL-compatible version using INNER JOIN on aggregated subqueries
    cursor.execute(
        "SELECT DISTINCT a.name "
        "FROM Artist a "
        "JOIN Song s1 ON a.artist_id = s1.artist_id AND s1.album_id IS NOT NULL "
        "JOIN Song s2 ON a.artist_id = s2.artist_id AND s2.album_id IS NULL"
    )
    return {row[0] for row in cursor.fetchall()}


def get_most_rated_songs(mydb, year_range: Tuple[int, int], n: int) -> List[Tuple[str, str, int]]:
    cursor = mydb.cursor()
    start_year, end_year = year_range
    cursor.execute(
        "SELECT s.title, a.name, COUNT(r.rating_id) "
        "FROM Song s "
        "JOIN Artist a ON s.artist_id = a.artist_id "
        "JOIN Rating r ON s.song_id = r.song_id "
        "WHERE YEAR(r.rating_date) BETWEEN %s AND %s "
        "GROUP BY s.song_id "
        "ORDER BY COUNT(r.rating_id) DESC LIMIT %s",
        (start_year, end_year, n)
    )
    return cursor.fetchall()


def get_most_engaged_users(mydb, year_range: Tuple[int, int], n: int) -> List[Tuple[str, int]]:
    cursor = mydb.cursor()
    start_year, end_year = year_range
    cursor.execute(
        "SELECT u.username, COUNT(r.rating_id) "
        "FROM User u "
        "JOIN Rating r ON u.user_id = r.user_id "
        "WHERE YEAR(r.rating_date) BETWEEN %s AND %s "
        "GROUP BY u.user_id "
        "ORDER BY COUNT(r.rating_id) DESC LIMIT %s",
        (start_year, end_year, n)
    )
    return cursor.fetchall()
