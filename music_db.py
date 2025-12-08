from typing import Tuple, List, Set

def clear_database(mydb):
    """
    Deletes all the rows from all the tables of the database.
    If a table has a foreign key to a parent table, it is deleted before 
    deleting the parent table, otherwise the database system will throw an error. 

    Args:
        mydb: database connection
    """
    cursor = mydb.cursor()
    # Deleting in order of Foreign Key dependencies (Child -> Parent)
    tables = ['Rating', 'SongGenre', 'Song', 'Album', 'User', 'Genre', 'Artist']
    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
    mydb.commit()

def load_single_songs(mydb, single_songs: List[Tuple[str,Tuple[str,...],str,str]]) -> Set[Tuple[str,str]]:
    """
    Add single songs to the database. 
    """
    cursor = mydb.cursor()
    rejected_songs = set()

    for title, genres, artist_name, release_date in single_songs:
        # 1. Get or Insert Artist
        cursor.execute("SELECT artist_id FROM Artist WHERE name=%s", (artist_name,))
        res = cursor.fetchone()
        if res:
            artist_id = res[0]
        else:
            cursor.execute("INSERT INTO Artist (name) VALUES (%s)", (artist_name,))
            mydb.commit()
            artist_id = cursor.lastrowid

        # 2. Insert Song (Single -> album_id is NULL)
        cursor.execute(
            "INSERT IGNORE INTO Song (title, artist_id, album_id, release_date) VALUES (%s,%s,NULL,%s)",
            (title, artist_id, release_date)
        )
        mydb.commit()

        # Check if insertion was ignored (duplicate)
        if cursor.rowcount == 0:
            rejected_songs.add((title, artist_name))
            continue 

        # Retrieve the new song_id
        cursor.execute("SELECT song_id FROM Song WHERE title=%s AND artist_id=%s", (title, artist_id))
        song_id = cursor.fetchone()[0]

        # 3. Handle Genres
        for genre_name in genres:
            cursor.execute("SELECT genre_id FROM Genre WHERE name=%s", (genre_name,))
            res = cursor.fetchone()
            if res:
                genre_id = res[0]
            else:
                cursor.execute("INSERT INTO Genre (name) VALUES (%s)", (genre_name,))
                mydb.commit()
                genre_id = cursor.lastrowid

            cursor.execute(
                "INSERT IGNORE INTO SongGenre (song_id, genre_id) VALUES (%s,%s)",
                (song_id, genre_id)
            )
            mydb.commit()

    return rejected_songs

def get_most_prolific_individual_artists(mydb, n: int, year_range: Tuple[int,int]) -> List[Tuple[str,int]]:   
    """
    Get the top n most prolific individual artists by number of singles released in a year range. 
    Break ties by alphabetical order of artist name.
    """
    cursor = mydb.cursor()
    start_year, end_year = year_range
    cursor.execute(
        "SELECT a.name, COUNT(s.song_id) as song_count "
        "FROM Artist a JOIN Song s ON a.artist_id = s.artist_id "
        "WHERE s.album_id IS NULL AND YEAR(s.release_date) BETWEEN %s AND %s "
        "GROUP BY a.artist_id "
        "ORDER BY song_count DESC, a.name ASC LIMIT %s",
        (start_year, end_year, n)
    )
    return cursor.fetchall()

def get_artists_last_single_in_year(mydb, year: int) -> Set[str]:
    """
    Get all artists who released their last single in the given year.
    """
    cursor = mydb.cursor()
    cursor.execute(
        "SELECT a.name "
        "FROM Artist a "
        "JOIN Song s ON a.artist_id = s.artist_id "
        "WHERE s.album_id IS NULL "
        "GROUP BY a.artist_id "
        "HAVING YEAR(MAX(s.release_date)) = %s",
        (year,)
    )
    return {row[0] for row in cursor.fetchall()}
    
def load_albums(mydb, albums: List[Tuple[str,str,str,str,List[str]]]) -> Set[Tuple[str,str]]:
    """
    Add albums to the database. 
    """
    cursor = mydb.cursor()
    rejected_albums = set()

    for album_name, genre_name, artist_name, release_date, songs in albums:
        # 1. Get or Insert Artist
        cursor.execute("SELECT artist_id FROM Artist WHERE name=%s", (artist_name,))
        res = cursor.fetchone()
        if res:
            artist_id = res[0]
        else:
            cursor.execute("INSERT INTO Artist (name) VALUES (%s)", (artist_name,))
            mydb.commit()
            artist_id = cursor.lastrowid
        
        # --- FIX START: CHECK FOR DUPLICATE SONGS BEFORE INSERTING ALBUM ---
        # The prompt implies if an artist already has a song with a title, 
        # they can't record it again. If this album contains such a song, reject the album.
        
        # Get all existing song titles for this artist
        cursor.execute("SELECT title FROM Song WHERE artist_id=%s", (artist_id,))
        existing_artist_songs = {row[0] for row in cursor.fetchall()}
        
        # Check if any song in the new album is already in the database for this artist
        has_duplicate_song = False
        for song in songs:
            if song in existing_artist_songs:
                has_duplicate_song = True
                break
        
        if has_duplicate_song:
            rejected_albums.add((album_name, artist_name))
            continue
        # --- FIX END ---

        # 2. Get or Insert Genre
        cursor.execute("SELECT genre_id FROM Genre WHERE name=%s", (genre_name,))
        res = cursor.fetchone()
        if res:
            genre_id = res[0]
        else:
            cursor.execute("INSERT INTO Genre (name) VALUES (%s)", (genre_name,))
            mydb.commit()
            genre_id = cursor.lastrowid

        # 3. Insert Album
        cursor.execute(
            "INSERT IGNORE INTO Album (name, artist_id, release_date, genre_id) VALUES (%s,%s,%s,%s)",
            (album_name, artist_id, release_date, genre_id)
        )
        mydb.commit()

        if cursor.rowcount == 0:
            rejected_albums.add((album_name, artist_name))
            continue 

        # Get album_id for linking songs
        cursor.execute("SELECT album_id FROM Album WHERE name=%s AND artist_id=%s", (album_name, artist_id))
        album_id = cursor.fetchone()[0]

        # 4. Insert Songs
        for song_title in songs:
            # We already checked for duplicates above, so these should insert fine.
            # But we still use INSERT IGNORE just in case.
            cursor.execute(
                "INSERT IGNORE INTO Song (title, artist_id, album_id, release_date) VALUES (%s,%s,%s,%s)",
                (song_title, artist_id, album_id, release_date)
            )
            mydb.commit()
            
            # Map song to the album's genre
            cursor.execute("SELECT song_id FROM Song WHERE title=%s AND artist_id=%s AND album_id=%s", 
                           (song_title, artist_id, album_id))
            song_res = cursor.fetchone()
            
            if song_res:
                song_id = song_res[0]
                cursor.execute(
                    "INSERT IGNORE INTO SongGenre (song_id, genre_id) VALUES (%s,%s)",
                    (song_id, genre_id)
                )
                mydb.commit()

    return rejected_albums

def get_top_song_genres(mydb, n: int) -> List[Tuple[str,int]]:
    """
    Get n genres that are most represented in terms of number of songs in that genre.
    """
    cursor = mydb.cursor()
    cursor.execute(
        "SELECT g.name, COUNT(sg.song_id) as song_count "
        "FROM Genre g JOIN SongGenre sg ON g.genre_id = sg.genre_id "
        "GROUP BY g.genre_id "
        "ORDER BY song_count DESC, g.name ASC LIMIT %s",
        (n,)
    )
    return cursor.fetchall()

def get_album_and_single_artists(mydb) -> Set[str]:
    """
    Get artists who have released albums as well as singles.
    """
    cursor = mydb.cursor()
    # Updated to use a cleaner subquery logic that is more robust
    cursor.execute(
        "SELECT name FROM Artist "
        "WHERE artist_id IN (SELECT artist_id FROM Song WHERE album_id IS NULL) "
        "AND artist_id IN (SELECT artist_id FROM Song WHERE album_id IS NOT NULL)"
    )
    return {row[0] for row in cursor.fetchall()}
    
def load_users(mydb, users: List[str]) -> Set[str]:
    """
    Add users to the database. 
    """
    cursor = mydb.cursor()
    rejected_users = set()
    for username in users:
        cursor.execute("INSERT IGNORE INTO User (username) VALUES (%s)", (username,))
        mydb.commit()
        if cursor.rowcount == 0:
            rejected_users.add(username)
    return rejected_users

def load_song_ratings(mydb, song_ratings: List[Tuple[str,Tuple[str,str],int, str]]) -> Set[Tuple[str,str,str]]:
    """
    Load ratings for songs, which are either singles or songs in albums. 
    """
    cursor = mydb.cursor()
    rejected_ratings = set()

    for username, (artist_name, song_title), rating, rating_date in song_ratings:
        # Condition (d): Check rating range
        if not (1 <= rating <= 5):
            rejected_ratings.add((username, artist_name, song_title))
            continue

        # Condition (a): Check username exists
        cursor.execute("SELECT user_id FROM User WHERE username=%s", (username,))
        user_res = cursor.fetchone()
        if not user_res:
            rejected_ratings.add((username, artist_name, song_title))
            continue
        user_id = user_res[0]

        # Condition (b): Check song (Artist, Title) exists
        cursor.execute(
            "SELECT s.song_id FROM Song s JOIN Artist a ON s.artist_id = a.artist_id "
            "WHERE s.title=%s AND a.name=%s",
            (song_title, artist_name)
        )
        song_res = cursor.fetchone()
        if not song_res:
            rejected_ratings.add((username, artist_name, song_title))
            continue
        song_id = song_res[0]

        # Condition (c): Check duplicate rating (handled via INSERT IGNORE)
        cursor.execute(
            "INSERT IGNORE INTO Rating (user_id, song_id, rating, rating_date) VALUES (%s,%s,%s,%s)",
            (user_id, song_id, rating, rating_date)
        )
        mydb.commit()

        if cursor.rowcount == 0:
            # Rating was ignored (likely because it already exists)
            rejected_ratings.add((username, artist_name, song_title))

    return rejected_ratings

def get_most_rated_songs(mydb, year_range: Tuple[int,int], n: int) -> List[Tuple[str,str,int]]:
    """
    Get the top n most rated songs in the given year range (both inclusive).
    """
    cursor = mydb.cursor()
    start_year, end_year = year_range
    cursor.execute(
        "SELECT s.title, a.name, COUNT(r.rating_id) as rating_count "
        "FROM Song s "
        "JOIN Artist a ON s.artist_id = a.artist_id "
        "JOIN Rating r ON s.song_id = r.song_id "
        "WHERE YEAR(r.rating_date) BETWEEN %s AND %s "
        "GROUP BY s.song_id "
        "ORDER BY rating_count DESC, s.title ASC LIMIT %s",
        (start_year, end_year, n)
    )
    return cursor.fetchall()

def get_most_engaged_users(mydb, year_range: Tuple[int,int], n: int) -> List[Tuple[str,int]]:
    """
    Get the top n most engaged users.
    """
    cursor = mydb.cursor()
    start_year, end_year = year_range
    cursor.execute(
        "SELECT u.username, COUNT(r.rating_id) as rating_count "
        "FROM User u "
        "JOIN Rating r ON u.user_id = r.user_id "
        "WHERE YEAR(r.rating_date) BETWEEN %s AND %s "
        "GROUP BY u.user_id "
        "ORDER BY rating_count DESC, u.username ASC LIMIT %s",
        (start_year, end_year, n)
    )
    return cursor.fetchall()

def main():
    pass

if __name__ == "__main__":
    main()