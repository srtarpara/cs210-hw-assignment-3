import mysql.connector
from music_db import (
    clear_database,
    load_single_songs,
    load_albums,
    load_users,
    load_song_ratings,
    get_most_prolific_individual_artists,
    get_artists_last_single_in_year,
    get_top_song_genres,
    get_album_and_single_artists,
    get_most_rated_songs,
    get_most_engaged_users,
)

def main():
    # 1. Connect to the music_db database
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="music_db",
    )

    # 2. Clear any existing data (just for testing)
    print("Clearing database...")
    clear_database(mydb)

        # 3. Load some simple test data
    print("Loading test data...")

    # Single songs: (title, (artists...), release_date, genre_name)
    single_songs = [
        ("Single 1", ("Artist A",), "2020-01-01", "Pop"),
        ("Single 2", ("Artist B",), "2021-05-03", "Rock"),
    ]
    load_single_songs(mydb, single_songs)

    # Albums: (album_name, artist_name, release_date, [song titles])
    albums = [
        ("Album 1", "Artist A", "2021-01-01", ["Track 1", "Track 2"]),
    ]
    load_albums(mydb, albums)

    # Users: [username]
    users = ["u1", "u2"]
    load_users(mydb, users)

    # Ratings: (username, (song_title, artist_name), rating, rating_date)
    ratings = [
        ("u1", ("Single 1", "Artist A"), 5, "2021-02-02"),
        ("u2", ("Track 1", "Artist A"), 4, "2021-02-03"),
    ]
    load_song_ratings(mydb, ratings)

    # 4. Run the query functions and print results
    print("\nTesting queries:\n")

    print("Most prolific individual artists:")
    print(get_most_prolific_individual_artists(mydb, 5, (2019, 2022)))

    print("\nArtists' last single in 2021:")
    print(get_artists_last_single_in_year(mydb, 2021))

    print("\nTop song genres:")
    print(get_top_song_genres(mydb, 5))

    print("\nArtists with both albums and singles:")
    print(get_album_and_single_artists(mydb))

    print("\nMost rated songs (2020–2022):")
    print(get_most_rated_songs(mydb, (2020, 2022), 10))

    print("\nMost engaged users (2020–2022):")
    print(get_most_engaged_users(mydb, (2020, 2022), 10))

    mydb.close()

if __name__ == "__main__":
    main()