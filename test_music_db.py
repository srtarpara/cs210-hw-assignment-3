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

# ===========================
# DB CONFIG (EDIT THIS)
# ===========================

DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""      # your MySQL password ("" if none)
DB_NAME = "music_db"  # or whatever DB name you created/imported


# ===========================
# SIMPLE TEST FRAMEWORK
# ===========================

TEST_PASSED = 0
TEST_FAILED = 0


def normalize_unordered(x):
    if isinstance(x, set):
        return x
    return set(x)


def show_result(name, actual, expected, pass_fail):
    print(f"\n=== {name} ===")
    print(f"EXPECTED:\n  {expected}")
    print(f"ACTUAL:\n  {actual}")
    print(f"RESULT: {pass_fail}")
    print("=" * 40 + "\n")


def run_test(name, actual, expected, unordered=False):
    global TEST_PASSED, TEST_FAILED

    if unordered:
        actual_norm = normalize_unordered(actual)
        expected_norm = normalize_unordered(expected)
    else:
        actual_norm = actual
        expected_norm = expected

    if actual_norm == expected_norm:
        TEST_PASSED += 1
        show_result(name, actual, expected, "PASS")
    else:
        TEST_FAILED += 1        
        show_result(name, actual, expected, "FAIL")


# ===========================
# TEST DATA GENERATION
# ===========================

def load_big_test_data(mydb):
    """
    Loads a moderately large dataset using the *template semantics*.

    - Single songs: (title, (genres...), artist_name, release_date)
    - Albums: (album_title, genre_name, artist_name, release_date, [song_titles])
    - Ratings: (username, (artist_name, song_title), rating, rating_date)
    """

    # Single songs: (title, (genres...), artist_name, release_date)
    single_songs = [
        ("Shine",   ("Pop",),     "Alice",     "2019-03-01"),
        ("Echo",    ("Pop",),     "Alice",     "2020-05-10"),
        ("Alone",   ("Rock",),    "Bob",       "2020-07-07"),
        ("Noise",   ("Rock",),    "Bob",       "2021-01-01"),
        ("Dreams",  ("Indie",),   "Carla Duo", "2020-02-02"),
        ("Pulse",   ("EDM",),     "Dave",      "2022-09-09"),
    ]
    rejected_singles = load_single_songs(mydb, single_songs)
    if rejected_singles:
        print("WARNING: Unexpected rejected singles:", rejected_singles)

    # Albums: (album_title, genre_name, artist_name, release_date, [song_titles])
    albums = [
        ("Skyline",   "Pop",   "Alice",     "2020-08-01",
         ["Sky Intro", "Skyline", "Sky Outro"]),
        ("Roadtrip",  "Rock",  "Bob",       "2021-06-15",
         ["Start", "Middle", "End"]),
        ("Duo Debut", "Indie", "Carla Duo", "2019-11-11",
         ["Track A", "Track B"]),
    ]
    rejected_albums = load_albums(mydb, albums)
    if rejected_albums:
        print("WARNING: Unexpected rejected albums:", rejected_albums)

    # Users
    users = ["u1", "u2", "u3", "u4"]
    rejected_users = load_users(mydb, users)
    if rejected_users:
        print("WARNING: Unexpected rejected users:", rejected_users)

    # Ratings: (username, (artist_name, song_title), rating, rating_date)
    ratings = [
        # 2019
        ("u1", ("Alice",     "Shine"),   5, "2019-03-05"),
        ("u2", ("Alice",     "Shine"),   4, "2019-03-06"),
        ("u3", ("Carla Duo", "Track A"), 5, "2019-12-01"),
        ("u3", ("Carla Duo", "Track B"), 4, "2019-12-02"),

        # 2020
        ("u1", ("Alice",     "Echo"),    3, "2020-05-20"),
        ("u3", ("Alice",     "Echo"),    4, "2020-05-21"),
        ("u2", ("Bob",       "Alone"),   5, "2020-08-08"),
        ("u3", ("Bob",       "Alone"),   4, "2020-08-09"),
        ("u1", ("Alice",     "Skyline"), 5, "2020-08-10"),

        # 2021
        ("u4", ("Bob",       "Noise"),   2, "2021-01-10"),
        ("u2", ("Bob",       "Start"),   5, "2021-07-01"),
        ("u2", ("Bob",       "Middle"),  4, "2021-07-02"),

        # 2022
        ("u4", ("Dave",      "Pulse"),   5, "2022-09-10"),
        ("u4", ("Alice",     "Skyline"), 4, "2022-09-11"),
    ]
    rejected_ratings = load_song_ratings(mydb, ratings)
    if rejected_ratings:
        print("WARNING: Unexpected rejected ratings:", rejected_ratings)


# ===========================
# EXPECTED VALUES
# ===========================

def expected_most_prolific_individual_artists():
    """
    Singles only, in years 2019–2022.

    Singles:
        Alice: Shine (2019), Echo(2020) => 2
        Bob:   Alone(2020), Noise(2021) => 2
        Carla Duo: Dreams (2020)        => 1
        Dave:  Pulse(2022)              => 1

    Sorted by count desc, then name asc:
        1. ('Alice', 2)
        2. ('Bob', 2)
        3. ('Carla Duo', 1)  <-- Included now (C comes before D)
        4. ('Dave', 1)
    """
    return [
        ("Alice", 2),
        ("Bob",   2),
        ("Carla Duo", 1),
        ("Dave",  1),
    ]


def expected_last_single_2020():
    """
    Last single per artist:
        Alice: Shine(2019), Echo(2020)    -> last = 2020
        Bob:   Alone(2020), Noise(2021)   -> last = 2021
        Carla Duo: Dreams(2020)           -> last = 2020
        Dave:  Pulse(2022)                -> last = 2022

    So for year 2020: {'Alice', 'Carla Duo'}
    """
    return {"Alice", "Carla Duo"}


def expected_last_single_2021():
    """
    Only Bob's last single is in 2021.
    """
    return {"Bob"}


def expected_top_genres():
    """
    Genres from singles + album tracks:
        Pop:  2 (singles) + 3 (album) = 5
        Rock: 2 (singles) + 3 (album) = 5
        Indie:1 (single)  + 2 (album) = 3
        EDM:  1 (single)              = 1

    Top 3 sorted by count desc, then name asc:
        Pop(5), Rock(5), Indie(3)
    """
    return [
        ("Pop",   5),
        ("Rock",  5),
        ("Indie", 3),
    ]


def expected_album_and_single_artists():
    """
    Artists with both albums and singles:
        Alice: singles + Skyline
        Bob:   singles + Roadtrip
        Carla Duo: Dreams + Duo Debut
        Dave: only singles

    => {'Alice', 'Bob', 'Carla Duo'}
    """
    return {"Alice", "Bob", "Carla Duo"}


def expected_most_rated_songs_2019_2022_top4():
    """
    Top 4 rated songs (any order for test, but strict SQL order is desc, title asc):
        Shine(Alice):   2
        Echo(Alice):    2
        Alone(Bob):     2
        Skyline(Alice): 2
    """
    return [
        ("Shine",   "Alice", 2),
        ("Echo",    "Alice", 2),
        ("Alone",   "Bob",   2),
        ("Skyline", "Alice", 2),
    ]


def expected_engaged_2019():
    """
    Ratings in 2019:
        u1: 1
        u2: 1
        u3: 2 (Track A, Track B)
    Top 1: ('u3', 2)
    """
    return [("u3", 2)]


def expected_engaged_2022():
    """
    Ratings in 2022:
        u4: 2 (Pulse, Skyline)
    """
    return [("u4", 2)]


# ===========================
# MAIN TEST RUNNER
# ===========================

def main():
    global TEST_PASSED, TEST_FAILED

    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to DB: {err}")
        return

    print("Clearing database...")
    clear_database(mydb)

    print("Loading dataset...")
    load_big_test_data(mydb)

    print("\nRunning tests...\n")

    # ---- Query tests ----

    # Note: We ask for top 4 here to capture everyone, verifying order
    run_test(
        "Most Prolific Individual Artists (2019–2022)",
        get_most_prolific_individual_artists(mydb, 4, (2019, 2022)),
        expected_most_prolific_individual_artists(),
        unordered=False,
    )

    run_test(
        "Artists whose LAST single is in 2020",
        get_artists_last_single_in_year(mydb, 2020),
        expected_last_single_2020(),
        unordered=True,
    )

    run_test(
        "Artists whose LAST single is in 2021",
        get_artists_last_single_in_year(mydb, 2021),
        expected_last_single_2021(),
        unordered=True,
    )

    run_test(
        "Top Song Genres (3)",
        get_top_song_genres(mydb, 3),
        expected_top_genres(),
        unordered=False,
    )

    run_test(
        "Artists with both albums and singles",
        get_album_and_single_artists(mydb),
        expected_album_and_single_artists(),
        unordered=True,
    )

    run_test(
        "Most Rated Songs (2019–2022, top 4)",
        get_most_rated_songs(mydb, (2019, 2022), 4),
        expected_most_rated_songs_2019_2022_top4(),
        unordered=True,
    )

    run_test(
        "Most Engaged Users (2019)",
        get_most_engaged_users(mydb, (2019, 2019), 1),
        expected_engaged_2019(),
        unordered=False,
    )

    run_test(
        "Most Engaged Users (2022)",
        get_most_engaged_users(mydb, (2022, 2022), 1),
        expected_engaged_2022(),
        unordered=False,
    )

    mydb.close()

    print("\n==============================")
    print(f"Tests Passed: {TEST_PASSED}")
    print(f"Tests Failed: {TEST_FAILED}")
    print("==============================\n")


if __name__ == "__main__":
    main()