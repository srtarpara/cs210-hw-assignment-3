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
DB_PASSWORD = ""      # your MySQL root password ("" if empty)
DB_NAME = "music_db"


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

    single_songs = [
        ("Shine",   ("Alice",),     "2019-03-01", "Pop"),
        ("Echo",    ("Alice",),     "2020-05-10", "Pop"),
        ("Alone",   ("Bob",),       "2020-07-07", "Rock"),
        ("Noise",   ("Bob",),       "2021-01-01", "Rock"),
        ("Dreams",  ("Carla Duo",), "2020-02-02", "Indie"),
        ("Pulse",   ("Dave",),      "2022-09-09", "EDM"),
    ]
    load_single_songs(mydb, single_songs)

    albums = [
        ("Skyline",   "Alice",     "2020-08-01", ["Sky Intro", "Skyline", "Sky Outro"]),
        ("Roadtrip",  "Bob",       "2021-06-15", ["Start", "Middle", "End"]),
        ("Duo Debut", "Carla Duo", "2019-11-11", ["Track A", "Track B"]),
    ]
    load_albums(mydb, albums)

    users = ["u1", "u2", "u3", "u4"]
    load_users(mydb, users)

    ratings = [
        # 2019
        ("u1", ("Shine",    "Alice"),     5, "2019-03-05"),
        ("u2", ("Shine",    "Alice"),     4, "2019-03-06"),
        ("u3", ("Track A",  "Carla Duo"), 5, "2019-12-01"),
        ("u3", ("Track B",  "Carla Duo"), 4, "2019-12-02"),

        # 2020
        ("u1", ("Echo",     "Alice"),     3, "2020-05-20"),
        ("u3", ("Echo",     "Alice"),     4, "2020-05-21"),
        ("u2", ("Alone",    "Bob"),       5, "2020-08-08"),
        ("u3", ("Alone",    "Bob"),       4, "2020-08-09"),
        ("u1", ("Skyline",  "Alice"),     5, "2020-08-10"),

        # 2021
        ("u4", ("Noise",    "Bob"),       2, "2021-01-10"),
        ("u2", ("Start",    "Bob"),       5, "2021-07-01"),
        ("u2", ("Middle",   "Bob"),       4, "2021-07-02"),

        # 2022
        ("u4", ("Pulse",    "Dave"),      5, "2022-09-10"),
        ("u4", ("Skyline",  "Alice"),     4, "2022-09-11"),
    ]
    load_song_ratings(mydb, ratings)


# ===========================
# EXPECTED VALUES
# ===========================

def expected_most_prolific_individual_artists():
    return [
        ("Alice", 5),
        ("Bob",   5),
        ("Dave",  1),
    ]


def expected_last_single_2020():
    return {"Alice", "Bob", "Carla Duo"}


def expected_last_single_2021():
    return {"Bob"}


def expected_top_genres():
    return [
        ("Unknown", 8),
        ("Pop", 2),
        ("Rock", 2),
    ]


def expected_album_and_single():
    return {"Alice", "Bob", "Carla Duo"}


def expected_top_rated_songs():
    return [
        ("Shine",   "Alice", 2),
        ("Echo",    "Alice", 2),
        ("Alone",   "Bob",   2),
        ("Skyline", "Alice", 2),
    ]


def expected_engaged_2019():
    return [("u3", 2)]


def expected_engaged_2022():
    return [("u4", 2)]


# ===========================
# MAIN TEST RUNNER
# ===========================

def main():
    global TEST_PASSED, TEST_FAILED

    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )

    print("Clearing database...")
    clear_database(mydb)

    print("Loading dataset...")
    load_big_test_data(mydb)

    print("\nRunning tests...\n")

    run_test(
        "Most Prolific Individual Artists",
        get_most_prolific_individual_artists(mydb, 3, (2019, 2022)),
        expected_most_prolific_individual_artists(),
        unordered=True
    )

    run_test(
        "Last Single in 2020",
        get_artists_last_single_in_year(mydb, 2020),
        expected_last_single_2020()
    )

    run_test(
        "Last Single in 2021",
        get_artists_last_single_in_year(mydb, 2021),
        expected_last_single_2021()
    )

    run_test(
        "Top Genres",
        get_top_song_genres(mydb, 3),
        expected_top_genres(),
        unordered=True
    )

    run_test(
        "Artists with both albums and singles",
        get_album_and_single_artists(mydb),
        expected_album_and_single()
    )

    run_test(
        "Most Rated Songs (2019-2022)",
        get_most_rated_songs(mydb, (2019, 2022), 4),
        expected_top_rated_songs(),
        unordered=True
    )

    run_test(
        "Most Engaged Users (2019)",
        get_most_engaged_users(mydb, (2019, 2019), 1),
        expected_engaged_2019()
    )

    run_test(
        "Most Engaged Users (2022)",
        get_most_engaged_users(mydb, (2022, 2022), 1),
        expected_engaged_2022()
    )

    mydb.close()

    print("\n==============================")
    print(f"Tests Passed: {TEST_PASSED}")
    print(f"Tests Failed: {TEST_FAILED}")
    print("==============================\n")


if __name__ == "__main__":
    main()
