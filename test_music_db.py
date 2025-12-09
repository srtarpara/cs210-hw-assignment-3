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
DB_PASSWORD = ""      # your MySQL password
DB_NAME = "music_db"  # your DB name


# ===========================
# TEST FRAMEWORK
# ===========================

TEST_PASSED = 0
TEST_FAILED = 0

def normalize_unordered(x):
    if isinstance(x, set):
        return x
    if isinstance(x, list):
        return set(x) # Convert list to set for unordered comparison
    return set(x)

def show_result(name, actual, expected, pass_fail):
    """
    Always show the expected vs actual so the user can see
    exactly what was produced vs what was required.
    """
    print(f"\n=== {name} ===")
    print(f"EXPECTED: {expected}")
    print(f"ACTUAL:   {actual}")
    print(f"RESULT: {pass_fail}")
    print("-" * 40)

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
# DATA LOADING
# ===========================

def load_base_data(mydb):
    """
    Loads the base valid data for the query tests.
    """
    print("--- Loading Base Data ---")
    
    # 1. Singles
    # Alice: Shine, Echo
    # Bob: Alone, Noise
    # Carla Duo: Dreams
    # Dave: Pulse (Single Only Artist)
    single_songs = [
        ("Shine",   ("Pop",),     "Alice",     "2019-03-01"),
        ("Echo",    ("Pop",),     "Alice",     "2020-05-10"),
        ("Alone",   ("Rock",),    "Bob",       "2020-07-07"),
        ("Noise",   ("Rock",),    "Bob",       "2021-01-01"),
        ("Dreams",  ("Indie",),   "Carla Duo", "2020-02-02"),
        ("Pulse",   ("EDM",),     "Dave",      "2022-09-09"),
    ]
    load_single_songs(mydb, single_songs)

    # 2. Albums
    # Alice: Skyline (Contains "Skyline")
    # Bob: Roadtrip (Contains "Start")
    # Carla Duo: Duo Debut
    # Eve: Eve's Debut (Album Only Artist - NO SINGLES)
    albums = [
        ("Skyline",   "Pop",   "Alice",     "2020-08-01", ["Sky Intro", "Skyline", "Sky Outro"]),
        ("Roadtrip",  "Rock",  "Bob",       "2021-06-15", ["Start", "Middle", "End"]),
        ("Duo Debut", "Indie", "Carla Duo", "2019-11-11", ["Track A", "Track B"]),
        ("Eve Debut", "Jazz",  "Eve",       "2021-01-01", ["Eve Intro", "Eve Outro"]),
    ]
    load_albums(mydb, albums)

    # 3. Users
    users = ["u1", "u2", "u3", "u4"]
    load_users(mydb, users)

    # 4. Ratings
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
    load_song_ratings(mydb, ratings)


def test_album_rejection_logic(mydb):
    """
    SPECIFIC TEST FOR AUTOGRADER FAILURES (Tests 4, 5, 6 in load_albums).
    Tries to load albums that contain duplicate songs.
    """
    print("\n--- Running Special Test: Album Rejection Logic ---")
    
    # 1. Test: Album containing a song that is already a SINGLE
    # Alice already has single "Shine".
    # We try to load an album "Bad Album 1" that contains "Shine".
    # EXPECTED: Entire album rejected.
    
    bad_album_1 = ("Bad Album 1", "Pop", "Alice", "2023-01-01", ["New Song 1", "Shine"])
    
    # 2. Test: Album containing a song that is already in ANOTHER ALBUM
    # Bob already has album "Roadtrip" containing "Start".
    # We try to load an album "Bad Album 2" that contains "Start".
    # EXPECTED: Entire album rejected.
    
    bad_album_2 = ("Bad Album 2", "Rock", "Bob", "2023-01-01", ["Start", "New Song 2"])

    # 3. Test: Valid Album (Control group)
    # Alice releasing a totally new album.
    valid_album = ("Good Album", "Pop", "Alice", "2023-02-01", ["Brand New 1", "Brand New 2"])

    albums_to_load = [bad_album_1, bad_album_2, valid_album]
    
    # Actual Call
    rejected_set = load_albums(mydb, albums_to_load)
    
    # Assertions
    expected_rejects = {
        ("Bad Album 1", "Alice"),
        ("Bad Album 2", "Bob"),
    }
    
    run_test("Test 4–6: Album Rejection Logic (Bad Album 1 & 2 rejected, Good Album accepted)",
             rejected_set,
             expected_rejects,
             unordered=True)


# ===========================
# LOADER TESTS
# ===========================

def test_load_single_songs(mydb):
    """
    Covers:
      - Loading one song with a single genre
      - Loading songs with multiple genres
      - Duplicate song rejection
      - Bulk loading list of songs
    """
    print("\n--- load_single_songs Tests ---")

    # Test 1: Loading one song with a single genre
    songs = [
        ("Shine", ("Pop",), "Alice", "2019-03-01"),
    ]
    rejects = load_single_songs(mydb, songs)
    run_test("load_single_songs – Test 1: one song, single genre (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 2: Loading one song with multiple genres
    songs = [
        ("Fusion", ("Pop", "Rock"), "Alice", "2019-04-01"),
    ]
    rejects = load_single_songs(mydb, songs)
    run_test("load_single_songs – Test 2: one song, multiple genres (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 3: Another multi-genre song
    songs = [
        ("Indie Hit", ("Indie", "Pop"), "Carla Duo", "2020-01-01"),
    ]
    rejects = load_single_songs(mydb, songs)
    run_test("load_single_songs – Test 3: another multi-genre (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 5: Loading a duplicate song for an artist – should be rejected
    songs = [
        ("Shine", ("Pop",), "Alice", "2019-03-01"),  # duplicate
    ]
    rejects = load_single_songs(mydb, songs)
    run_test("load_single_songs – Test 5: duplicate (should be rejected)",
             len(rejects) if rejects is not None else rejects,
             1)

    # Test 6: Bulk loading a list of songs
    songs = [
        ("Sky", ("Pop",), "Alice", "2020-01-01"),
        ("Alone", ("Rock",), "Bob", "2020-02-02"),
        ("Dreams", ("Indie",), "Carla Duo", "2020-03-03"),
    ]
    rejects = load_single_songs(mydb, songs)
    # Depending on whether some of these already exist, you may want 0 or >0.
    # Here we simply assert that the function returns *something* iterable.
    run_test("load_single_songs – Test 6: bulk load (check type is not None)",
             rejects is not None,
             True)


def test_load_users(mydb):
    """
    Covers:
      - Loading single user
      - Loading multiple users
      - Duplicate users rejected
      - Bulk load
    """
    print("\n--- load_users Tests ---")

    # Test 1: Loading a single user
    rejects = load_users(mydb, ["u1"])
    run_test("load_users – Test 1: single user (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 2: Loading two users
    rejects = load_users(mydb, ["u2", "u3"])
    run_test("load_users – Test 2: two users (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 3: Loading three users including two duplicates
    # u1, u2 already exist; u4 is new
    rejects = load_users(mydb, ["u1", "u2", "u4"])
    # Expect 2 rejects (u1 & u2), and 1 inserted (u4)
    run_test("load_users – Test 3: duplicates rejected",
             len(rejects) if rejects is not None else rejects,
             2)

    # Test 4: Bulk loading a list of users
    rejects = load_users(mydb, ["u5", "u6", "u7"])
    run_test("load_users – Test 4: bulk load (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)


def test_load_song_ratings(mydb):
    """
    Covers:
      - Valid rating of a single
      - Valid rating of an album song
      - Rater not in DB
      - (artist, song) combo not in DB
      - Duplicate ratings rejected
      - Multiple rejects in one call
      - Out-of-bounds rating rejected
      - Bulk load of valid ratings
    """
    print("\n--- load_song_ratings Tests ---")

    # Prepare minimal data: one single, an album with songs, and users.
    load_single_songs(mydb, [("Shine", ("Pop",), "Alice", "2019-03-01")])
    load_albums(mydb, [("Skyline", "Pop", "Alice", "2020-08-01", ["Start", "End"])])
    load_users(mydb, ["ru1", "ru2", "ru3"])

    # Test 1: User rating a single
    ratings = [
        ("ru1", ("Alice", "Shine"), 5, "2020-01-01"),
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 1: rating a single (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 2: User rating an album song
    ratings = [
        ("ru1", ("Alice", "Start"), 4, "2020-01-02"),
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 2: rating an album song (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)

    # Test 3: Rater not in database
    ratings = [
        ("ghost", ("Alice", "Shine"), 3, "2020-01-03"),
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 3: rater not in DB (should reject)",
             len(rejects) if rejects is not None else rejects,
             1)

    # Test 4: Rater in database but (artist,song) combo not in database
    ratings = [
        ("ru1", ("Alice", "NotASong"), 4, "2020-01-04"),
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 4: non-existent (artist,song) (should reject)",
             len(rejects) if rejects is not None else rejects,
             1)

    # Test 5: User duplicating a rating for an (artist,song) combo
    # First, insert a valid rating:
    ratings = [
        ("ru2", ("Alice", "Shine"), 5, "2020-01-05"),
    ]
    load_song_ratings(mydb, ratings)
    # Second, duplicate it:
    ratings = [
        ("ru2", ("Alice", "Shine"), 4, "2020-01-06"),
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 5: duplicate rating (should reject)",
             len(rejects) if rejects is not None else rejects,
             1)

    # Test 6: Multiple rejects in a single call
    ratings = [
        ("ghost", ("Alice", "Shine"), 5, "2020-01-07"),          # invalid rater
        ("ru3", ("Alice", "NotASong"), 5, "2020-01-07"),        # invalid song
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 6: multiple rejects in one call",
             len(rejects) if rejects is not None else rejects,
             2)

    # Test 7: Out-of-bounds rating
    ratings = [
        ("ru1", ("Alice", "Shine"), 6, "2020-01-08"),  # assuming valid range is 1–5
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 7: out-of-bounds rating (should reject)",
             len(rejects) if rejects is not None else rejects,
             1)

    # Test 8: Bulk loading valid ratings
    ratings = [
        ("ru1", ("Alice", "End"), 4, "2020-01-09"),
        ("ru2", ("Alice", "End"), 5, "2020-01-10"),
        ("ru3", ("Alice", "End"), 3, "2020-01-11"),
    ]
    rejects = load_song_ratings(mydb, ratings)
    run_test("load_song_ratings – Test 8: bulk valid ratings (no rejects)",
             len(rejects) if rejects is not None else rejects,
             0)


# ===========================
# EXPECTED VALUES (UPDATED)
# ===========================

def expected_most_prolific_individual_artists():
    return [
        ("Alice", 2),
        ("Bob",   2),
        ("Carla Duo", 1),
        ("Dave",  1),
    ]

def expected_last_single_2020():
    return {"Alice", "Carla Duo"}

def expected_last_single_2021():
    return {"Bob"}

def expected_top_genres():
    # Pop: 2 singles + 3 (Skyline) + 2 (Good Album from rejection test) = 7
    # Note: The "Good Album" added in the rejection test adds 2 Pop songs!
    # Rock: 2 singles + 3 (Roadtrip) = 5
    # Indie: 1 single + 2 (Duo Debut) = 3
    # Jazz: 2 (Eve Debut) = 2
    # EDM: 1
    return [
        ("Pop",   7),
        ("Rock",  5),
        ("Indie", 3),
    ]

def expected_album_and_single_artists():
    # Alice: Single + Album
    # Bob: Single + Album
    # Carla Duo: Single + Album
    # Dave: Single Only
    # Eve: Album Only
    # Result should be ONLY those with BOTH
    return {"Alice", "Bob", "Carla Duo"}

def expected_most_rated_songs_2019_2022_top4():
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
# MAIN
# ===========================

def main():
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return

    print("Clearing DB...")
    clear_database(mydb)

    # 1. Loader tests on fresh DB(s)
    print("\n--------- load_single_songs ---------")
    test_load_single_songs(mydb)
    clear_database(mydb)

    print("\n--------- load_users ---------")
    test_load_users(mydb)
    clear_database(mydb)

    print("\n--------- load_song_ratings ---------")
    test_load_song_ratings(mydb)
    clear_database(mydb)

    # 2. Load base data for query tests
    print("\n--------- Base Data & load_albums ---------")
    load_base_data(mydb)

    # 3. Critical album rejection logic (corresponds to autograder Tests 4–6)
    test_album_rejection_logic(mydb)

    print("\n--- Running Query Tests ---")

    print("\n--------- get_most_prolific_individual_artists ---------")
    run_test(
        "Most Prolific Artists (2019–2022)",
        get_most_prolific_individual_artists(mydb, 4, (2019, 2022)),
        expected_most_prolific_individual_artists(),
        unordered=False,
    )

    print("\n--------- get_artists_last_single_in_year ---------")
    run_test(
        "Artists Last Single 2020",
        get_artists_last_single_in_year(mydb, 2020),
        expected_last_single_2020(),
        unordered=True,
    )

    print("\n--------- get_top_song_genres ---------")
    run_test(
        "Top Song Genres (3)",
        get_top_song_genres(mydb, 3),
        expected_top_genres(), # Note: Count updated to account for Good Album
        unordered=False,
    )

    print("\n--------- get_album_and_single_artists ---------")
    run_test(
        "Artists with BOTH Albums and Singles",
        get_album_and_single_artists(mydb),
        expected_album_and_single_artists(),
        unordered=True,
    )

    print("\n--------- get_most_rated_songs ---------")
    run_test(
        "Most Rated Songs",
        get_most_rated_songs(mydb, (2019, 2022), 4),
        expected_most_rated_songs_2019_2022_top4(),
        unordered=True,
    )

    print("\n--------- get_most_engaged_users ---------")
    run_test(
        "Most Engaged Users (2019)",
        get_most_engaged_users(mydb, (2019, 2019), 1),
        expected_engaged_2019(),
        unordered=False,
    )

    mydb.close()
    
    print("\n" + "="*30)
    print(f"PASSED: {TEST_PASSED}")
    print(f"FAILED: {TEST_FAILED}")
    print("="*30)

if __name__ == "__main__":
    main()
