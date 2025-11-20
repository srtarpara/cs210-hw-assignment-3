-- ====================================================
-- Music Database Schema
-- File: music_db.sql
-- ====================================================

-- DROP TABLES IF THEY EXIST (to start fresh)
DROP TABLE IF EXISTS Rating;
DROP TABLE IF EXISTS SongGenre;
DROP TABLE IF EXISTS Song;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS User;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Artist;

-- ====================================================
-- Artist Table
-- ====================================================
CREATE TABLE Artist (
    artist_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- ====================================================
-- Genre Table
-- ====================================================
CREATE TABLE Genre (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

-- ====================================================
-- Album Table
-- ====================================================
CREATE TABLE Album (
    album_id INT AUTO_INCREMENT PRIMARY KEY,
    artist_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    release_date DATE NOT NULL,
    genre_id INT NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES Artist(artist_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES Genre(genre_id) ON DELETE RESTRICT,
    UNIQUE (artist_id, name)
);

-- ====================================================
-- Song Table
-- ====================================================
CREATE TABLE Song (
    song_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    artist_id INT NOT NULL,
    album_id INT NULL,
    release_date DATE NOT NULL,
    FOREIGN KEY (artist_id) REFERENCES Artist(artist_id) ON DELETE CASCADE,
    FOREIGN KEY (album_id) REFERENCES Album(album_id) ON DELETE SET NULL,
    UNIQUE (artist_id, title)
);

-- ====================================================
-- SongGenre Table (Many-to-Many relationship)
-- ====================================================
CREATE TABLE SongGenre (
    song_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (song_id, genre_id),
    FOREIGN KEY (song_id) REFERENCES Song(song_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES Genre(genre_id) ON DELETE CASCADE
);

-- ====================================================
-- User Table
-- ====================================================
CREATE TABLE User (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE
);

-- ====================================================
-- Rating Table
-- ====================================================
CREATE TABLE Rating (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    song_id INT NOT NULL,
    rating TINYINT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    rating_date DATE NOT NULL,
    FOREIGN KEY (user_id) REFERENCES User(user_id) ON DELETE CASCADE,
    FOREIGN KEY (song_id) REFERENCES Song(song_id) ON DELETE CASCADE,
    UNIQUE (user_id, song_id)
);
