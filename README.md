# cs210-hw-assignment-3
Schema Overview

```
+---------+                         +---------+                         +---------+
| Artist  | 1                     * | Album   | 1                     * | Song    |
+---------+                         +---------+                         +---------+
| artist_id PK                       | album_id PK                       | song_id PK
| name U                             | artist_id FK -------------------->| artist_id FK
                                     | name                              | title
                                     | release_date                      | album_id FK NULL
                                     | genre_id FK                       | release_date
                                                                         | U(artist_id, title)



+--------+                           +-------------+
| Genre  | 1                       * | SongGenre   |
+--------+                           +-------------+
| genre_id PK                         | song_id FK
| name U                              | genre_id FK
                                      | PK(song_id, genre_id)



+--------+                           +---------+
| User   | 1                       * | Rating  |
+--------+                           +---------+
| user_id PK                          | rating_id PK
| username U                          | user_id FK
                                      | song_id FK
                                      | rating (1–5)
                                      | rating_date
                                      | U(user_id, song_id)
```
