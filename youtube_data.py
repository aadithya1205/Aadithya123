import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Positive011205?",
  database="youtube_data"
)
mycursor=mydb.cursor()
def create_channel():
     mycursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                Channel_Name VARCHAR(255),
                Channel_Id VARCHAR(255),
                Subscription_Count INT,
                Channel_Views INT,
                Channel_Description TEXT,
                Total_Video_Count INT,
                Playlist_Id VARCHAR(255)
            )
        """)
        mydb.commit()
def create_videos():
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                Channel_name VARCHAR(255),
                Channel_id VARCHAR(255),
                video_id VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                tags TEXT[],
                publishedAt TIMESTAMP,
                thumbnail_url VARCHAR(255),
                viewCount INTEGER,
                likeCount INTEGER,
                favoriteCount INTEGER,
                commentCount INTEGER,
                duration INTERVAL,
                definition VARCHAR(20),
                caption VARCHAR(20)
            )
        """)
        mydb.commit()
def create_comments():

        mycursor.execute("""
                       CREATE TABLE IF NOT EXISTS comments (
                           Video_Id VARCHAR(255),
                           Comment_Id VARCHAR(255),
                           Comment_Text TEXT,
                           Comment_Author VARCHAR(255),
                           Comment_Published_At TIMESTAMP
                       )
                   """)
        mydb.commit()
create_channel()
create_videos()
create_comments()
