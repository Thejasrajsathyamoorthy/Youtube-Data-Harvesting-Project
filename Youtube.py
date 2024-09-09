import streamlit as st
from streamlit_option_menu import option_menu
from functools import partial
import re
from datetime import timedelta
import pandas as pd
import psycopg2
import googleapiclient.discovery

# Get credentials and create an API client
API_key = "AIzaSyDldtiEGL9hTJP3nDR5TOJRQjXkwC1xuzs"
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(
   api_service_name, api_version, developerKey= API_key)


# SQL Connection

mydb = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        port = "5432",
                        database = "youtube_data_harvesting",
                        password = "Thejas@4218")

cursor = mydb.cursor()


# Channel Details

def Channel_details(ch_id):
    ch_request = youtube.channels().list(
        part="snippet,statistics,contentDetails,status",
        id=f"{ch_id}"
        )
    channel_response = ch_request.execute()
    
    Channel_Data_table = {"Channel_Id": [],
                            "Channel_Name": [],
                                "Video_count": [],
                                "Subscribers_count": [],
                                    "Channel_views": [],
                                    "Channel_description": [],
                                        "Channel_status": [] }

    try:
        ch_name = channel_response['items'][0]['snippet']['title']
        ch_id = channel_response["items"][0]["id"]
        video_count = channel_response['items'][0]["statistics"]["videoCount"]
        subscribers_count = channel_response['items'][0]['statistics']['subscriberCount']
        ch_views = channel_response['items'][0]['statistics']['viewCount']
        ch_description = channel_response['items'][0]['snippet']['description']
        ch_status = channel_response['items'][0]['status']['privacyStatus']
        Channel_Data_table["Channel_Name"].append(ch_name)
        Channel_Data_table["Channel_Id"].append(ch_id)
        Channel_Data_table["Video_count"].append(video_count)
        Channel_Data_table["Subscribers_count"].append(subscribers_count)
        Channel_Data_table["Channel_views"].append(ch_views)
        Channel_Data_table["Channel_description"].append(ch_description)
        Channel_Data_table["Channel_status"].append(ch_status)

        Channel_Data = pd.DataFrame(Channel_Data_table)

        return Channel_Data

    except KeyError:
        print("An unexpected error occurred...Try Again")



# Playlist Details

def Playlist_details(ch_id):
    plst_request = youtube.playlists().list(
        part="snippet,contentDetails",
        maxResults=50,
        channelId=ch_id
    )
    plst_response = plst_request.execute()

    playlist_ids_table = { "Channel_Id":[],
                                "Playlist_ids": [],
                                    "Playlist_name":[] }

    for i in range(len(plst_response["items"])):
        playlist_ids_table["Channel_Id"].append(plst_response["items"][i]["snippet"]["channelId"])
        playlist_ids_table["Playlist_ids"].append(plst_response['items'][i]['id'])
        playlist_ids_table["Playlist_name"].append(plst_response["items"][i]["snippet"]["title"])
        
    Playlist_Data = pd.DataFrame(playlist_ids_table)

    return Playlist_Data



# Get Video Ids

def get_video_ids(ch_id):
    
    video_id = []

    ch_request = youtube.channels().list(
    part="snippet,statistics,contentDetails,status",
    id=f"{ch_id}"
    )
    channel_response = ch_request.execute()
    
    playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    next_page_token = None

    while True:
        plst_items_request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,
            playlistId=playlist_id,
            pageToken= next_page_token        
        )
        plst_items_response = plst_items_request.execute()


        for item in range(len(plst_items_response["items"])):
            video_id.append(plst_items_response["items"][item]["snippet"]["resourceId"]["videoId"])

        next_page_token = plst_items_response.get("nextPageToken")

        if next_page_token is None:
            break
    
    return video_id



# Video Details

def Video_details(Video_Ids):
    
    all_video_data = []
    Duration_in_seconds = []

    for vid_id in Video_Ids:
        vid_request = youtube.videos().list(
            part="snippet,contentDetails,statistics,status",
            id=vid_id
        )
        vid_response = vid_request.execute()


        video_item = vid_response["items"][0]
        
        video_data = {
            "Channel_Id": video_item["snippet"]["channelId"],
            "Video_Id": video_item["id"],
            "Video_Name": video_item["snippet"]["title"],
            "Video_description": video_item["snippet"]["description"],
            "Channel_Name":video_item["snippet"]["channelTitle"],
            "Video_publishedAt": video_item["snippet"]["publishedAt"],
            "Video_published_year": video_item["snippet"]["publishedAt"][0:4],
            "View_count": video_item["statistics"].get("viewCount", 0),
            "Like_count": video_item["statistics"].get("likeCount", 0),
            "Favourite_count": video_item["statistics"].get("favoriteCount", 0),
            "Comment_count": video_item["statistics"].get("commentCount", 0),
            "Video_duration": video_item["contentDetails"]["duration"],
            "Thumbnail_url": video_item["snippet"]["thumbnails"]["default"]["url"],
            "Caption_status": video_item["contentDetails"]["caption"],
            "Privacy_status": video_item["status"]["privacyStatus"]
        }

        Video_duration = video_item["contentDetails"]["duration"]


        hours_pattern = re.compile(r'(\d+)H')
        minutes_pattern = re.compile(r'(\d+)M')
        seconds_pattern = re.compile(r'(\d+)S')


        Hours = hours_pattern.search(Video_duration)
        Minutes = minutes_pattern.search(Video_duration)
        Seconds = seconds_pattern.search(Video_duration)

        Hours = int(Hours.group(1)) if Hours else 0
        Minutes = int(Minutes.group(1)) if Minutes else 0
        Seconds = int(Seconds.group(1)) if Seconds else 0

        video_duration_in_sec = timedelta(
            hours= Hours,
            minutes= Minutes,
            seconds= Seconds
        ).total_seconds()

        Duration_in_seconds.append(video_duration_in_sec)

   
        all_video_data.append(video_data)

        Video_data = pd.DataFrame(all_video_data)
        Video_duration_sec = pd.DataFrame({"Dur_in_sec" :Duration_in_seconds})

        Video_Data = pd.concat([Video_data, Video_duration_sec], axis=1)

        Video_Data["Video_publishedAt"] = Video_Data["Video_publishedAt"].str.replace("T", " ").str.replace("Z", " ")
        Video_Data["Video_duration"] = Video_Data["Video_duration"].str.replace("PT", "").str.replace("H"," Hr ").str.replace("M", " Min ").str.replace("S", " Sec")
        Video_Data["Favourite_count"] = Video_Data["Favourite_count"].replace('', '0')
        Video_Data["View_count"] = Video_Data["View_count"].replace('', '0')
        Video_Data["Like_count"] = Video_Data["Like_count"].replace('', '0')
        Video_Data["Comment_count"] = Video_Data["Comment_count"].replace('', '0')

    return Video_Data



# Comment Details

def Comment_details(Video_Ids):
    comment_details = []
    try:
        for video_id in Video_Ids:
            cmt_request = youtube.commentThreads().list(
                part="snippet,replies,id",
                maxResults=50,
                videoId=video_id
                
            )
            cmt_response = cmt_request.execute()

            for item in cmt_response["items"]:

                Cmt_data_table = {"Channel_id": item["snippet"]["channelId"],
                                        "Video_Id":item["snippet"]["videoId"],
                                            "Comment_Id":item.get("id"),
                                                "Author_Name":item.get("snippet",{}).get("topLevelComment",{}).get("snippet",{}).get("authorDisplayName"),
                                                    "Author_Channel_Id":item.get("snippet",{}).get("topLevelComment",{}).get("snippet",{}).get("authorChannelId",{}).get("value"),
                                                        "Comment":item.get("snippet",{}).get("topLevelComment",{}).get("snippet",{}).get("textOriginal"),
                                                            "Comment_publishedAt":item.get("snippet",{}).get("topLevelComment",{}).get("snippet",{}).get("publishedAt") }

                comment_details.append(Cmt_data_table)
                Comment_Data = pd.DataFrame(comment_details)



        Comment_Data["Comment_publishedAt"] = Comment_Data["Comment_publishedAt"].str.replace("T"," ")
        Comment_Data["Comment_publishedAt"] = Comment_Data["Comment_publishedAt"].str.replace("Z"," ")
        Comment_Data["Author_Name"] = Comment_Data["Author_Name"].str.replace("@","")   

        return Comment_Data

    except:
        pass


    


# Export to SQL

def export_to_sql(ch_id):
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            port = "5432",
                            database = "youtube_data_harvesting",
                            password = "Thejas@4218")

    cursor = mydb.cursor()

    Channel_Details = Channel_details(ch_id)
    Playlist_Details = Playlist_details(ch_id)
    Video_Ids = get_video_ids(ch_id)
    Video_Details = Video_details(Video_Ids)
    Comment_Details = Comment_details(Video_Ids)


    # Channel_Details
    try:
        try:
            create_ch_details = '''CREATE TABLE if not exists Channel_Details (Channel_Id varchar(255) PRIMARY KEY,
                                                                                Channel_Name varchar(255),
                                                                                Video_Count bigint,
                                                                                Subscribers_count bigint,
                                                                                Channel_views bigint,
                                                                                Channel_description varchar(1000),
                                                                                Channel_status varchar(255)
                                                                                )'''

            cursor.execute(create_ch_details)
            mydb.commit()



            insert_ch_details = '''INSERT INTO Channel_Details(Channel_ID, Channel_Name, Video_count, Subscribers_count,
                                                                                Channel_views, Channel_description, Channel_status)
                                                                                
                                                                                values(%s,%s,%s,%s,%s,%s,%s)'''

            ch_details_data = Channel_Details.values.tolist()
            cursor.executemany(insert_ch_details, ch_details_data)
            mydb.commit()

        except:
            pass



        # Playlist_Details

        create_plylst_details = '''CREATE TABLE if not exists Playlist_Details (Channel_Id varchar(250),
                                                                                    Playlist_Id varchar(255),
                                                                                    Playlist_Name varchar(500)
                                                                                    )'''

        cursor.execute(create_plylst_details)
        mydb.commit()


        insert_plylst_details = '''INSERT INTO Playlist_Details (Channel_Id, Playlist_Id, Playlist_Name)

                                                                    values (%s,%s,%s)'''

        plylst_details_data = Playlist_Details.values.tolist()
        cursor.executemany(insert_plylst_details, plylst_details_data)
        mydb.commit()


        # Video_Details

        create_vid_details = '''CREATE TABLE if not exists Video_Details (Channel_Id varchar(255),
                                                                            Video_Id varchar(255),
                                                                            Video_Name varchar(255),
                                                                            Video_description text,
                                                                            Channel_Name Varchar(255),
                                                                            Video_publishedAt varchar(255),
                                                                            Video_published_year varchar(255),
                                                                            View_count int,
                                                                            Like_count int,
                                                                            Favourite_count int,
                                                                            Comment_count int,
                                                                            Video_duration varchar(255),
                                                                            Thumbnail_url varchar(500),
                                                                            Caption_status varchar(255),
                                                                            Pivacy_status varchar(255),
                                                                            Dur_in_sec float
                                                                            )'''

        cursor.execute(create_vid_details)
        mydb.commit()

        insert_vid_details = '''INSERT INTO Video_Details (Channel_Id, Video_Id, Video_Name, Video_description, Channel_Name, Video_publishedAt, Video_published_year, View_count,
                                                                Like_count, Favourite_count, Comment_count, Video_duration,
                                                                Thumbnail_url,Caption_status, Pivacy_status, Dur_in_sec )
                                                                
                                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        video_details_data = Video_Details.values.tolist()
        cursor.executemany(insert_vid_details, video_details_data)
        mydb.commit()


        # Comment_Details

        try:
            create_cmt_details = '''CREATE TABLE if not exists Comment_Details (Channel_Id varchar(255),
                                                                                Video_Id varchar(255),
                                                                                Comment_Id varchar(255),
                                                                                Author_Name varchar(255),
                                                                                Author_Channel_Id varchar(255),
                                                                                Comment varchar(10000),
                                                                                Comment_publishedAt varchar(255)
                                                                                )'''

            cursor.execute(create_cmt_details)
            mydb.commit()


            insert_cmt_details = '''INSERT INTO Comment_Details (Channel_Id, Video_Id, Comment_Id, Author_Name, Author_Channel_Id,
                                                                    Comment, Comment_publishedAt)
                                                                    
                                                                    values(%s,%s,%s,%s,%s,%s,%s)'''

            cmt_details_data = Comment_Details.values.tolist()
            cursor.executemany(insert_cmt_details, cmt_details_data)
            mydb.commit()
        except:
            pass

        st.success("Successfully Loaded")

    except :
        st.info("This Channel Already exists")


# Video name and their channel name

def vid_ch_name():
    Vid_ch_name = '''SELECT Video_Name, Channel_Name 
                        From Video_Details;'''
    
    cursor.execute(Vid_ch_name)
    video_channel_name_table = cursor.fetchall()
    mydb.commit()

    Video_Channel_name_df = pd.DataFrame(video_channel_name_table, columns= ("Video_Name","Channel_Name"))

    st.dataframe(Video_Channel_name_df)


# Channel name and their video count

def ch_name_vid_count():
    Ch_name_Vid_count = '''SELECT Channel_Name,Video_count 
                                FROM Channel_Details
                                ORDER BY Video_count DESC
                                LIMIT 20;'''
    
    cursor.execute(Ch_name_Vid_count)
    Channel_name_Video_count_table = cursor.fetchall()
    mydb.commit()

    Channel_Video_count_name_df = pd.DataFrame(Channel_name_Video_count_table, columns= ("Channel_Name","Video_Count"))
    st.dataframe(Channel_Video_count_name_df)


# Top 10 most viewed videos and their channel name

def Top_10_most_viewed_videos():
    Top_10_most_viewed_videos = '''SELECT Video_Name, View_count, Channel_Name 
                                        FROM Video_Details
                                        ORDER BY View_Count DESC
                                        LIMIT 10;'''
    
    cursor.execute(Top_10_most_viewed_videos)
    Top_10_most_viewed_videos_table = cursor.fetchall()
    mydb.commit()

    Top_10_most_viewed_videos_df = pd.DataFrame(Top_10_most_viewed_videos_table, columns= ("Video_Name","View_Count","Channel_Name"))
    st.dataframe(Top_10_most_viewed_videos_df)


# Comment on each Video and their name

def Cmt_count_and_Vid_name():
    Cmt_count_and_Vid_name = '''SELECT Video_Name, Comment_count FROM Video_Details;'''

    cursor.execute(Cmt_count_and_Vid_name)
    Cmt_count_and_Vid_name_table = cursor.fetchall()
    mydb.commit()

    Cmt_count_and_Vid_name_df = pd.DataFrame(Cmt_count_and_Vid_name_table, columns= ("Video_Name","Comment_count"))
    st.dataframe(Cmt_count_and_Vid_name_df)



# Highest number of likes and channel name

def Highest_like_ch_and_vid_name():
    Highest_like_ch_and_vid_name = '''SELECT Video_Name, Like_count,Channel_Name 
                                    FROM Video_Details
                                    ORDER BY Like_Count DESC
                                    LIMIT 20;'''

    cursor.execute(Highest_like_ch_and_vid_name)
    Highest_like_ch_and_vid_name_table = cursor.fetchall()
    mydb.commit()

    Highest_like_ch_and_vid_name_df = pd.DataFrame(Highest_like_ch_and_vid_name_table, columns= ("Video_Name","Like_count","Channel_Name"))
    st.dataframe(Highest_like_ch_and_vid_name_df)


# Total Likes each video

def Total_likes_Video_name():
    Total_likes_Video_name = '''SELECT Video_Name, Like_count FROM Video_Details;'''

    cursor.execute(Total_likes_Video_name)
    Total_likes_Video_name_table = cursor.fetchall()
    mydb.commit()

    Total_likes_Video_name_df = pd.DataFrame(Total_likes_Video_name_table, columns= ("Video_Name","Like_count"))
    st.dataframe(Total_likes_Video_name_df)


# Total Channel Views

def Total_channel_views_and_name():
    Total_channel_views_and_name = '''SELECT Channel_Name, Channel_views FROM Channel_Details;'''

    cursor.execute(Total_channel_views_and_name)
    Total_channel_views_and_name_table = cursor.fetchall()
    mydb.commit()

    Total_channel_views_and_name_df = pd.DataFrame(Total_channel_views_and_name_table, columns= ("Channel_Name","View_count"))
    st.dataframe(Total_channel_views_and_name_df)



# Video published at 2022

def Video_publishedAt_2022():
    Video_publishedAt_2022 = '''SELECT Video_Name, Video_published_Year, Channel_Name FROM Video_Details WHERE Video_published_year='2022';'''

    cursor.execute(Video_publishedAt_2022)
    Video_publishedAt_2022_table = cursor.fetchall()
    mydb.commit()

    Video_publishedAt_2022_df = pd.DataFrame(Video_publishedAt_2022_table, columns= ("Video_Name","Video_published_Year","Channel_Name"))
    st.dataframe(Video_publishedAt_2022_df)



# Avg Duration of video
def Channel_name_avg_dur():
    Channel_name_avg_dur = '''SELECT Channel_Name, AVG(Dur_in_sec)/60 as Avg_dur_min FROM Video_Details GROUP BY Channel_Name;'''

    cursor.execute(Channel_name_avg_dur)
    Channel_name_avg_dur_table = cursor.fetchall()
    mydb.commit()

    Channel_name_avg_dur_df = pd.DataFrame(Channel_name_avg_dur_table, columns= ("Channel_Name","Dur_in_min"))
    st.dataframe(Channel_name_avg_dur_df)



# Highest Comments and channel name

def Highest_cmts_ch_name():
    Highest_cmts_ch_name = '''SELECT Video_Name, Comment_count, Channel_Name FROM Video_Details ORDER BY Comment_count DESC LIMIT 20;'''

    cursor.execute(Highest_cmts_ch_name)
    Highest_cmts_ch_name_table = cursor.fetchall()
    mydb.commit()

    Highest_cmts_ch_name_df = pd.DataFrame(Highest_cmts_ch_name_table, columns= ("Video_Name","Comment_count","Channel_Name"))
    st.dataframe(Highest_cmts_ch_name_df)




# Streamlit Part

st.set_page_config(page_title="Youtube Data Harvesting", page_icon="🔍",layout = "wide")
st.title(" 🔍 YouTube Data Harvesting")

with st.sidebar:

    select = option_menu("Main menu", ["Home","Data Exploration and Analysis"])

if select == "Home":
    col1, col2, col3 = st.columns([4, 0.1, 4])
    with col1:
        st.write("")

        st.markdown("### :red[_YouTube_] is an American online video sharing platform owned by Google, and it has worldwide accessibility. YouTube was launched on February 14, 2005. ")
        st.markdown("#### :blue[_Problem Statement_ :] To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.")
        st.markdown("#### :blue[_Skills take away_ :] Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL")  
        st.markdown("#### :blue[_Domain_ :] Social Media")         
    st.markdown("### In this project, the datas of multiple youtube channels are collected and used all those datas to get some usefull insights about the channels.")
               
    with col2:
        st.image("C:/Users/Theju/Desktop/Guvi Capstone Projects/Youtube Data Harvesting/Youtube.jpg", width=700)
    

elif select == "Data Exploration and Analysis":
    tab1, tab2 = st.tabs(["Data Exploration","Analysis"])
    with tab1:
        col1, col2 = st.columns(2)

        with col1:
            ch_id = st.text_input("Enter Channel Id : ")
            
            clk = st.button("Export to SQL",  on_click= partial(export_to_sql, ch_id))

            st.write(f"Entered Channel Id : {ch_id}")
    with tab2:
        col1,col2 = st.columns(2)
        with col1:
            try:
                Questions = st.selectbox(" ",
                            ("Select the Question",
                            "1. What are the names of all the videos and their corresponding channels?",
                            "2. Which channels have the most number of videos, and how many videos do they have?",
                            "3. What are the top 10 most viewed videos and their respective channels?",
                            "4. Name of the video and its comment count?",
                            "5. Videos that have the highest number of likes, and their corresponding channel names?",
                            "6. What is the video names and total number of likes?",
                            "7. What is the total number of views for each channel, and its channel name?",
                            "8. What are the names of all the channels that have published videos in the year 2022?",
                            "9. What is the average duration of all videos in every channel, and their corresponding channel names?",
                            "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))
                
                if Questions == "1. What are the names of all the videos and their corresponding channels?":
                    vid_ch_name()

                elif Questions == "2. Which channels have the most number of videos, and how many videos do they have?":
                    ch_name_vid_count()

                elif Questions == "3. What are the top 10 most viewed videos and their respective channels?":
                    Top_10_most_viewed_videos()
                
                elif Questions == "4. Name of the video and its comment count?":
                    Cmt_count_and_Vid_name()

                elif Questions == "5. Videos that have the highest number of likes, and their corresponding channel names?":
                    Highest_like_ch_and_vid_name()

                elif Questions == "6. What is the video names and total number of likes?":
                    Total_likes_Video_name()

                elif Questions == "7. What is the total number of views for each channel, and its channel name?":
                    Total_channel_views_and_name()

                elif Questions == "8. What are the names of all the channels that have published videos in the year 2022?":
                    Video_publishedAt_2022()

                elif Questions == "9. What is the average duration of all videos in every channel, and their corresponding channel names?":
                    Channel_name_avg_dur()

                elif Questions == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
                    Highest_cmts_ch_name()
            except:
                pass