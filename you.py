import googleapiclient.discovery
from pprint import pprint
import pymongo
import pandas as pd
import streamlit as st
import psycopg2
import matplotlib.pyplot as plt
import isodate


# youtube api key
api_key = '#api key'

# api calling
youtube = googleapiclient.discovery.build("youtube", 'v3', developerKey = api_key)
# MongoDb Connect
client = pymongo.MongoClient("mongodb+srv://Vishnu:password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['youtube_data']

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id)
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids
def get_comment_info(video_ids):
    Comment_data=[]
    # next_page_token=None
    try:
        # while True:
            for video_id in video_ids:
                request=youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100
                )
                response=request.execute()

                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Channel_id = item['snippet']['channelId'])
                    Comment_data.append(data)
                    # next_page_token=response.get('nextPageToken')

                    # if next_page_token is None:
                    #     break
    except:
        pass
    return Comment_data

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        

        for item in response["items"]:
            ### convert duration to seconds
            duration_str = item['contentDetails']['duration']
            duration = isodate.parse_duration(duration_str)
            seconds = duration.total_seconds()

            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration = seconds,
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Dislikes=item['statistics'].get('dislikeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data

## table insertion to mysql
def Create_table_sql():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="Password",dbname="youtube_data",port="5432")
    cursor=mydb.cursor()

    # 1. create Channel table 

    create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                                    Channel_Id varchar(80),
                                                                    Subscribers bigint,
                                                                    Views bigint,
                                                                    Total_Videos int,
                                                                    Channel_Description text,
                                                                    Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()

    # 2. create viedo table 

    create_query2 ='''create table if not exists videos(Channel_Name varchar(100),
                            Channel_Id varchar(100),
                            Video_Id varchar(30),
                            Title varchar(100),
                            Tags text,
                            Thumbnail varchar(250),
                            Description text,
                            Published_Date timestamp,
                            Duration bigint,
                            Views bigint,
                            Likes bigint,
                            Dislikes bigint,
                            Comments bigint,
                            Favorite_Count int,
                            Definition varchar(10),
                            Caption_Status varchar(10)
                            )'''
    cursor.execute(create_query2)
    mydb.commit()

   

    # 3. create comment table 

    create_query3='''create table if not exists comments(Comment_Id varchar(100),
                                    Video_Id varchar(50),
                                    Comment_Text text,
                                    Comment_Author varchar(100),
                                    Comment_Published timestamp,Channel_Id varchar(100))
                                    '''
    cursor.execute(create_query3)
    mydb.commit()
    ### If channel not created 
    # Create_table_sql()
def data_to_mongodb(channel_id):
    
        ch_details=get_channel_info(channel_id)
        pl_details=get_playlist_details(channel_id)
        vi_ids=get_videos_ids(channel_id)
        vi_details=get_video_info(vi_ids)
        com_details=get_comment_info(vi_ids)
        print("Scrape Data Successfully")
        client = pymongo.MongoClient("mongodb+srv://Vishnu:Password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db = client['youtube_data']
        db
        collection1=db["channel_details"]
        
        if collection1.find_one({'channel_information.Channel_Id': channel_id }):
                print ("channel id exists in MongoDB !!! please try other channel id")
        else:
                collection1.insert_one({"channel_information":ch_details,"video_information":vi_details,"comment_information":com_details})
                print ('successfully upload data to mongoDB')
#### All channel in Mongo DB
def channels_mongo():

    client = pymongo.MongoClient("mongodb+srv://Vishnu:Password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client['youtube_data']
    col = db['channel_details']
    channels_mongo = []
    for ch in col.find({},{"_id":0,'channel_information.Channel_Id':1,'channel_information.Channel_Name':1}):
        channels_mongo.append(ch['channel_information'])
    df = st.dataframe(channels_mongo)
    return df



def insert_channels_sql(channel_id):
        mydb=psycopg2.connect(host="localhost",user="postgres",password="Password",dbname="youtube_data",port="5432")
        cursor=mydb.cursor()
        client = pymongo.MongoClient("mongodb+srv://Vishnu:Password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db = client['youtube_data']
        col = db['channel_details']
        

        cursor.execute('''select Channel_Id from channels''')
        rows = cursor.fetchall()
        channel_ids = [row[0] for row in rows]
        
        if  channel_id in channel_ids:
                print ("channel exists in SQL")
        else:
        
                insert = col.find_one({'channel_information.Channel_Id': channel_id },{'_id':0})
                new = (insert['channel_information'])

                ## 1. channel table
                insert_query='''insert into channels(Channel_Name ,
                                                        Channel_Id,
                                                        Subscribers,
                                                        Views,
                                                        Total_Videos,
                                                        Channel_Description,
                                                        Playlist_Id)
                                                                
                                                        values(%s,%s,%s,%s,%s,%s,%s)'''
                values=(tuple(new.values()))
                cursor.execute(insert_query,values)
                mydb.commit()



                ## 2. Viedo Table

                insert2 = col.find_one({'channel_information.Channel_Id': channel_id },{'_id':0})
                new2 = (insert2['video_information'])

                insert_query='''insert into videos(Channel_Name,
                                                Channel_Id ,
                                                Video_Id ,
                                                Title ,
                                                Tags ,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration ,
                                                Views,
                                                Likes,
                                                Dislikes,
                                                Comments,
                                                Favorite_Count,
                                                Definition ,
                                                Caption_Status)
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                for row in new2:
                        values = tuple((row.values()))
                        cursor.execute(insert_query,values)
                        mydb.commit()



                # 3 Comment Table

                insert3 = col.find_one({'channel_information.Channel_Id': channel_id },{'_id':0})
                new3 = (insert3['comment_information'])


                insert_query='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published,Channel_Id)
                                                values(%s,%s,%s,%s,%s,%s)'''
                for row in new3:
                        values = tuple((row.values()))
                        cursor.execute(insert_query,values)
                        mydb.commit()

                print ("Channel Data Migrate to SQL Successfully")


client = pymongo.MongoClient("mongodb+srv://Vishnu:Password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['youtube_data']
col = db['channel_details']

### Channel data frame
def show_channel(channel_id):
    insert = col.find_one({'channel_information.Channel_Id': channel_id },{'_id':0})
    df1 = st.dataframe([insert['channel_information']])

### videos dataframe
def show_viedo(channel_id): 
    insert = col.find_one({'channel_information.Channel_Id': channel_id },{'_id':0})  
    videos=[]
    for i in range(len(insert['video_information'])):
        videos.append(insert['video_information'][i])
    df2= st.dataframe(videos)

### comments dataframe
def show_comment(channel_id): 
    insert = col.find_one({'channel_information.Channel_Id': channel_id },{'_id':0})
    comments=[]
    for i in range(len(insert['comment_information'])):
        comments.append(insert['comment_information'][i])
    df3= st.dataframe(comments)

#### Streamlit part 
import streamlit as st

### Background image
background_color = '#F3CFC6'

custom_css = f"""
<style>
body {{
    background-image: url("{background_color}");
    background-size: cover;
}}
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

### Header

st.subheader(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]',divider = 'rainbow')
st.subheader(":violet[Scrape & Store Data to MongoDB]")

# Get user input
channel_id = st.text_input("Enter Your Channel ID",placeholder="channel ID contains 11 Digits")

# Check if user_input is not empty
if channel_id:
    # Connect to your code here
    st.write("Welcome user!!!")
    st.write("This is Your Channel ID :  ", channel_id)

button_clicked = st.button("Scrape & Store in MongoDB")
if button_clicked:
        client = pymongo.MongoClient("mongodb+srv://Vishnu:Password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db = client['youtube_data']
        collection1=db["channel_details"]
        
        if collection1.find_one({'channel_information.Channel_Id': channel_id }):
                collection1.delete_one({'channel_information.Channel_Id': channel_id})
                data_to_mongodb(channel_id)  
                st.write (":red[your channel id information updated in MongoDB !!! ]")
        else:
              data_to_mongodb(channel_id)  
              st.write(":green[Scrape and Update Data to MongoDB Successfully!!!]")
#### All channel Datas in MongoDB
st.subheader(":violet[View All Channel Details Exists in MongoDB]")
all_channel_button = st.button("Channels In MongoDB")
if all_channel_button:
     channels_mongo()
           
## view Data stored in mongoDB
st.subheader(":violet[View Your Current Channel Scraped Data]")


show_table1 = st.checkbox('Channels')
if show_table1:
    try:
        show_channel(channel_id) 
    
    except:
        st.write(":red[Please scarpe your channel]")

show_table2 = st.checkbox("videos")
if show_table2:
    try:
        show_viedo(channel_id)
    except:    
        st.write(":red[Please scarpe your channel]")

show_table3 = st.checkbox("Comments")
if show_table3:
    try:
        show_comment(channel_id)
    except:    
        st.write(":red[Please scarpe your channel]")

### Data migrate to SQL
st.subheader(":violet[Data Migration to SQL]")



client = pymongo.MongoClient("mongodb+srv://Vishnu:Password@cluster0.8onkuv9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['youtube_data']
col = db['channel_details']
channels_mongo = []
for ch in col.find({},{"_id":0,'channel_information.Channel_Id':1,'channel_information.Channel_Name':1}):
    channels_mongo.append(ch['channel_information'])
df = pd.DataFrame(channels_mongo)
unique_channel= st.selectbox("Select the Channel",df['Channel_Name'])
new_channel_id = col.find_one({'channel_information.Channel_Name': unique_channel })
ip_channel = new_channel_id['channel_information']['Channel_Id']


mydb=psycopg2.connect(host="localhost",user="postgres",password="Password",dbname="youtube_data",port="5432")
cursor=mydb.cursor()
cursor.execute('''select Channel_Id from channels''')
rows = cursor.fetchall()
channel_ids = [row[0] for row in rows]
        
button_sql = st.button("Store in SQL")
if button_sql:
    if  ip_channel in channel_ids:
        delete_query = "DELETE FROM channels WHERE channel_id = %s"
        cursor.execute(delete_query, (ip_channel,))
        
        delete_query2 = "DELETE FROM comments WHERE channel_id = %s"
        cursor.execute(delete_query, (ip_channel,))

        delete_query3 = "DELETE FROM videos WHERE channel_id = %s"
        cursor.execute(delete_query, (ip_channel,))
        
        
        mydb.commit()
        insert_channels_sql(ip_channel)
        st.write(":red[Your channel details updated in SQL]")
    else:
        insert_channels_sql(ip_channel)
        st.write(":green[Data Migrated to SQL Successfully]")

### Data Analysis 
st.subheader(":violet[Data Analysis]")

question=st.selectbox("Select your question",('Choose Your Question for Analysis',
                                '1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

mydb=psycopg2.connect(host="localhost",user="postgres",password="Password",dbname="youtube_data",port="5432")
cursor=mydb.cursor()

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)
    plt.figure(figsize=(8, 6))
    plt.bar(df2["channel name"], df2["No of videos"], color = 'pink')
    plt.xlabel("Channel Name")
    plt.ylabel("No of videos")
    plt.title("No of Videos in Channels")
    plt.xticks(rotation=90, ha='right')
    st.pyplot(plt.gcf())

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)


elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select likes as likecount,dislikes as dislikecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount",'dislikecount',"videotitle"])
    st.write(df6)


elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)
    ### Bar chart 
    plt.figure(figsize=(8, 6))
    plt.pie(df7["totalviews"], labels = df7["channel name"])
    plt.title("Total Views of Channels")
    st.pyplot(plt.gcf())


elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    st.write(df9)
    ### Bar chart 
    plt.figure(figsize=(8, 6))
    plt.bar(df9["channelname"], df9["averageduration"], color = 'pink')
    plt.xlabel("Channel Name")
    plt.ylabel("Average Duration in Seconds")
    plt.title("Average Duration of Channels")
    plt.xticks(rotation=90, ha='right')
    st.pyplot(plt.gcf())

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
