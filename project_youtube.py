#YOUTUBE DATA HARVESTING AND WAREHOUSING

#Library part

# activating google client to run the google api-->pip install google-api-python-client
from googleapiclient.discovery import build

#To convert any format of date to standard format
from datetime import datetime

#To Convert any format of Time to Standat format---->pip install isodate
import isodate

#To import pandas-->pip install pandas
import pandas as pd

#To import MongoDB-->pip install pymongo
import pymongo

#To import MySQL--->pip install mysql-connector-python
import mysql.connector

# To import streamlit--->pip install streamlit
import streamlit as st

#API KEY connection Interface
#creating user defined function for 'API KEY connection' to access data from youtube 
def api_connect():
    api_id='AIzaSyDxQDFSWeBbuYzvnTu97ym_PqJPCMCVLtg'
    api_service_name="youtube"
    api_version='v3'
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=api_connect()


#User defined function to get Channel details
def get_channel_info(channel_id):
    request=youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id
    )
    response=request.execute()
    response
    #using for loop to select data what we need from the youtube channel
    for i in response['items']: #here items contains all the required details 
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subcribers=i['statistics']['subscriberCount'],
                Views=i['statistics']['viewCount'],
                Total_Videos=i['statistics']['videoCount'],
                Channel_Description=i['snippet']['description'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


#user defined function to get playlist details
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                                part= 'snippet,contentDetails',
                                channelId=channel_id, # Channel Id 'UCY6KjrDBN_tIRFT_QNqQbRQ' Mandan Gowri
                                maxResults=50,
                                pageToken=next_page_token)
                response=request.execute()
                for item in response['items']:
                                    # Parse 'Published_Date' string into datetime object
                        published_datetime = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                        # Format datetime object into MySQL TIMESTAMP format
                        published_timestamp = published_datetime.strftime('%Y-%m-%d %H:%M:%S')
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=published_timestamp,
                                Video_Count=item['contentDetails']['itemCount'])
                        data
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


# User defined function to find the videos id in the playlist of youtube channel
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,#youtube channel id
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_Page_Token=None
    while True:# loop is used connect to connect the next_Page_Token-keyword to next page of the playlist
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_Page_Token).execute()#'pageToken'-'next_Page_Token' is parameter to full videoplay list
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_Page_Token=response1.get('nextPageToken') # get function is used to return the value of none incase 'next_Page_Token=null'    
        if next_Page_Token is None: # if condition is used to break the operation of while loop
            break
    return video_ids


#user defined function to get video information using 'videoid' of the video in the specific youtube channel
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
    #'videos' is a parameter to get video information from youtube  
        request=youtube.videos().list(  
                                    part='snippet,contentDetails,statistics',
                                    id=video_id)
        response=request.execute()
        for item in response['items']:
            tags = ', '.join(item['snippet'].get('tags', [])) #make the list in tags into single string
            # Parse 'Published_Date' string into datetime object
            published_datetime = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            # Format datetime object into MySQL TIMESTAMP format
            published_timestamp = published_datetime.strftime('%Y-%m-%d %H:%M:%S')
            # Parse duration string into timedelta object
            duration_timedelta = isodate.parse_duration(item['contentDetails']['duration'])
            # Extract total seconds from timedelta object
            duration_seconds = duration_timedelta.total_seconds()
            #hours = duration_timedelta.days * 24 + duration_timedelta.seconds // 3600
            #minutes = (duration_timedelta.seconds % 3600) // 60
            #seconds = duration_timedelta.seconds % 60
            # Format the duration as "hh:MM:ss"
            #duration_formatted = "{:02d}:{:02d}:{:02d}".format(hours,minutes,seconds )
            data=dict(Channel_Name=item['snippet'].get('channelTitle'),
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=tags,
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=published_timestamp,  # Use formatted timestamp
                    Duration=duration_seconds,#duration_formatted,
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Dislikes=item['statistics'].get('dislikeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    FavouriteCount=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data


#user defined function to get comment details
def get_Comment_info(video_ids):
    Comment_data=[]
    try:# 'try and except pass'-here it is used to continue the program if the comment is disabled by the channel owner
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                        part='snippet',
                        videoId=video_id,
                        maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                # Parse 'Published_Date' string into datetime object
                published_datetime = datetime.strptime(item['snippet']['topLevelComment']['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
                # Format datetime object into MySQL TIMESTAMP format
                comment_published_timestamp = published_datetime.strftime('%Y-%m-%d %H:%M:%S')
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Ids=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=comment_published_timestamp)
                Comment_data.append(data)
    except:
        pass
    return Comment_data


#MongoDB Part
#To install MongoDB in Python use the command 'pip install pymongo'

client=pymongo.MongoClient('mongodb://localhost:27017') # Connecting the python to MongoDB using 'localhost'
db=client['youtube_data'] #database name is youtube_data


#User defined function to upload data into MongoDB

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_Comment_info(vi_ids)

#creating one collection with name 'coll1'
    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,'playlist_information':pl_details,
                      'video_information':vi_details,'comment_information':com_details})
    return'upload completed successfully'


#MySQL Part
#To install MySQL in Python use the command 'pip install mysql-connector-python'
import mysql.connector
# User Defined funtion to Connect with MySQL database
def connect_to_database():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="shrini",
        database="youtube_data",
        port=3306)
    return mydb


#create channel Table in MySqL
def channel_table(mydb):
    # Create a cursor object
    mycursor=mydb.cursor()

        #to avoid creating same table name in a single database
    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()


    create_table_query = '''create table if not exists channels(
                                                            Channel_Name varchar(255),
                                                            Channel_Id varchar(255) primary key,
                                                            Subcribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(255)
                                                            )'''
    mycursor.execute(create_table_query)
    mydb.commit()
        
    ch_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)

    for index, row in df.iterrows():
            insert_query='''insert into channels(
            Channel_Name,
            Channel_Id,
            Subcribers,
            Views,
            Total_Videos,
            Channel_Description,
            Playlist_Id)
            
            values(%s,%s,%s,%s,%s,%s,%s)'''
        
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subcribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
            
            mycursor.execute(insert_query, values)
            mydb.commit()     
        


#create playlist Table
def playlist_table(mydb):
    # Create a cursor object
    mycursor=mydb.cursor()

        #to avoid creating same table name in a single database
    drop_query='''drop table if exists playlists'''
    mycursor.execute(drop_query)
    mydb.commit()

    #try:
    create_table_query = '''create table if not exists playlists(
            Channel_Name varchar(255),
            Channel_Id varchar(255),
            Playlist_Id varchar(255) primary key,
            Title varchar(255),
            PublishedAt timestamp,
            Video_Count bigint
                )'''

    mycursor.execute(create_table_query)
    mydb.commit()

    pl_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
                pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)


    mycursor=mydb.cursor()

    for index, row in df1.iterrows():
            insert_query='''insert into playlists(             
                Channel_Name,
                Channel_Id,
                Playlist_Id,
                Title,
                PublishedAt,
                Video_Count)
                
                values(%s,%s,%s,%s,%s,%s)'''
            
                                
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Playlist_Id'],
                    row['Title'],
                    row['PublishedAt'],
                    row['Video_Count'])
            
            
            mycursor.execute(insert_query, values)
            mydb.commit()  


#create videos Table
def videos_table(mydb):
# Create a cursor object
    mycursor=mydb.cursor()

    #to avoid creating same table name in a single database
    drop_query='''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    #try:
    create_query = '''create table if not exists videos(
                                                        Channel_Name varchar(255),
                                                        Channel_Id varchar(255),
                                                        Video_Id varchar(255) primary key,
                                                        Title text,
                                                        Tags text,
                                                        Thumbnail varchar(255),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration bigint,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Dislikes int,
                                                        Comments bigint,
                                                        FavouriteCount int,
                                                        Definition varchar(255),
                                                        Caption_Status varchar(255)
                                                        )'''

    mycursor.execute(create_query)
    mydb.commit() 

    vi_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
                vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)


    mycursor=mydb.cursor()
    for index, row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Dislikes,
                                                Comments,
                                                FavouriteCount,
                                                Definition,
                                                Caption_Status
                                                )
        
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            
                                
        values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Dislikes'],
                    row['Comments'],
                    row['FavouriteCount'],
                    row['Definition'],
                    row['Caption_Status']
                    )
            
            
        mycursor.execute(insert_query, values)
        mydb.commit()


#create comments Table
def comments_table(mydb):
# Create a cursor object
    mycursor=mydb.cursor()

        #to avoid creating same table name in a single database
    drop_query='''drop table if exists comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    #try:
    create_table_query = '''create table if not exists comments(
                            Comment_Id varchar(255) primary key,
                            Video_Ids varchar(255),
                            Comment_Text text,
                            Comment_Author varchar(255),
                            Comment_Published timestamp
                )'''

    mycursor.execute(create_table_query)
    mydb.commit()

    com_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
                com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)

    for index, row in df3.iterrows():
            insert_query='''insert into comments(
                Comment_Id,
                Video_Ids,
                Comment_Text,
                Comment_Author,
                Comment_Published)

                            
                values(%s,%s,%s,%s,%s)'''
            
                                
            values=(row['Comment_Id'],
                    row['Video_Ids'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )
                            
            
            mycursor.execute(insert_query, values)
            mydb.commit()



#User defined fuction to concardinate all the from table            
def create_tables():
    mydb = connect_to_database()
    channel_table(mydb)
    playlist_table(mydb)
    videos_table(mydb)
    comments_table(mydb)
    return 'Table Created Successfully'

#streamlit part

#Streamlit Python Script to create user defined function of data frame for channel,playlist,video,comment-table 
import streamlit as st
def show_channel_tabls():
    ch_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)
    return(df)



def show_playlists_table():
    pl_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
                pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)
    return(df1)



def show_videos_table():
    vi_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
                vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)
    return(df2)



def show_comments_table():    
    com_list=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
                com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list)
    return(df3)



#streamlit python script to create webpage interface
with st.container():
    st.write("<h1 style='text-align: center; color: green;'>YOUTUBE DATA HARVESTING AND WAREHOUSING</h1>", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("<h2 style='color: blue; text-decoration: underline;'>Skill Take Away:</h2>", unsafe_allow_html=True)
    st.markdown("<p style='color: deeppink;'>Python Scripting</p>", unsafe_allow_html=True)
    st.markdown("<p style='color: deeppink;'>API Integration</p>", unsafe_allow_html=True)
    st.markdown("<p style='color: deeppink;'>Data Collection</p>", unsafe_allow_html=True)
    st.markdown("<p style='color: deeppink;'>MongoDB-Data Storage</p>", unsafe_allow_html=True)
    st.markdown("<p style='color: deeppink;'>Data Management using MongoDB and SQL</p>", unsafe_allow_html=True)

channel_id=st.text_input('Entre the Channel ID')

if st.button('Collect and Store data ~~> MongoDB'):
    ch_ids=[]
    db=client['youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])
    
    if channel_id in ch_ids:
        st.success("Channel Details already exist")
    
    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button('Data Transfer to SQL'):
    Table=create_tables()
    st.success(Table)
show_table=st.radio('SELECT THE TABLE TO VIEW',('CHANNELS','PLAYLISTS','VIDEOS','COMMENTS'))

if show_table=='CHANNELS':
    show_channel_tabls()

elif show_table=='PLAYLISTS':
    show_playlists_table()

elif show_table=='VIDEOS':
    show_videos_table()

elif show_table=='COMMENTS':
    show_comments_table()

#SQL Connection
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="shrini",
        database="youtube_data",
        port=3306)
mycursor=mydb.cursor()

question=st.selectbox('Choose your question',('Choose the Question',
                                              '1.What are the names of all the videos and their corresponding channels',
                                              '2.Which channels have the most number of videos, and how many videos do they have?',
                                              '3.What are the top 10 most viewed videos and their respective channels?',
                                              '4.How many comments were made on each video, and what are their corresponding video names?',
                                              '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
                                              '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                              '7.What is the total number of views for each channel, and what are their corresponding channel names?',
                                              '8.What are the names of all the channels that have published videos in the year 2022?',
                                              '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                              '10.Which videos have the highest number of comments, and what are their corresponding channel names?'
                                              ))

if question=='1.What are the names of all the videos and their corresponding channels':
        query1='''select title as video,channel_name as channel_name from videos'''
        mycursor.execute(query1)
        t1=mycursor.fetchall()
        mydb.commit()
        df1=pd.DataFrame(t1,columns=['Video Title','Channel Name'])
        st.write(df1)

elif question == '2.Which channels have the most number of videos, and how many videos do they have?':
        query2='''select channel_name as ChannelName, Total_Videos as No_of_Videos from channels order by Total_videos desc;'''
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        mydb.commit()
        df2=pd.DataFrame(t2,columns=['Channel Name','No of Videos'])
        st.write(df2)

elif question=='3.What are the top 10 most viewed videos and their respective channels?':
        query3='''select channel_Name as Channel_Name, Views as Views from videos order by Views desc limit 10'''
        mycursor.execute(query3)
        t3=mycursor.fetchall()
        mydb.commit()
        df3=pd.DataFrame(t3,columns=['Channel Name','Top 10 Most Viewed Videos'])
        st.write(df3)
        
elif question=='4.How many comments were made on each video, and what are their corresponding video names?':
        query4='''select Channel_Name,Title,Comments from videos'''
        mycursor.execute(query4)
        t4=mycursor.fetchall()
        mydb.commit()
        df4=pd.DataFrame(t4,columns=['Channel Name','Video Tile','Comments Count'])
        st.write(df4)

elif question=='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5='''select Title,Likes,Channel_Name from videos order by Likes desc;'''
        mycursor.execute(query5)
        t5=mycursor.fetchall()
        mydb.commit()
        df5=pd.DataFrame(t5,columns=['Video Title','Likes','Channel Name'])
        st.write(df5)

elif question=='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        query6='''select channel_name,title,likes,dislikes from videos order by channel_name desc'''
        mycursor.execute(query6)
        t6=mycursor.fetchall()
        mydb.commit()
        df6=pd.DataFrame(t6,columns=['Channel Name','Video Ttile','Total Likes','Total Dislikes'])
        st.write(df6)

elif question=='7.What is the total number of views for each channel, and what are their corresponding channel names?':
        query7='''select channel_name,Views from channels order by views desc'''
        mycursor.execute(query7)
        t7=mycursor.fetchall()
        mydb.commit()
        df7=pd.DataFrame(t7,columns=['Channel Name','Total No. of Viwes'])
        st.write(df7)

elif question=='8.What are the names of all the channels that have published videos in the year 2022?':
        query8='''select Channel_name,Title,Published_date from videos where year(Published_Date)=2022 order by Published_date'''
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        mydb.commit()
        df8=pd.DataFrame(t8,columns=['Channel Name','Video Title','Published Date and Time'])
        st.write(df8)
elif question=='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9='''select Channel_name,avg(duration) as avg_duration_sec from videos group by channel_name'''
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        mydb.commit()
        df9=pd.DataFrame(t9,columns=['Channel Name','Average Duration of all videos in Sec'])
        st.write(df9)
        
elif question=='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10='''select Channel_name,title,comments as highest_comments from videos where comments is not null order by comments desc'''
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        mydb.commit()
        df10=pd.DataFrame(t10,columns=['Channel Name','Highly Commented Video Title','Comment Count'])
        st.write(df10)
