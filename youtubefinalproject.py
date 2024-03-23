import googleapiclient.discovery
import pymongo
import pandas as pd
import mysql.connector
import streamlit as st
import isodate
from datetime import datetime

def Api_connect():
  youtube = googleapiclient.discovery.build("youtube", "v3",developerKey="AIzaSyBpM2n7b-z3b-IzC5hpkeEePW__nDZwNJE")
  return youtube
youtube=Api_connect()


# get channel info
def getchannel_information(channel_id):
  request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id) 
  response = request.execute()

  for i in response['items']:
      data=dict(Channel_Name=i['snippet']['title'],channel_Id=i['id'],
                description=i['snippet']['description'],
                sub_count=i['statistics']['subscriberCount'],
                v_count=i['statistics']['videoCount'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']) # get videos using playlistid
  return data


#get video details
def get_video_ids(channel_id):
  video_ids=[]
  response=youtube.channels().list(id=channel_id,part='contentDetails').execute()
  Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token=None

  while True:
    response1 = youtube.playlistItems().list(part='snippet',playlistId=Playlist_Id,maxResults=50,pageToken=next_page_token).execute()
    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token=response1.get('nextPageToken')

    if next_page_token is None:
      break
  return video_ids


# get video information
def get_video_info(v_ids):
      video_Data=[]
      for video_id in v_ids:
            request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
            response=request.execute()

            for item in response["items"]:
                  data=dict(Channel_Name=item['snippet']['channelTitle'],Channel_id=item['snippet']['channelId'],video_id=item['id'],Title=item['snippet']['title'],published_Date=item['snippet']['publishedAt'],Duration=item['contentDetails']['duration'],
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],Description=item.get('description'),view_count=item['statistics'].get('viewCount'),comments_count=item['statistics'].get('commentCount'),
                        Like_count=item['statistics'].get('likeCount'),fav_count=item['statistics']['favoriteCount'])
                  #Tags=item.get('tags',"notag")
            video_Data.append(data)
      return video_Data


#get comment information # using try block for avoiding errors (i.e incase no comment means)
def get_comment_info(video_ids):
  comment_Data=[]
  try:

    for video_id in video_ids:
      request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,maxResults=50)
      response = request.execute()

      for item in response['items']:
          data=dict(channel_id=item['snippet']['channelId'],video_id=item['id'],comment_id=item['snippet']['topLevelComment']['id'],
                    comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    ) 

          comment_Data.append(data)
  except:
    pass
  return comment_Data

# Connection and upload to mongodb

from pymongo import MongoClient
client=MongoClient("mongodb+srv://anusuyaanusuya1372:koS7VON7qOo3vFts@cluster0.ynomcqu.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
client
db=client['youtube_data_harvest']


def channel_details(channel_id):
    ch_det=getchannel_information(channel_id)
    v_id=get_video_ids(channel_id)
    v_info=get_video_info(v_id)
    c_info=get_comment_info(v_id)

    coll1=db["channel_details"]
    coll1.insert_one({"Channel_Information":ch_det,
                     "Videos_Information":v_info,
                     "Video_Id":v_id,"Comment_Information":c_info})
    return "uploaded successfully"


# Table creation for channels,videos,comments

def channels_table1(s_channel_name):
    mydb=mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_Data",port="3306")
    mycursor=mydb.cursor()

    #drop_query='''drop table if exists channels'''
    #mycursor.execute(drop_query)
    #mydb.commit()

    #try:
      #  create_query='''create table if not exists channels(Channel_Name varchar(100),Channel_id varchar(80) primary key,
                #        description text,sub_count bigint,v_count bigint,Playlist_Id varchar(80) )'''
       # mycursor.execute(create_query)
       # mydb.commit()

    #except:
      #print("Channels tables already created")

    single_channel_detail=[]
    db=client["youtube_data_harvest"]
    coll1=db["channel_details"] 
    for Ch_data in coll1.find({"Channel_Information.Channel_Name":s_channel_name},{"_id":0}): 
                single_channel_detail.append(Ch_data["Channel_Information"])  
    df_single_channel_detail=pd.DataFrame(single_channel_detail) 

    #df3 = df.loc[df['channel_Id'] == Ch_data_id]

    for index,row in df_single_channel_detail.iterrows():
            insert_query='''insert into channels(Channel_Name,Channel_id,description,sub_count,v_count,Playlist_Id) 
                        values(%s,%s,%s,%s,%s,%s)''' 
        
            values=(row['Channel_Name'],row['channel_Id'],row['description'],
                row['sub_count'],row['v_count'],row['Playlist_Id'])    

    try:
            mycursor.execute(insert_query,values)
            mydb.commit()

    except:
               Exist_error=f"Your provided channel name{s_channel_name} is already exists"  

               return Exist_error
    mydb.close()


def videos_table1(s_channel_name):
        mydb=mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_Data",port="3306")
        mycursor=mydb.cursor()

        #drop_query='''drop table if exists videos'''    
        #mycursor.execute(drop_query)
        #mydb.commit()

        #create_query='''create table if not exists videos(Channel_Name varchar(100),Channel_id varchar(100),video_id varchar(100) primary key,
         #               Title varchar(100),Extraction_Date DATE,Duration_sec int,Thumbnail varchar(200),Description text,view_count bigint,Like_count bigint,fav_count int,comments_count int )'''
                
        #mycursor.execute(create_query)
        #mydb.commit()

        single_video_details=[]
        db=client["youtube_data_harvest"]
        coll1=db["channel_details"] 
        #Ch_data_id=channel_id
        for v_data in coll1.find({"Channel_Information.Channel_Name":s_channel_name},{"_id":0}):   
                  for i in range(len(v_data["Videos_Information"])):                    
                        single_video_details.append(v_data["Videos_Information"][i]) #[i]

        df_single_video_details=pd.DataFrame(single_video_details) #[0]

        Duration2=[]
        Date=[]
        for i in range(len(df_single_video_details)):
                dur=df_single_video_details.loc[i]['Duration']
                Duration1=isodate.parse_duration(dur).total_seconds()
                Duration2.append(Duration1)  


                date1=df_single_video_details.loc[i]['published_Date']
                parsed_datetime = datetime.fromisoformat(date1)
                Ext_Date = parsed_datetime.date()
                Date.append(Ext_Date)

        df_single_video_details.loc[:,"Duration_sec"] = Duration2 
        df_single_video_details.loc[:,"Extraction_Date"] = Date


        for index,row in df_single_video_details.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                Channel_id,
                                                video_id,
                                                Title,
                                                Extraction_Date,
                                                Duration_sec,
                                                Thumbnail,
                                                Description,
                                                view_count,
                                                Like_count,
                                                fav_count,
                                                comments_count	
                                                ) 

                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 

                values=(row['Channel_Name'],
                            row['Channel_id'],
                            row['video_id'],
                            row['Title'],
                            row['Extraction_Date'],
                            row['Duration_sec'],
                            row['Thumbnail'],
                            row['Description'],
                            row['view_count'],
                            row['Like_count'],
                            row['fav_count'],
                            row['comments_count'])    

                mycursor.execute(insert_query,values)
                mydb.commit()

        mydb.close()


def comments_table1(s_channel_name):
        mydb=mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_Data",port="3306")
        mycursor=mydb.cursor()

       # drop_query='''drop table if exists comments'''    
        #mycursor.execute(drop_query)
        #mydb.commit()

        #create_query='''create table if not exists comments(Channel_id varchar(100),video_id varchar(100) primary key,comment_id varchar(100),
         #               comment_text text,comment_Author varchar(150)
          #              )'''

        #mycursor.execute(create_query)
        #mydb.commit()

        single_comments_details=[]
        db=client["youtube_data_harvest"]
        coll1=db["channel_details"] 
        #Ch_data_id=channel_id
        for com_data in coll1.find({"Channel_Information.Channel_Name":s_channel_name},{"_id":0}):
                #for i in range(len(com_data["Comment_Information"])):
                        single_comments_details.append(com_data["Comment_Information"]) #[i]
        df_single_comments_details=pd.DataFrame(single_comments_details[0])  
        #df4 = df2.loc[df2['channel_id'] == Ch_data_id]

        
        for index,row in df_single_comments_details.iterrows():
                                insert_query='''insert into comments(Channel_id,
                                                                    video_Id,
                                                                    comment_id,
                                                                    comment_text,
                                                                    comment_Author)
                                                                    values(%s,%s,%s,%s,%s)''' 

                                values=(row['channel_id'],
                                        row['video_id'],
                                        row['comment_id'],
                                        row['comment_text'],
                                        row['comment_Author']
                                )    

                                mycursor.execute(insert_query,values)
                                mydb.commit()
        mydb.close()


def tables(s_channel):
    Exist_error=channels_table1(s_channel)

    if Exist_error:
          return Exist_error
    else:
        videos_table1(s_channel)
        comments_table1(s_channel)

        return "Tables created successfully"   


def view_channels_tables():
    ch_list=[]
    db=client["youtube_data_harvest"]
    coll1=db["channel_details"] 
    for Ch_data in coll1.find({},{"_id":0,"Channel_Information":1}):   #empty dict for getting all channel details
        ch_list.append(Ch_data["Channel_Information"])  
    df=st.dataframe(ch_list) 

    return df  


def view_videos_tables():
        v_list=[]
        db=client["youtube_data_harvest"]
        coll1=db["channel_details"] 
        for v_data in coll1.find({},{"_id":0,"Videos_Information":1}):   
                for i in range(len(v_data["Videos_Information"])):
                        v_list.append(v_data["Videos_Information"][i]) 
        df1=st.dataframe(v_list)  

        return df1


def view_comments_tables():
    com_list=[]
    db=client["youtube_data_harvest"]
    coll1=db["channel_details"] 
    for com_data in coll1.find({},{"_id":0,"Comment_Information":1}):
            for i in range(len(com_data["Comment_Information"])):
                    com_list.append(com_data["Comment_Information"][i]) 
    df2=st.dataframe(com_list) 
     
    return df2


#STREAMLIT

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("skill Take Away")
    st.caption("python scripting")
    st.caption("Data Collection")
    st.caption("Mongodb")
    st.caption("API Integration")
    st.caption("Data Management using Mongodb and sql")

channel_id=st.text_input("Enter the channel ID")    

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_data_harvest"]
    coll1=db["channel_details"]
    for Ch_data  in coll1.find({},{"_id":0,"Channel_Information":1}):
        ch_ids.append(Ch_data["Channel_Information"]["channel_Id"])  

    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

All_Channels=[]
db=client["youtube_data_harvest"]
coll1=db["channel_details"]
for Ch_data in coll1.find({},{"_id":0,"Channel_Information":1}):   
        All_Channels.append(Ch_data["Channel_Information"]["Channel_Name"])  

Select_Channel=st.selectbox("Select the channel",All_Channels)


if st.button("Upload to sql"):
    Table=tables(Select_Channel)
    st.success(Table)

show_table=st.radio("select the table for view",("Channels","Videos","Comments"))

if show_table=="Channels":
    view_channels_tables()

elif show_table=="Videos":
   view_videos_tables()

elif show_table=="Comments":
   view_comments_tables()   


# SQL CONNECTION AND QUERIES:
   
mydb=mysql.connector.connect(host="localhost",user="root",password="12345",database="Youtube_Data",port="3306")
mycursor=mydb.cursor()

Questions=st.selectbox("Select your Questions",("1.What are all the names of all the videos and their corresponding channels",
                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))



if Questions =='1.What are all the names of all the videos and their corresponding channels':
    query1='''select title as VideoTitle,channel_name as channelname from videos'''
    mycursor.execute(query1)

    table1=mycursor.fetchall()
    df1=pd.DataFrame(table1,columns=["VideoTitle","Channelname"])
    st.write(df1)

elif Questions == '2.Which channels have the most number of videos, and how many videos do they have?':
    query2 = '''select Channel_Name as channelname,v_count as Videocount from channels order by v_count desc'''
    mycursor.execute(query2)

    table2=mycursor.fetchall()
    df2=(pd.DataFrame(table2, columns=["channelname","Videocount"]))
    st.write(df2)    

elif Questions == '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Channel_Name as channelname,title as videoTitle,view_count as Viewcount from videos order by view_count desc LIMIT 10 '''
    mycursor.execute(query3)

    table3=mycursor.fetchall()
    df3=(pd.DataFrame(table3, columns=["channelname","videoTitle","Viewcount"]))
    st.write(df3)

elif Questions == "4.How many comments were made on each video, and what are their corresponding video names?":
    query4 = '''select comments_count as No_comments,Title as videoTitle from videos where comments_count is not null'''
    mycursor.execute(query4)

    table4=mycursor.fetchall()
    df4=(pd.DataFrame(table4, columns=["No_comments","videoTitle"]))
    st.write(df4) 
    

elif Questions == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5 = '''select Channel_Name as channelname,Title as VideoTitle, Like_count as Likecount from videos where Like_count is not null order by Like_count desc'''
    mycursor.execute(query5)

    table5=mycursor.fetchall()
    df5=(pd.DataFrame(table5, columns=["channelname","VideoTitle","Likecount"]))
    st.write(df5) 

elif Questions == "6.What is the total number of likes and what are their corresponding video names?":
    query6 = '''select Title as VideoTitle ,Like_count as Likecount from videos'''
    mycursor.execute(query6)

    table6=mycursor.fetchall()
    df6=(pd.DataFrame(table6, columns=["VideoTitle","Likecount"]))
    st.write(df6)    

elif Questions == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7 = '''select Channel_Name as channelname ,view_count as Viewscount from videos'''
    mycursor.execute(query7)

    table7=mycursor.fetchall()
    df7=(pd.DataFrame(table7, columns=["channelname","Viewscount"]))
    st.write(df7)

elif Questions == "8.What are the names of all the channels that have published videos in the year 2022?":
    query8 = '''select Channel_Name as channelname,Title as VideoTitle ,Extraction_Date as VideoRelease from videos  where extract(year from Extraction_Date)=2022'''
    mycursor.execute(query8)

    table8=mycursor.fetchall()
    df8=(pd.DataFrame(table8, columns=["channelname","VideoTitle","VideoRelease"]))
    st.write(df8) 

elif Questions == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9 = '''select Channel_Name as channelname,Duration_sec as AverageDuration from videos group by Channel_Name'''
    mycursor.execute(query9)

    table9=mycursor.fetchall()
    df9=(pd.DataFrame(table9, columns=["channelname","AverageDuration"]))
    

    Table9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        Avg_duration=row["AverageDuration"] 
       # Avg_duration_str=str["AverageDuration"]
        Table9.append(dict(channeltitle=channel_title,Avg_duration=Avg_duration)) #=Avg_duration_str
    df=pd.DataFrame(Table9)
    st.write(df) 


elif Questions == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10 = '''select Channel_Name as channelname,Title as VideoTitle,comments_count as comments from videos where comments_count is not null order by comments_count desc'''
    mycursor.execute(query10)   

    table10=mycursor.fetchall()
    df10=(pd.DataFrame(table10, columns=["channelname","VideoTitle","comments"]))
    st.write(df10)
            
    mydb.close()

