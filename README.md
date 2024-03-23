# Youtube-harvesting
# Introduction
This project extracts the particular youtube channel data by using the youtube channel id, processes the data, and stores it in the MongoDB database. It has the option to migrate the data to MySQL from MongoDB then analyse the data and give the results depending on the customer questions.
# Developer Guide

# Tools Used
   1.Visual Studio Code
   
   2.Postgresql
   
   3..MongoDB Atlas
   
   4.Youtube API key.
   
   5.Google Colab
   
   6.Python 3.11.0 or higher.
   
# Libraries used
  1. googleapiclient.discovery
  
  2. pprint
  
  3. pymongo
  
  4. pandas
  
  5. streamlit
  
  6. psycopg2
  
  7. matplotlib
  
  8. isodate
  
  9. pyplot
# User Guide
Step 1. Scrape & Store Data to MongoDB
Search channel_id, copy and paste on the input box and click the "Scarpe & Store in MongoDB" button in the Scrape & Store Data to MongoDB Step.

Step 2. View All Channel Details Exists in MongoDB
By clicking "Channels in MongoDb" button, to view all the channels name & channel id exists in the mongoDB

Step 3. View Your Current Channel Scraped Data
By enable the channel check box , able to view the given channel(current channel) info . Similarly for videos & comments check box.

Step 4. Data Migration to SQL
Select a channel name in the drop down list box and click " Migrate to sql" button. the selected channel inforamtion are migrated to Postgresql

Step 5 Data Analysis
Select a Question from the dropdown option you can get the results in Dataframe format or bar chat format.



![GUI_Image](https://github.com/baymax18/Youtube-harvesting/assets/33507845/09a1e349-6c24-421c-b173-99bd072e2556)
