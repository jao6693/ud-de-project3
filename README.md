# Sparkify Analytics 

<b>Sparkify</b> is an international media services provider  
It's primary business is to build an <b>audio streaming</b> platform that provides music, videos and podcasts from record labels and media companies  

## Challenge

Sparkify wants to better serve its users and thus needs to analyze the data collected on songs and user activity on their music streaming app. 
The analytical goal is to understand what songs users are listening to  

## Architecture 

The data is stored as JSON files and resides on AWS in a S3 bucket (logs on user activity as well as metadata on the songs). 
This architecture doesn't provide an easy way to directly query the data  

## Analytics goals 

Sparkify wants to leverage its cloud investment and use highly scalable components to support the analytical queries. They chose Redshift as hosted service to support it.  

The main idea is to build a 3-step process as follow :
* create a <b>staging area</b> on Redshift to transfer data from S3 to tables (not accessible by end-users)
* create a <b>(OLAP-oriented) analytic model</b> on Redshift to expose consistent and organized data to end-users
* create an <b>ETL pipeline</b> to populate the staging area from S3 with their metadata and logs and then the analytic tables (fact & dimension tables)

## Design considerations  

The staging tables are relatively permissive and are used to transfer JSON content from S3 to SQL tables  
The fact and dimension tables require a bit more of attention  

The users table is now <b>time-dependent</b>, which allows to track changes on an account when upgrading/downgrading  

Also, in order to have analytic queries running without performance issues, `distkey` and `sortkey` have been positioned on fields of fact and dimension tables.  

The songs table is relatively large, and the `song_id` field is the one with the <b>highest cardinality</b> in the data model. That's why a `distkey` has been positioned on this field in the fact table as well as in the songs dimension table.  

The analysis will also certainly be time-centric, tracking the behavior changes of the users <b>overtime</b>. That's why a `sortkey` has been positioned on the `start_time` field. This allows queries to run efficiently when selecting a <b>year/month/day</b> (compounded) but probably not an hour or a weekday.  

Finally, most of the dimension tables have a `diststyle all`, as they are <b>relatively small</b> in size. This will boost the queries execution time.

## Scripts

Run the following commands in the terminal:  
* `python create_tables.py` to create the staging, fact and dimension tables
* `python etl.py` to process the files (logs/songs) stored in S3, and populate first the staging area and then the fact and dimension tables

At the end the Redshift cluster can be requested for analysis purposes using BI tools.