## Project: Data Lake (3)
#### Data Engineering Nanodegree
##### Student: Brian Pederson
&nbsp;
#### Project Description
Using the fictitious startup Sparkify, build a dimensional star schema data model consisting of one fact and four dimensions utilizing Spark running on AWS. Write a basic ETL pipeline that transfers data from source json files stored in AWS S3 buckets using Python and SQL and then outputs the fact and dimensions as parquet files in AWS S3 buckets.

##### Data Sources (json files)
- song_data - s3://udacity-dend/song_data
- log_data - s3://udacity-dend/log_data

##### Data Targets (parquet files)
- songplays - fact table representing events associated with song plays
- users - dimension table representing users of the Sparkify service
- time - dimension table containing timestamps associated with songplay events
- songs - dimension table containing referenced songs
- artists - dimension table containing referenced artists

##### Program files
- etl.py - Script to implement ETL processes for DWH tables
- dl.cfg - configuration file containing environment parameters (note AWS keys removed)
- README.md - this descriptive file

Note there are other files present in the project workspace used in development which are not technically part of the project submission.
- etl.ipynb - Jupyter notebook containing code snippets used in developing etl.py
- test.ipynb - Jupyter notebook containing various queries to examine contents of the staging and DWH tables

##### How to run the project
The process assumes :
- source data is located in an AWS s3 bucket with subdirectories song_data and log_data
- target diretory is set up within a target AWS s3 bucket (or local directory)
- an IAM Role/ARN with approprirate read privs to the s3 data
- the dl.cfg file has been edited to apply all necessary configuration parms

Then run scripts:
1. Execute etl.py to load data from s3 source files into five DWH tables. Specify the AWS s3 bucket/directory as a parameter. e.g. "--output_path s3a://sparkify-bp/analytics/"
2. Optionally execute queries from test Jupyter notebook to observe contents of various tables.

##### Miscellaneous Notes
1. The test data provided is poorly configured. In particular the log/event data is not matched well against song/artist data. I noted that less than 5% of fact rows had matching songs/artists. In a normal DWH the dimension data should match closely with the fact data. This is because it's normal to use inner joins when joining fact to dimension tables. The missing dimension song/artist data causes sparse results for any inner join query which includes those dimensions. One workaround is to create dummy rows in the dimensions with a meaning of 'missing data'. Note that I have done that for this project so expect one extra row in the songs and artists dimensions.
2. Additional problem with the song/artist data is that while there are pseudo unique identifiers for the songs and artists, there are actually multiple different artist names (and perhaps song names). So the raw data does not have a 1:1 relationship between the song ids and the songs. So I "normalize" the song and artist dimension data by doing a group by on the respective keys. This results in there being only one record per key and therefore some of the aliased names for artists (and perhaps songs) are lost in the dimension. So the join that links the source song data the log reference data has to be made on the raw source data to make sure that aliased names are matched. However I notice that the source song data takes forever to load from S3 buckets while the parquet files are more efficient. So I create a utility table called song_keys.parquet which contains the IDs and business keys for songs and artists and this utility table is used in the join between songs and log records that produces the songplays parquet fact table.
3. There is a rudimentary option to load the songplays fact table by overwrite vs append. This is a first step at allowing an incremental load. All the dimensions are hard coded to do a complete refresh via overwrite.


