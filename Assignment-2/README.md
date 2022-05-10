DATA ASSIGNMENT – 2 (TENSORIOT)

    • Download the data files from here - http://jmcauley.ucsd.edu/
    • Apache spark tools locally and necessary tools
    • Download a review file with a million reviews
    • Using Jupyter notebook create a program to read the million reviews and get the following
    • transform date to MM-DD-YYYY format
    • Save the data into a table (postgres/sql server)
    • Save the output as a Parquet file
    • Upload code to Github  and complete Readme.md which anyone can understand
    • Send Github link to HR
    • postgres
    • workbench/J
    • Jupyter notebook
    • Apache spark
    • Data frame transform


Step 1 : Create a spark session if you have already installed spark on your local system or else install spark and install python library for pyspark i,e pyspark  

Step 2 : After downloading review_Electronics_5.json.gz extarct the file, you will get  review_Electronics_5.json  and read the same file as pyspark dataframe, from this file we will get the longest review , date and reviewerID

review_df = spark.read.json('reviews_Electronics_5.json')

Step 3 : As mentioned we have to change the date format to MM-DD-YYYY for that we have to modify the date column present in dataframe as new column to dataframe and rename the column name

Steo 4 : As  mentioned we have to store that dataframe to database here we are using SQLServer (MSSQL) for saving the dataframe to database

Step 5 : As we are going to save the dataframe to table we need jar files, you have to download the required jar files and keep that in a folder or in your jars.

Dowload jars from here : https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-2017

Step 6 : I have kept my username and password of SQLServer database in a config file and using the same for storing the dataframe to database table 

(Learn about the use of config file in python : https://www.tutorialspoint.com/configuration-file-parser-in-python-configparser)

Step 7 : Finally save the dataframe to a paraquet file 

df.write.parquet('Assignment1paraquetfile')
