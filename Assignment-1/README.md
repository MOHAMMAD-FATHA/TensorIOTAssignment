DATA ASSIGNMENT - 1(TENSORIOT)

    • Download the data files from here - http://jmcauley.ucsd.edu/
    • Apache spark tools locally and necessary tools
    • Download a review file with a million reviews
    • Using Jupyter notebook create a program to read the million reviews and get the following
        ◦ Item having the least rating
        ◦ Item having most rating
        ◦ Item having the longest reviews
    • Transform: change the date MM-DD-YYYY format
    • Show a desired data frame operation which you learnt recently
    • Convert the whole file into Parquet file after transforming.
    • Upload code to Github and complete Readme.md which anyone can understand

Step 1 : Create a spark session if you have already installed spark on your local system or else install spark and install python library for pyspark i,e pyspark  

Step 2 : After downloading review_Electronics_5.json.gz extarct the file, you will get  review_Electronics_5.json  and read the same file as pyspark dataframe, from this file we will get the longest review , date and reviewerID

review_df = spark.read.json('reviews_Electronics_5.json')

Step 3 : As mentioned we have to change the date format to MM-DD-YYYY for that we have to modify the date column present in dataframe as new column to dataframe and rename the column name

Step 4 : Now we have to read rating_Electronics.csv for item having least rating and item having most rating as lowrate, highrate with reviewerID and create a Pyspark dataframe for same csv file

Step 5 : After creating the both datarames and modifying the date format to MM-DD-YYYY we will join the both datafarme with selected columns based on reviewerID, this is the final datafarme we will get which consist of reviewText, lowrate,highrate and Date 

Step 6 : Now we have to save the final datafarme to a paraquet file 

df.write.parquet('Assignment1paraquetfile')
