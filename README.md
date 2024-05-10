# Census-Data-Standardization-and-Analysis-Pipeline
# Overview : 

The Census Data Standardization and Analysis Pipeline to clean, process, and analyze census data from a given source, including data renaming, missing data handling, state/UT name standardization, new state/UT formation handling, data storage, database connection, and querying.

The goal is to ensure uniformity, accuracy, and accessibility of the census data for further analysis and visualization.

In this website we extract the data and process it to obtain insights and information that can be visualized in a user-friendly manner.

# How its done

Step 1 : We extract the data from the dataset provided.

Step 2 : We process the data and make it as a data frame.

Step 3 : Now we clean the data.

Step 4 : After cleaning the data, we are storing it in MongoDb as unstructured data.

Step 5 : We process the unstructured data and store in a MySql database in a structured format

Step 6 : We create a web application using Streamlit library of Python

Step 7 : In the application we provide data visualization option to the user depending the data fetched from the SQL table

Step 8 : Data Visualization is been done with the help of python libraries like plotly and pandas

# Outcome

Users can get an insight on the Census data in a visually appealing manner and easy to understand.

# Requirements

* streamlit
* streamlit_option_menu
* pandas
* docx
* pymongo
* mysql-connector-python
* sqlalchemy
* plotly

# To run the application

stream run .\census.py\

# Tasks

* Renaming the Column Names
* Renaming State Names
* New State Formation
* Finding and Processing missing Data
* Saving data to MongoDb and SQL
* Running query on database and showing result in streamlit
