# Project 1: Data Modeling with Apache Cassandra
--------------------------

### Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.


### Project Overview
In this project, you'll apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, you will need to model your data by creating tables in Apache Cassandra to run queries. You are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

### Project Dataset
All of the data is at event_data folder. The directory of CSV files partitioned by date. 
Here are examples of filepaths to two files in the dataset. 
* event_data/2018-11-08-events.csv
* event_data/2018-11-09-events.csv


### Project Structure
Files and their purposes:

|<div align="center">File</div>|<div align="center">Info</div>|
|----------|--------|
|<div align="center">event_data</div>|<div align="center">CSV files partitioned by date.</div>|
|<div align="center">etc</div>|<div align="center">Contains a image file for jupyter notebook, and a csv file for processed data.</div>|
|<div align="center">Modeling_with_Apache_Cassandra.ipynb</div>|<div align="center">A Jupyter notebook file to create database and query data.</div>|

