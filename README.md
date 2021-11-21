# Spark_Project
## Overview
 In this project, i used Python to create Spark jobs with the goal of leveraging Spark's processing engine program to produce a report of the total number of accidents per make and
year of the car. I utilized data from an automobile tracking platform that tracks the history of important incidents after the initial sale of a new vehicle. 

## Summary of files
- **Data.csv**: Input data file of car incidents of different types ("R"= Repair, "I"= Initial Sale, "A"=Accident). Information regarding car make and year is null in Type A and Type I rows 
- **Spark_script.py** - Pyspark file which creates an RDD from data and specifies the RDD transformations and actions 
- **Execution_log.txt** - the output from terminal when running locally

## Running Spark Locally
``` 
spark-submit spark_script.py
```
Output:
```
Nissan-2003, 1
Mercedes-2015, 2
Mercedes-2016, 1
```
