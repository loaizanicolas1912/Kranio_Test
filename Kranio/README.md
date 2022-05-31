## Data Engineer Problem

Imagine we work for a movie studio companny, and we're trying to decide what will be our next great production. For that, you should do some
analytics based on past releases database found on kaggle movies dataset and present them for executives of the companny.

## Metrics
The final report should present the following metrics:

Most profitable movies by semester

Most profitable genres by year

Top 10 profitable movies by genres (maximum of 5 genres)

Movies popularity by month (to know the best release date)

Total of releases for each genre for the last 5 years

## Architecture
I have followed the architecture for this problem using Airflow for orchestration of Transform and Load files. Airflow is inside a local Docker container with the official Apache Airflow image

![arqui](https://user-images.githubusercontent.com/66838187/171284842-99e2e1b2-23bf-4411-a416-855253099a12.PNG)

## Files
1) Airflow_Kranio_dag.py = This file contain Dag and code about orchestration with Airflow using PapermillOperator 
![ELT_AIRFLOW](https://user-images.githubusercontent.com/66838187/171286970-d9b75043-a8c9-46fe-a4e7-41227b848093.PNG)
2) TransformPySpark.ipynb = This file contain all transformations with Spark - PySpark where this generates five csv files with data about metrics with folder structure data1p, data2p... until 5p
3) PandasLoad.ipynb = This file contain all transformations Pandas where this generates five visual graphs that represent the metrics calculated

## HOW DOES IT WORK

The dataset downloaded from Kaggle is processed as the file TransformPySpark.ipynb where the necessary transformations are created, these end up in 5 folders with their partitions joined with a csv file for each metric.

The file is responsible for going through each folder of the project, extracting the csv and generating a dataframe for each metric that will later be displayed.
