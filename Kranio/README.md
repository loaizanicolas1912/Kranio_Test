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

## ARCHITECTURE
I have followed the architecture for this problem using Airflow for orchestration of Transform and Load files
![arqui](https://user-images.githubusercontent.com/66838187/171284842-99e2e1b2-23bf-4411-a416-855253099a12.PNG)

## FILES
1) Airflow_Kranio_dag.py = This file contain Dag and code about orchestration with Airflow using PapermillOperator 
![arqui](https://user-images.githubusercontent.com/66838187/171286881-06812759-71f8-44f2-8673-55a777cab5f9.PNG)
