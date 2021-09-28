# sync_spark_jobs
This repo includes PySpark jobs generic enough to be executed on EMR, Databricks

Contact: Chinmay Arankalle (ak.chinmay@gmail.com)

----
This project includes the PySpark jobs which revolves around online casino gaming scenarios.It has from simple to 
intense performance test jobs which with higher amount of data could
test the underlying Spark Cluster

Below are the technologies which I have used and 
will be required to run the workflow

##### Requirements
- Python 3.7.x
- Spark 2.4 (local single node or multi-node cluster)
- Python packages
    - pyspark
    - argparse
    
##### Jobs
Following jobs are part of project:
1. `get_players_with_most_events`: This job exports the player names with most events
2. `get_pivoted_events_type_stats_by_month`: This job exports the pivoted event stats by month. So bascially for every month aggregated event stats will be shown
3. `get_pivoted_events_type_stats_by_year`: This job exports the pivoted event  stats by year
4. `get_region_with_most_games`: This job exports the regions with most games played
5. `get_region_with_most_games_in_dates`: This job exports the regions with most games played with a date range
6. `get_stats_for_each_region`: This job exports aggregated stats for each region

##### How to RUN it
This codebase is argument driven, you need to pass the specific argument 
to run a specific job.

e.g. `bash run.sh --scope EVENT --job get_players_with_most_events -a CALL --inputfiles ./export/event_sept_2021.json ./export/player_sept_2021.json --exportpath ./export/`

Following is the reference of parameters:

`--scope/-s` (required): This parameter defines the scope of the jobs (`EVENT`/`GAME`)

`--jobs/-j`: This parameter specifies the job from the scope, you can 
pass `-l` to list all the jobs from a scope

`--arguments/-a`: This parameter passes dynamic arguments for each job, these
arguments are specific to job

`--inputfiles/-files`: Input file paths are specified here, If there are more 
than one source involved then primary scope source comes first and then others
e.g. for `EVENT` scope event source file will come first and then others

`--exportpath/-ep`: Here the export path should be provided, feel free to provide 
`S3` path

`--listjobs/-l`: You can view all the jobs from a scopes here
e.g. `bash run.sh --scope EVENT --listjobs`


##### Modules and code understanding

The project contains following packages and modules:

1. jobs : This package contains the business logic of the project
   
   1.1 event: This is events entity and jobs
   1.2 game: This is game entity and jobs
   1.3 player: This is player entity and jobs
2. io : This package contains the io read write logic<br/>
    2.1 spark_connection : This module connects to Spark cluster<br/>
    2.2 export: This module handles the export logic 
4. util : This package contains the common logic
5. main.py : This is the main module which is responsible for orchestration of all the activities

##### Reference

The clean code guidelines are followed as per [PEP8](https://www.python.org/dev/peps/pep-0008/) 
