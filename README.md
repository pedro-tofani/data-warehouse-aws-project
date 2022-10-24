# Data Warehouse

## Description

This project was built during Udacity's nanodegree and aimed to create a database in AWS Redshift to support analytical analysis. The objective is to load data from logs stored on a S3 bucket to a datawarehouse located in the Redshift Cluster provided by AWS.
The logs, after loaded to two stanging tables were then loaded into some normalized tables.

Logs:
* s3://udacity-dend/log_data
* s3://udacity-dend/song_data

Tables:
* staging_events
* staging_songs

* songplays - Fact table: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* users - Dimensional table: user_id, first_name, last_name, gender, level
* songs - Dimensional table: song_id, title, artist_id, year, duration
* artists - Dimensional table: artist_id, name, location, lattitude, longitude
* time - Dimensional table: start_time, hour, day, week, month, year, weekday

## How To Run

1 - Install the requirements
```
pip install requirements.txt
```

2 - Configure dwh.cfg
  Some of the configurations are already fulfiled with recommended settings. The other ones must be completed with yours information. Some info migth be completed after creating the IAM Role and the cluester.
```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=
DWH_IAM_ROLE_NAME=

[IAM_ROLE]
ARN=

[S3]
LOG_DATA='s3://udacity-dend/log_data'
SONG_DATA='s3://udacity-dend/song_data/A/A'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'

[AWS]
KEY=
SECRET=
REGION=us-west-2

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large
DWH_CLUSTER_IDENTIFIER=
DWH_DB=
DWH_DB_USER=
DWH_DB_PASSWORD=
DWH_PORT=
DWH_IAM_ROLE_NAME=
```

3 - Create the IAM Role and the cluster
```
python3 app.py
```
  This command will start a CLI that will help you configuring your architeture.
  First create the IAM role (option 1)
  Then create the cluster (option 2). It may take a while and your cursor will be locked until the cluster is avalable. A verification will be made after 30 seconds.
  You can check your cluster status using the option 3.
  Open your TCP connection with option 4.
  Test your connection with option 5.
  When your work is done, delete the cluster and the IAM Role with options 6 and 7.

4 - Create the tables
```
python3 create_tables.py
```

5 - Load the data
```
python3 etl.py
```

6 - Test if everything is fine
```
python3 test.py
```
Your output should show each created table with the respectives columns and the total number of registers.

## Files description
* app.py - CLI to create and delete our aws infrastructure. The CLI can automatically calls new_IAM_role.py, new_Redshift_Cluster.py and redshift_cluster_actions.py
* new_IAM_role.py - Script create our IAM Role
* new_Redshift_Cluster.py - Script to create the cluster.
* redshift_cluster_actions.py - Auxiliar script to manipulate our cluster
* create_tables.py - Script to create the tables on our Redshift cluster
* etl.py - Extract, transform and load script.
* tests.py - Script to check if everything went well 