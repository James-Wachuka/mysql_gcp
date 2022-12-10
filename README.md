###### Mysql-to-GCP
An airflow dag that extracts data from Mysql db transforms and loads it to GCP-bigquery. 

###### Accessing the Biquery 
Create a gcp account activate biquery service and create IAM account. Extract keys/credentials in form of a json file that will be used to connect to biqguery.

Bigquery is accessed using the ```google-cloud-bigquery``` python library.Creating a client and and using given credentials provides connection to bigquery. C

to use bigquery, Create a bigquery service on GCP and acquire the keys.

#### bigquery dataset
Before loading data in biquery, a ```create_dataset``` method is used to create a dataset in bigquery where the tales/data will be stored

###### source data -classicmodels db
MySql contains a classicmodels db which is a database for a car retail company.
From the database we can extract the following.
* customers who made the most orders
* products that have the highest number of purchases
* customers who have spent more
this data is then extracted and transformed using pandas.

```airflow.hooks.mysql_hook import MySqlHook``` is used to connect to local db (MySql) fr0m airflow. setting host to ```host.docker.internal``` enables access to local db. Connection string/properties should be set  in airflow web UI under admin/connections menu

###### inserting data into biquery
```client.load_table_from_dataframe(df,'table_name')``` is a method used to insert data into biquery tables using dataframes created from queries and tables_names of the target tables in bigquery.

###### Automating with airflow
This job runs every 20 minutes. The ETL is seperated into 3 tasks ```creating_dataset >> truncating_tables >> inserting_data``` which are executed using ```PythonVirtualenvOperator``` in airflow

airflow is run in docker. A volume ```./data:/opt/airflow/data``` is added into the docker-compose file to store the classicmodels db and the json file that contains google application credentials

the dag file ```gcp_dag.py``` which is the complete dag is contained in the dags folder
