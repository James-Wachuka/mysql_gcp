
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator # for executing python functions
from airflow.operators.python import PythonVirtualenvOperator # for working with venvs airflow
from airflow.hooks.mysql_hook import MySqlHook # for connecting to local db
from datetime import timedelta

# function to create a dataset in bigquery to store data
def create_dataset():
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
    import os
    
    # setting application credentials to access biqguery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "data/introduction-to-gcp.json"
    client = bigquery.Client()
    
    """
    Create a dataset in Google BigQuery if it does not exist.
    :param dataset_id: Name of dataset
    :param region_name: Region name for data center, i.e. europe-west2 for London
    """
    dataset_id = 'dataset'
    region_name = 'europe-west2'
    
    reference = client.dataset(dataset_id)

    try:
        client.get_dataset(reference)
    except NotFound:
        dataset = bigquery.Dataset(reference)
        dataset.location = region_name
        
        dataset = client.create_dataset(dataset)
  

# function to truncate tables before inserting data
def truncate():
    from google.cloud import bigquery
    import os

    # setting application credentials to access biqguery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "data/introduction-to-gcp.json"

    # tables to truncate in biquery (*this service task is billed)
    table1 = 'dataset.product_dem'
    table2 = 'dataset.toporders'
    table3 = 'dataset.customer_spe'

    # Truncate a Google BigQuery table
    client = bigquery.Client()
    query1 = ("DELETE FROM "+ table1 +" WHERE 1=1")
    query2 = ("DELETE FROM "+ table2 +" WHERE 1=1")
    query3 = ("DELETE FROM "+ table3 +" WHERE 1=1")

    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    query_job1 = client.query(query1, job_config=job_config)
    query_job2 = client.query(query2, job_config=job_config)
    query_job3 = client.query(query3, job_config=job_config)

# function to insert data from a pandas df to bigquery
def insert():
    from google.cloud import bigquery
    from airflow.hooks.mysql_hook import MySqlHook # for connecting to local db
    import pandas as pd
    import os

    #setting application credentials to access biqguery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']= "data/introduction-to-gcp.json"
    
    # connecting to local db to query classicmodels db
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default', schema='classicmodels')
    connection = mysql_hook.get_conn()

    # querying the source db
    # products with highest number purchase
    query1 =""" 
    SELECT productName , SUM(quantityOrdered) AS quantity_ordered\
        FROM  products, orderdetails\
        WHERE products.productCode = orderdetails.productCode\
        GROUP BY productName\
        ORDER BY quantity_ordered DESC\
        LIMIT 20;
        """
    
    # customers that have made the most orders
    query2 = """
    SELECT contactFirstName, contactLastName , COUNT(*) AS number_of_orders\
        FROM  customers, orders\
        WHERE customers.customerNumber = orders.customerNumber\
        GROUP BY customerName\
        ORDER BY number_of_orders DESC\
        LIMIT 20;
        """
    
    # customers that have spent more
    query3 = """ 
    SELECT contactFirstName , contactLastName, SUM(quantityOrdered*priceEach) AS total_amount_spent\
        FROM  customers, orders, orderdetails\
        WHERE customers.customerNumber = orders.customerNumber AND orderdetails.orderNumber= orders.orderNumber\
        GROUP BY customerName\
        ORDER BY total_amount_spent DESC\
        LIMIT 10;
        """
  
    sql_query1 = pd.read_sql_query(query1, connection)
    sql_query2 = pd.read_sql_query(query2, connection)
    sql_query3 = pd.read_sql_query(query3, connection)
    df1 = pd.DataFrame(sql_query1)
    df2 = pd.DataFrame(sql_query2)
    df3 = pd.DataFrame(sql_query3)
    client = bigquery.Client()
    # load the data to bigquery tables
    client.load_table_from_dataframe(df1, 'dataset.product_dem')
    client.load_table_from_dataframe(df2, 'dataset.toporders')
    client.load_table_from_dataframe(df3, 'dataset.customer_spe')


def message():
    print("data successfully loaded into gc-bigquery")


default_args = {
 'owner':'airflow',
 'depends_on_past' : False,
 'start_date': airflow.utils.dates.days_ago(7),
}

mysql_to_gcp = DAG(
    'mysql-to-gcp', #name of the dag
    default_args = default_args,
    schedule_interval = timedelta(minutes=20),
    catchup = False
)


creating_dataset = PythonVirtualenvOperator(
    task_id='creating-a-dataset-in-bigquery',
    python_callable = create_dataset,
    requirements = ["google-cloud-bigquery","google-cloud-bigquery-storage"],
    system_site_packages=False,
    dag = mysql_to_gcp,
)

truncating_tables = PythonVirtualenvOperator(
    task_id='truncating-tables-in-bigquery',
    python_callable = truncate,
    requirements = ["google-cloud-bigquery","google-cloud-bigquery-storage"],
    dag = mysql_to_gcp,
)

inserting_data = PythonVirtualenvOperator(
    task_id='inserting-data-into-bigquery',
    python_callable = insert,
    requirements = ["google-cloud-bigquery","google-cloud-bigquery-storage"],
    dag = mysql_to_gcp,
)


message_out = PythonOperator(
    task_id = 'task-complete-message',
    python_callable = message,
    dag = mysql_to_gcp,
)

creating_dataset >> truncating_tables >> inserting_data >> message_out