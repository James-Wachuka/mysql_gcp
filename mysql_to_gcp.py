import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from sqlalchemy import create_engine


# importing credentials using json file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "data/introduction-to-gcp.json"

# create a dataset in bigquery
def create_dataset(dataset_id, region_name):
    """Create a dataset in Google BigQuery if it does not exist.
    :param dataset_id: Name of dataset
    :param region_name: Region name for data center, i.e. europe-west2 for London
    :return: Create dataset
    """
    
    client = bigquery.Client()
    reference = client.dataset(dataset_id)



    try:
        client.get_dataset(reference)
    except NotFound:
        dataset = bigquery.Dataset(reference)
        dataset.location = region_name
        
        dataset = client.create_dataset(dataset)
    
create_dataset('dataset','europe-west2')


# create connection to local db
engine = create_engine('mysql+pymysql://root:password@127.0.0.1:3306/classicmodels')

# truncate tables before inserting data
def truncate(table):
    """Truncate a Google BigQuery table
    
    :param sql: Google BigQuery dataset and table, i.e. competitors.products
    """
    
    client = bigquery.Client()

    query = ("DELETE FROM "+ table +" WHERE 1=1")

    job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
    query_job = client.query(query, job_config=job_config)

truncate('dataset.product_dem')
truncate('dataset.toporders')
truncate('dataset.customer_spe')

# function to insert data from a pandas df to bigquery
def insert(df, table):
    """Insert data from a Pandas dataframe into Google BigQuery. 
    
    :param df: Name of Pandas dataframe
    :param table: Name of BigQuery dataset and table, i.e. competitors.products
    :return: BigQuery job object
    """
    
    client = bigquery.Client()
    
    return client.load_table_from_dataframe(df, table)

# sql statement to read data from classic models database and load into bigquery
# products with highest number purchase
query1 = """
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

df1 = pd.read_sql(query1, con=engine)
df2 = pd.read_sql(query2, con=engine)
df3 = pd.read_sql(query3, con=engine)

print(df1)
job = insert(df1, 'dataset.product_dem')
job = insert(df2, 'dataset.toporders')
job = insert(df3, 'dataset.customer_spe')
