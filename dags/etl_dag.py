from airflow import DAG
from airflow.models import DAG, DagRun
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta , timezone
from textwrap import dedent
import json
import pandas as pd
import hashlib
from google.cloud import storage
from sqlalchemy import create_engine

# initialize Postgres engine
engine = create_engine('postgresql://root:Tester2965@34.69.30.119:5432/triviapractice')
engine.connect()

# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket_name = 'triviapractice_bucket'
bucket = storage_client.get_bucket(bucket_name)

with DAG(
    'etl_dag',
    default_args = {
    'owner': 'richc',
    'start_date': datetime(2023, 5, 3),
    'depends_on_past': False,
    'retries': 0,   
    'retry_delay': timedelta(minutes=5)},
    description='DAG for transforming the data from Cloud Storage and inputting into Cloud SQL Postgres',
    schedule_interval='*/60 * * * *', # runs every 10 minutes
    catchup=False
) as dag:

    dag.doc_md = __doc__

    def extract_data_gcp(**kwargs):
        ti = kwargs["ti"]

        # Define the time window to filter the GCP bucket blobs by
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        #Get the last DAG execution, if it exists, limit the timedelta else a large timewindow to cover everything
        if dag.get_latest_execution_date(): 
            time_window = timedelta(minutes=70)
        else:
            time_window = timedelta(days=1000)


        #list ot store the data from extraction 
        data_list = []

        # Iterate through all the blobs (i.e., files) in the bucket
        for blob in bucket.list_blobs():   
            last_modified = blob.updated.replace(tzinfo=timezone.utc) # Converts to UTC time of the Last modified date from GCP
            if (now - last_modified) <= time_window: #Only looks at files within the configured time_window so less data needs to be parsed
                # Check if file is JSON
                if blob.name.endswith('.json'):       
                    print("Processing json blob: {}".format(blob.name))
                    data = blob.download_as_string().decode('utf-8') #downloads the blob and decodes
                    json_data = json.loads(data) # load into JSON format
                    text = json_data["choices"][0]["text"].strip()  # extract 'text' field and remove leading/trailing whitespace
                    lines = text.split("\n")
                    for line in lines:
                        try:
                            columns= line.split("|")
                            new_row= {'id':hashlib.md5(columns[1].encode()).hexdigest(),
                                    'question':columns[1],
                                    'answer':columns[2],
                                    'source': 'ChatGPT',
                                    'category':json_data["Category"],
                                    'file_name':blob.name
                                    }
                            data_list.append(new_row)
                        except Exception:
                            print(Exception)
        ti.xcom_push("data_list", data_list) # push the data_list to be used in other tasks
    
    def transform_data(**kwargs):
        # Retrieve variables from previous tasks
        ti = kwargs["ti"]
        data_list = ti.xcom_pull(task_ids="extract_data_gcp", key="data_list")

        #Pull Data from Postgres Table to compare against Extracted blob data 
        query = 'SELECT * FROM question_testing'
        resultset = pd.read_sql_query(sql=query, con=engine.connect())

        # Load data_list to dataframe and Clean the data
        df = pd.DataFrame(data_list)
        df = df.dropna() # drops columns with null values
        df = df.drop_duplicates(subset=['question']) # drop duplicate questions
        df = df.apply(lambda x: x.str.replace('"', '')) # Remove double quotes from values

        # merge the two DataFrames on the 'id' column. Merging the target table and source data to ensure no dups
        merged = pd.merge(resultset, df, on='id', how='outer', indicator=True,suffixes=('_left',''))

        # filter out the rows that already exist in df_sql_result
        filtered_data = merged.loc[merged['_merge'] == 'right_only', df.columns]

        #Convert data back to JSON so it can be passed through XCOM
        data = filtered_data.to_dict()
        data_to_ingest_json = json.dumps(data)

        ti.xcom_push("data_to_ingest_json", data_to_ingest_json)

    

    def load_data(**kwargs):
        # Retrieve variables from previous tasks
        ti = kwargs["ti"]
        data_to_ingest_json = ti.xcom_pull(task_ids="transform_data", key="data_to_ingest_json")

        # Convert back to Dataframe
        data_to_ingest = pd.read_json(data_to_ingest_json)

        # ingest data into Postgres
        try:
            data_to_ingest.to_sql(name='question_testing',con=engine,if_exists='append',index=None)
        except Exception:
            print(Exception)
        print(f"Number of rows processed: {len(data_to_ingest)}")

    extract_task = PythonOperator(
        task_id='extract_data_gcp',
        python_callable=extract_data_gcp,
        dag=dag,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )    

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag,
    )

    extract_task >> transform_task >> load_task