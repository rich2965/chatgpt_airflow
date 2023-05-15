from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
import openai
import json
import random
import os
import pandas as pd
from google.cloud import storage

#Load from Config File:

with open('/home/rich2/airflow/chatgpt_config.json', 'r') as f:
    config = json.load(f)

# Dag Default Args

with DAG(
    'chatgpt_dag',
    default_args = {
    'owner': 'richc',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 26),
    'retries': 0,   
    'retry_delay': timedelta(minutes=5)},
    description='DAG for running my Python script',
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    dag.doc_md = __doc__

    def chatgpt_request(**kwargs):
        # Config
        openai.api_key = config["api_key"]
        # categories = ['US Geography','US History', 'Friends','US Music Awards','Seinfield','Olympics','Oscars','US Companies','Razzies','US Alcohol']
        categories = config["categories"]
        random_category = random.choice(categories)
        random_category_name = random_category["name"]
        random_category_source = random_category["source"]
        ti = kwargs["ti"]
        print("SELECTED CATEGORY: " + random_category_name)
        print(random_category_source)
        # Define the prompt
        prompt = f"5 random trivia questions about {random_category_name} using {random_category_source}, no multiple choice type questions. I want the output in pipe delimited output: number|question|answer"
        # Generate a response using the OpenAI API
        try:
            response = openai.Completion.create(
                engine="text-davinci-003",
                prompt=prompt,
                max_tokens=1000
            )
        except Exception:
            print (Exception)
        #Add Category to the output
        response['Category'] = random_category_name
        ti.xcom_push("response", response)

    def save_response(**kwargs):
        ti = kwargs["ti"]
        chatgpt_response = ti.xcom_pull(task_ids="chatgpt_request", key="response")
        # Save JSON object to file and timestamp it
        category_text = chatgpt_response["Category"].replace(' ','').lower()
        current_timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f'{category_text}_{current_timestamp}.json'
        output_path = f'/home/rich2/airflow/scripts/chatgpt_output/{file_name}'
        with open(output_path, 'w') as output_file:
            json.dump(chatgpt_response, output_file)
        ti.xcom_push("output_path", output_path)
        ti.xcom_push("file_name", file_name)
        
        

    def upload_to_gcp(**kwargs):
        ti = kwargs["ti"]
        file_path = ti.xcom_pull(task_ids="chatgpt_saveresponse", key="output_path")
        file_name = ti.xcom_pull(task_ids="chatgpt_saveresponse", key="file_name")
        current_date = datetime.utcnow().strftime('%Y-%m-%d')
        client = storage.Client()
        bucket = client.get_bucket('triviapractice_bucket')
        blob = bucket.blob(f'chatgpt_output/{current_date}/{file_name}')
        blob.upload_from_filename(file_path)
        print(f"File_name saved to GCP: {file_name}")

    def delete_file(**kwargs):
        ti = kwargs["ti"]
        file_path = ti.xcom_pull(task_ids="chatgpt_saveresponse", key="output_path")
        # check if file exists
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_path} deleted successfully")
        else:
            print(f"{file_path} does not exist")


    request_task = PythonOperator(
        task_id='chatgpt_request',
        python_callable=chatgpt_request,
        dag=dag,
    )
    request_task.doc_md = dedent(
        """\
    #### Reqyest task
    A simple Request task to ask for data from ChatGPT.
    Will be Returned as JSON
    """
    )

    save_task = PythonOperator(
        task_id='chatgpt_saveresponse',
        python_callable=save_response,
        dag=dag,
    )
    save_task.doc_md = dedent(
        """\
    #### Save task
    A simple Save task to save the data from ChatGPT.
    Adds a category to the JSON data
    """
    )

    upload_to_gcp = PythonOperator(
        task_id='upload_to_gcp',
        python_callable=upload_to_gcp,
        dag=dag,
    )
    save_task.doc_md = dedent(
        """\
    #### Upload task
    Save the JSON file to GCP Cloud Storage
    """
    )

    delete_file = PythonOperator(
        task_id='delete_file',
        python_callable=delete_file,
        dag=dag,
    )
    save_task.doc_md = dedent(
        """\
    #### Upload task
    Save the JSON file to GCP Cloud Storage
    """
    )
    request_task >> save_task >> upload_to_gcp >> delete_file
