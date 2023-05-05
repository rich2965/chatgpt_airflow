# chatgpt_airflow
Airflow project to periodically pull ChatGPT responses and load to a Postgres database hosted on Google CloudSQL. 
The intention of the project was to learn how to use Airflow, improve skills with GCP, and to take a more defined data engineering approach to pulling data for my Trivia app

The steps of the tool:

1) In the script, I specified a list of question categories I want ChatGPT to provide Questions and Answers for
2) Make the request and receive a response from ChatGPT. Data comes back as JSON
3) Add a category object to the resopnse and save the file to blob on Google Cloud Storage. This is all in 1 Dag
4) In another DAG, called etl_dal, I extract the most recent data in blob and process it. I compare against what's already been loaded in Postgres to check for duplicates. The process also does some additional cleaning with extra quotes and spaces
5) After the data has been cleaned, the dag ends with a load_task which loads the clean data into the Postgres database that's hostbed on Google Cloud SQL
6) The data is then used by the Trivia App

# Apache Airflow enablement 

On the Google VM, ran the following: 
sudo apt-get update
sudo apt-get install python3-pip -y
sudo pip3 install apache-airflow

airflow db init
airflow users create --username admin --password admin --firstname Rich --lastname Chung --role Admin --email rich.chung2965@gmail.com

airflow webserver -p 8080
airflow scheduler

# Set up Virtual Environment within Google VM
sudo apt-get install virtualenv
virtualenv airflow-env
source airflow-env/bin/activate
airflow webserver -p 8080
airflow scheduler
Airflow UI will be available at 35.188.162.190:8080  (IP of the VM:8080)

# Start airflow every time the VM turns on
sudo nano /etc/systemd/system/airflow.service

Copied and paste the following into the airflow.service file
[Unit]
Description=Airflow service
After=network.target

[Service]
User=rich2
Type=simple
ExecStart=/bin/bash -c 'source /home/rich2/airflow/airflow-env/bin/activate && airflow webserver -p 8080 & airflow scheduler'

[Install]
WantedBy=multi-user.target
