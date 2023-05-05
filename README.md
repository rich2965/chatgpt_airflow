# chatgpt_airflow
Airflow project to periodically pull ChatGPT responses and load to a Postgres database hosted on Google CloudSQL

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
