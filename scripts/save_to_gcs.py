from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('triviapractice_bucket')

blob = bucket.blob('chatgpt_output/friends_2023-04-26_17-54-26.json')
blob.upload_from_filename('/home/rich2/airflow/scripts/chatgpt_output/friends_2023-04-26_17-54-26.json')