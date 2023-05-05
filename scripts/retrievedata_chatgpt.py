import openai
import json
import datetime
import random
import os
import pandas as pd

# Config
openai.api_key = "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"
folder_path = 'chatgpt_output'
categories = ['US Geography','US History', 'Friends','US Music Awards','Seinfield','Olympics','Oscars','US Companies','Razzies','US Alcohol']
random_category = random.choice(categories)

# Define the prompt
prompt = f"5 random trivia questions about {random_category}, no multiple choice type questions. I want the output in pipe delimited output: number|question|answer"
# Generate a response using the OpenAI API
response = openai.Completion.create(
    engine="text-davinci-003",
    prompt=prompt,
    max_tokens=1000
)

#Add Category to the output
response['Category'] = random_category

# Save JSON object to file and timestamp it
category_text = random_category.replace(' ','').lower()
current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
with open(f'scripts/chatgpt_output/{category_text}_{current_timestamp}.json', 'w') as output_file:
    json.dump(response, output_file)


