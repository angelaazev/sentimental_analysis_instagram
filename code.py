from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from instagrapi import Client
from instagrapi.exceptions import LoginRequired
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient
from azure.datalake.store import lib,core
import pandas as pd
import logging
import tempfile
import os

def upload_csv_to_azure_blob(dataframe, container_name, blob_name, connection_string):
    # Save the dataframe to a temporary CSV file
    temp_csv_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
    dataframe.to_csv(temp_csv_file.name, index=False)
    temp_csv_file.close()

    try:
        # Create BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a container client
        container_client = blob_service_client.get_container_client(container_name)

        # Create a blob client
        blob_client = container_client.get_blob_client(blob_name)

        # Upload the CSV file
        with open(temp_csv_file.name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"CSV file '{blob_name}' uploaded to Azure Blob Storage successfully.")

    finally:
        # Delete the temporary CSV file
        os.unlink(temp_csv_file.name)
credential = AzureKeyCredential("azure_key")
text_analytics_client = TextAnalyticsClient(endpoint="https://namespace.cognitiveservices.azure.com/", credential=credential)

cl = Client()
cl.login(ACCOUNT_USERNAME, ACCOUNT_PASSWORD)
cl.dump_settings("session.json")

ACCOUNT_USERNAME = 'user_name'
ACCOUNT_PASSWORD = 'password'

#get the user id from the username account and uses it to get the medias
user_id = cl.user_id_from_username(ACCOUNT_USERNAME)
medias = cl.user_medias(user_id, 20)

#get the last media pk
pk_last_media = medias[1].pk

#uses the last media pk to get the last media_id
media_id = cl.media_id(pk_last_media)

#get the comments for the last media published
comment = cl.media_comments(media_id)

#create the doc that will be analysed 
for i in range(len(comment)):
    document = []
    itens = comment[i].text
    document.append(itens)
    print(document)

#sentimental analysis
response = text_analytics_client.analyze_sentiment(document, language="pt")
result = [doc for doc in response if not doc.is_error]

for doc in result:
    print("Overall sentiment: {}".format(doc.sentiment))
    print("Scores: positive={}; neutral={}; negative={} \n".format(
        doc.confidence_scores.positive,
        doc.confidence_scores.neutral,
        doc.confidence_scores.negative,
    ))

#creating the dataframe
dic = {'positive': [doc.confidence_scores.positive], 'neutral': [doc.confidence_scores.neutral], 'negative': [doc.confidence_scores.negative], 'overall_sentiment': [doc.sentiment]}
df = pd.DataFrame(data=dic)
df

#saving into blob storage
container_name = "insta"
blob_name = "df.csv"
connection_string = "blob_storage_connection_string"

upload_csv_to_azure_blob(df, container_name, blob_name, connection_string)
