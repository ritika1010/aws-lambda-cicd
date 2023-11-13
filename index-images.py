import json
import urllib.parse
import boto3
rekognition = boto3.client('rekognition')

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        #Get bucket object when uploaded
        response = s3.get_object(Bucket=bucket, Key=key)
        print("EVENT --", response)
        print("CONTENT TYPE: " + response['ContentType'])
        
        # print(response['x-amz-meta-customlabels'])
        
        #If image is uploaded
        if "image" in response['ContentType']:
            
            #Image sent to Rekognition to extract labels
            image = response['Body'].read()
            response_recognition = rekognition.detect_labels(Image={'Bytes': image})
            labels = [label['Name'] for label in response_recognition['Labels']]
            print("Labels found:", labels)
            # labels.append(response['x-amz-meta-customlabels'])
            
            #Image has Metadata
            s3_object_metadata = s3.head_object(Bucket=bucket, Key=key)
            if s3_object_metadata["Metadata"]:
                print("Metadata  contents found:", s3_object_metadata["Metadata"])
                metadata = s3_object_metadata["Metadata"]
                custom_labels = metadata.get("customlabels")
                if custom_labels is not None:
                    print(custom_labels) 
                    labels.append(custom_labels)
            else:
                print("metadata empty")
                
            #Prepare Json for sending to OS
            # {
            #         “objectKey”: “my-photo.jpg”,
            #         “bucket”: “my-photo-bucket”,
            #         “createdTimestamp”: “2018-11-05T12:40:02”,
            #         “labels”: [
            #              “person”,
            #              “dog”,
            #              “ball”,
            #              “park”
            #              ] 
            # }
            
            data = {}
            
            data["objectKey"] = key
            data["bucket"] = bucket
            data["createdTimestamp"] = event['Records'][0]['eventTime']
            data["labels"] = labels
            print(data)
            query(data)
        return {
        'statusCode': 200,
        'headers':{
            'Access-Control-Allow-Origin':'*',
            'Access-Control-Allow-Credentials':True,
            'Access-Control-Request-Headers':'POST, PUT, GET, OPTIONS',
            'Access-Control-Allow-Headers':'*'
            
            },
        }
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
        
        
import requests

def insert_os(data):
    print("inside insert")
    endpoint = "https://search-images-s5gkffqwg5yhsfednmdbnxtkze.us-east-1.es.amazonaws.com"  # Replace with your OpenSearch endpoint
    index = "images_key"

    headers = {
        "Content-Type": "application/json"
    }

    json_data = json.dumps(data)


    response = requests.post(f"{endpoint}/{index}/_doc/", json=json_data, headers=headers)
    print(response)
    if response.status_code == 201:
        print("Data inserted successfully")
    else:
        print("Failed to insert data")

from botocore.exceptions import ClientError

import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


REGION = 'us-east-1'
HOST = 'search-photos-v1-ipvcdd53gpgk4q3fegqioz7f4m.us-east-1.es.amazonaws.com'
INDEX = 'images'

def query(data):
    # q = {'size': 5, 'query': {'multi_match': {'query': term}}}
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    json_data = json.dumps(data)

    res = client.index(index=INDEX, body=json_data)
    print("res ----", res)

    document_id = res['_id']
    print("Document ID:", document_id)
    return res

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)