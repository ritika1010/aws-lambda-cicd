import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = 'us-east-1'
HOST = 'search-photos-v1-ipvcdd53gpgk4q3fegqioz7f4m.us-east-1.es.amazonaws.com'
INDEX = 'images'

s3 = boto3.client('s3')
client = boto3.client('lexv2-runtime')

def send_msg_toLex(msg_from_user):
  # Initiate conversation with Lex
  response = client.recognize_text(
    botId='2RPF9XEM9A', # MODIFY HERE
    botAliasId='TSTALIASID', # MODIFY HERE
    localeId='en_US',
    sessionId='testuser',
    text=msg_from_user)
  
  msg_from_lex = response.get('messages', [])
  if msg_from_lex:
    print(f"Message from Chatbot: {msg_from_lex[0]['content']}")
    print(response)
  
  slots = response.get('sessionState', {}).get('intent', {}).get('slots', {})
  print("SLOTS =====", slots)
  result_array = []
  for key, value in slots.items():
    if key == 'key1':
        result_array.append(value['value']['interpretedValue'])
    if key == 'key2':
        result_array.append(value['value']['interpretedValue'])
    
  print(result_array)
  return result_array
  

def lambda_handler(event, context):
    
    print ("EVENT ___" , event)
    print ("CONTEXT ___" , context)
    
    msg_from_user = event['queryStringParameters']['q']
    
    print(f"Message from frontend: {msg_from_user}")
    
    keywords = send_msg_toLex(msg_from_user)
    print(keywords)
    results = []
    print("#SEARCH OPENSEARCH")
    for keyword in keywords:
        print(keyword,"-->", get_singular(keyword))
        keyword_results = query(get_singular(keyword))  # Call the query function for the current keyword
        results.extend(keyword_results)
    print('Results from opensearch : ' ,results)
    images = []
    responseArray = []
    output = results
    if output is not None:
        for i in output:
            # print("i is "+str(i))
            json_object = {}
            s3_object_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': i["bucket"], 'Key': i["objectKey"]},
            ExpiresIn=3600  # URL expiration time in seconds
            ) 
            print("S3 URL " , s3_object_url)
            # json_object['url'] = "https://s3.amazonaws.com/" + i["bucket"] + "/" + i["objectKey"]
            json_object['url'] = s3_object_url
            json_object['labels'] = i["labels"]
            responseArray.append(json_object)
            
            
                
        print ('Response array ---- : ' , responseArray)
    # if results:
    #     for r in results:
    #         images.append(get_image_from_s3(r["objectKey"],r["bucket"]))
    #         # print("next image")
    #     print(images)
        
    else:
        print("No search result")
    
    return {
          "statusCode": 200,
          "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin" : "*"
          },
          "body": json.dumps({
                "results":responseArray
                })
        }
    
def get_image_from_s3(key,bucket):
    
    try:
        #Get bucket object when uploaded
        response = s3.get_object(Bucket=bucket, Key=key)
        # print("EVENT ----", response)
        # print("CONTENT TYPE: " + response['ContentType'])
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e  
        
#OPENSEARCH      
def query(term):
    # q = {'size': 5, 'query': {'multi_match': {'query': term}}}

    q = { 'size': 5,
        "query": {
            "bool": {
                "should": [
                    {"match": {"labels": term}},
                ]
            }
        }
    }
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    # print("res -----" ,res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
                    
                    
PLURAL_TO_SINGULAR_SUFFIX_MAPPING = [
    ('people', 'person'),
    ('men', 'man'),
    ('women', 'woman'),
    ('menus', 'menu'),
    ('us', 'us'),
    ('ss', 'ss'),
    ('is', 'is'),
    ("'s", "'s"),
    ('ies', 'y'),
    ('es', 'e'),
    ('s', '')
]

def get_singular(word):
    if word is None or word=="":
        return ""
    for suffix,singular_suffix in PLURAL_TO_SINGULAR_SUFFIX_MAPPING:
        if word.endswith(suffix):
            return word[:-len(suffix)] + singular_suffix
    return word