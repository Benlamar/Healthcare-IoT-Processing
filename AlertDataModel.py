import boto3
import time
import json
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

aggr_table = dynamodb.Table('bsm_agg_data')
anomaly_table = dynamodb.Table('anomaly_table')

response = aggr_table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = aggr_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

for i in data:
    reading = i['payload']
    if (reading['datatype'] == 'HeartRate') and ((60 > int(reading['value'])) or (int(reading['value']) > 100)):
        anomaly_table.put_item(Item=reading)
        print("Anomaly detected, entry added in DynamoDB Table")
    elif (reading['datatype'] == 'SPO2') and ((85 > int(reading['value'])) or (int(reading['value']) > 110)):
        anomaly_table.put_item(Item=reading)
        print("Anomaly detected, entry added in DynamoDB Table")
    elif (reading['datatype'] == 'Temperature') and ((96 > int(reading['value'])) or (int(reading['value']) > 101)):
        anomaly_table.put_item(Item=reading)
        print("Anomaly detected, entry added in DynamoDB Table")
    