import boto3
import time
import json
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

raw_table = dynamodb.Table('raw_data_table')
aggr_table = dynamodb.Table('bsm_agg_data')

response = raw_table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = raw_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])

heart_avg_list =[]
spo2_avg_list=[]
temp_avg_list = []
bsm2_heart_avg_list =[]
bsm2_spo2_avg_list =[]
bsm2_temp_avg_list = []

timestamp = datetime.fromisoformat(data['payload']['timestamp'])
check_min = int(timestamp.strftime("%M"))


for i in data:
    reading = i['payload']
    # print(reading)
    timestamp = datetime.fromisoformat(reading['timestamp'])
    curr_minute = int(timestamp.strftime("%M"))

    if reading['deviceid'] == 'BSM_G101':
        if (reading['datatype'] == 'HeartRate'):
            heart_avg_list.append(reading['value'])

        elif (reading['datatype'] == 'SPO2'):
            spo2_avg_list.append(reading['value'])

        elif (reading['datatype'] == 'Temperature'):
            temp_avg_list.append(reading['value'])
    else:
        if (reading['datatype'] == 'HeartRate'):
            bsm2_heart_avg_list.append(reading['value'])

        elif (reading['datatype'] == 'SPO2'):
            bsm2_spo2_avg_list.append(reading['value'])

        elif (reading['datatype'] == 'Temperature'):
            bsm2_temp_avg_list.append(reading['value'])

    if curr_minute != check_min:
        Item1 = {'deviceid':'BSM_G101',
                'timestamp': timestamp,
                'datatype': 'HeartRate',
                'average':sum(heart_avg_list)/len(heart_avg_list),
                'max':max(heart_avg_list),
                'min':min(heart_avg_list)}
        aggr_table.put_item(Item=Item1)

        Item2 = {'deviceid':'BSM_G101',
                'timestamp': timestamp,
                'datatype': 'SPO2',
                'average':sum(spo2_avg_list)/len(spo2_avg_list),
                'max':max(spo2_avg_list),
                'min':min(spo2_avg_list)}
        aggr_table.put_item(Item=Item2)

        Item2 = {'deviceid':'BSM_G101',
                'timestamp': timestamp,
                'datatype': 'Temperature',
                'average':sum(temp_avg_list)/len(temp_avg_list),
                'max':max(temp_avg_list),
                'min':min(temp_avg_list)}
        aggr_table.put_item(Item=Item2)

        Item1 = {'deviceid':'BSM_G102',
                'timestamp': timestamp,
                'datatype': 'HeartRate',
                'average':sum(bsm2_heart_avg_list)/len(heart_avg_list),
                'max':max(heart_avg_list),
                'min':min(heart_avg_list)}
        aggr_table.put_item(Item=Item1)

        Item2 = {'deviceid':'BSM_G102',
                'timestamp': timestamp,
                'datatype': 'SPO2',
                'average':sum(bsm2_spo2_avg_list)/len(spo2_avg_list),
                'max':max(spo2_avg_list),
                'min':min(spo2_avg_list)}
        aggr_table.put_item(Item=Item2)

        Item2 = {'deviceid':'BSM_G102',
                'timestamp': timestamp,
                'datatype': 'Temperature',
                'average':sum(bsm2_temp_avg_list)/len(temp_avg_list),
                'max':max(temp_avg_list),
                'min':min(temp_avg_list)}
        aggr_table.put_item(Item=Item2)
        check_min = curr_minute
        heart_avg_list =[]
        spo2_avg_list=[]
        temp_avg_list = []
        bsm2_heart_avg_list =[]
        bsm2_spo2_avg_list =[]
        bsm2_temp_avg_list = []