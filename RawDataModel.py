import boto3
from boto3.dynamodb.conditions import Key

class RawDataModel:
    def __init__(self, table_name, partition, sortkey) -> None:
        self.table_name = table_name
        self.partition = partition
        self.sortkey = sortkey

    def getTable(self):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(self.table_name)

        #Met 2
        # response = table.get_item(
        #     Key={'deviceid':self.partition,  'timestamp':self.sortkey})
        # print(response['Item']['payload'])

        #Met 2
        # response =table.scan()
        # data = response['Items']
        # for i in data:
        #     print(i['payload'])

        response = table.query(
            KeyConditionExpression=Key('deviceid').eq(self.partition))

        for i in response['Items']:
            print(i['payload'])