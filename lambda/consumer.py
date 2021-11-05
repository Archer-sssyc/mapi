import boto3
from setting import *
import json

if __name__ == '__main__':
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    dynamodb_client = session.client('dynamodb', region_name=REGION)
    rep = dynamodb_client.get_item(
        TableName='demo-recommended',
        Key={
            "user_id": {"S": "232b2fa5-bcca-4d05-8856-e0c7f4c4be19"},
            "log_time": {"S": "2021-11-03T09:04:59.974Z"}
        }
    )
    result = dict()
    if rep['ResponseMetadata']['HTTPStatusCode'] == 200:
        raw_data = rep["Item"]
        for (k, v) in raw_data.items():
            for (k1, v1) in v.items():
                if k1 in ('S', 'N','BOOL'):
                    result[k] = v1
                    break
        print(result)



