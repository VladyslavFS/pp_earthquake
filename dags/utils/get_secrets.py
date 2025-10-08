import boto3
import json
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name="eu-north-1"):
    #secret_name = "earthquake/aws/credentials"
    #secret_name = "earthquake/rds/postgres"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"Error fetching secret {secret_name}: {e}")
        raise