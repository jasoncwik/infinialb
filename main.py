import os
import boto3
from infinialb import apply_round_robin_to_client

# Demo of how to use the client-side load balancer

# Read environment variables
access_key_id = os.getenv('ACCESS_KEY_ID')
secret_access_key = os.getenv('SECRET_ACCESS_KEY')
endpoint_url = os.getenv('ENDPOINT_URL')

print(f"Endpoint URL: %s" % endpoint_url)

# Create an S3 client with custom endpoint
s3_client = boto3.client('s3',
                         aws_access_key_id=access_key_id,
                         aws_secret_access_key=secret_access_key,
                         endpoint_url=endpoint_url,
                         verify=False)

# Enable the client-side load balancer
apply_round_robin_to_client(s3_client)

print("Listing buckets...")
print(s3_client.list_buckets())
