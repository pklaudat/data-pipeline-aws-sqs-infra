import os
import logging as log
import json
import boto3


def append_data_to_s3(existing_data, new_data, source):
    s3_client = boto3.client('s3')
    updated_data = existing_data + new_data  # You may need to format this data as per your requirements
    s3_client.put_object(Bucket=os.environ['BUCKET_NAME'], Key=source, Body=updated_data)

def lambda_handler(event, context):

    log.info(event, context)

    sqs_client = boto3.client("sqs")
    s3_client = boto3.client("s3")

    queue_url = os.environ["QUEUE_URL"]

    # Receive message from SQS queue
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["SentTimestamp"],
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
        VisibilityTimeout=0,
        WaitTimeSeconds=0,
    )

    if 'Messages' in response:
        for message in response['Messages']:
            # Extract the new data from the SQS message
            new_data = json.loads(message['Body'])
            
            log.info(f'new data just arrived: {new_data}')
            # Get the existing data from S3
            existing_object = s3_client.get_object(Bucket=os.environ["BUCKET_NAME"], Key=os.environ["S3_KEY"])
            existing_data = existing_object['Body'].read().decode('utf-8')

            # Append new data to the existing data
            append_data_to_s3(existing_data, new_data, os.environ["S3_KEY"])

            # Delete the SQS message to remove it from the queue
            sqs_client.delete_message(
                QueueUrl=os.environ['QUEUE_URL'],
                ReceiptHandle=message["ReceiptHandle"]
            )


    



            
    
    
    



