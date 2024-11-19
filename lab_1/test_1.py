import json
import os
from urllib.parse import unquote
import boto3
from PyPDF2 import PdfFileReader, PdfFileWriter
from botocore.client import Config


def pdf_format(file):
    """
        Formats a PDF file by reading its content and adding custom metadata.

        :param file: The path to the PDF file to be formatted.
        :return: The path to the newly formatted PDF file.
        """
    fin = open(file, 'rb')
    reader = PdfFileReader(fin)
    writer = PdfFileWriter()
    writer.appendPagesFromReader(reader)
    metadata = reader.getDocumentInfo()
    writer.addMetadata(metadata)
    # Write your custom metadata here:
    writer.addMetadata({
        '/Some': 'Example'
    })
    fout = open(file.split('.')[0]+'.pdf', 'wb')
    writer.write(fout)
    fin.close()
    fout.close()
    return (file.split('.')[0]+'.pdf')


def lambda_handler(event):
    """
        AWS Lambda function handler that processes S3 events to format PDF files
        and start a Textract job.

        :param event: The event data received from S3.
        :return: A response indicating the status of the processing.
        """
    print('Received event is ', json.dumps(event, indent=2))
    # Set directory for preprocessing PDFs
    os.chdir('/tmp/')
    for record in event['Records']:
        print(record)
        # Extract keys from event
        bucket = record['s3']['bucket']['name']
        url_key = record['s3']['object']['key']
        key = unquote(url_key).replace('+',' ')
        key_list = key.split('/')
        # Define original directory in S3
        directories = ''
        for item in key_list:
            if item != key_list[-1]:
                directories += (item+'/')
        print (key)
        s3 = boto3.client('s3')
        # Download s3 pdf file that triggered Lambda
        s3.download_file(bucket, key, key.split('/')[-1])
        # Format PDF for Textract
        new_file = pdf_format(key.split('/')[-1])
        # upload new file to s3
        # Start textract job with new file
        # client = getClient('textract')
        # response = client.start_document_analysis(
        #     DocumentLocation={
        #         'S3Object': {
        #             'Bucket': bucket,
        #             'Name': directories+new_file
        #         }
        #     },
        #     FeatureTypes=[
        #         'TABLES','FORMS'
        #     ],
        #     # ClientRequestToken=reqId,
        #     JobTag='HPtextract',
        #     NotificationChannel={
        #         'SNSTopicArn': SNS_TOPIC,
        #         'RoleArn': SNS_ROLE
        #     },
        #     OutputConfig={
        #         'S3Bucket': OUTPUT_BUCKET,
        #         'S3Prefix': key.split('.')[0]
        #     }
        # )
        # print(response)
    
    return {
        'statusCode': 200,
        'body': 'Successfully converted the file ' + new_file
    }


def get_client(name, aws_region=None):
    """
        Creates a Boto3 client for a specified AWS service.

        :param name: The name of the AWS service (e.g., 's3', 'dynamodb').
        :param aws_region: The AWS region to connect to (optional).
        :return: A Boto3 client for the specified service.
        """
    config = Config(
        retries = dict(
            max_attempts = 30
        )
    )
    if(aws_region):
        return boto3.client(name, region_name=aws_region, config=config)
    else:
        return boto3.client(name, config=config) 


def get_resource(name, aws_region=None):
    """
        Creates a Boto3 resource for a specified AWS service.

        :param name: The name of the AWS service (e.g., 's3', 'dynamodb').
        :param aws_region: The AWS region to connect to (optional).
        :return: A Boto3 resource for the specified service.
        """
    config = Config(
        retries = dict(
            max_attempts = 30
        )
    )

    if(aws_region):
        return boto3.resource(name, region_name=aws_region, config=config)
    else:
        return boto3.resource(name, config=config)
