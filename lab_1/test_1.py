import boto3
import json
import os
from urllib.parse import unquote
from PyPDF2 import PdfFileReader, PdfFileWriter
from botocore.client import Config


def pdf():
    fin = open('input.pdf','rb')

def pdfFormat(file):
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


def lambda_handler(event, context):
    print('Received event is ', json.dumps(event, indent=2))
    # Set directory for preprocessing PDFs
    os.chdir('/tmp/')
    for record in event['Records']:
        print(record)
        # Extract keys from event
        bucket = record['s3']['bucket']['name']
        urlKey = record['s3']['object']['key']
        reqId = record['responseElements']['x-amz-request-id']
        key = unquote(urlKey).replace('+',' ')
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
        new_file = pdfFormat(key.split('/')[-1])
        # upload new file to s3
        s3upload = s3.upload_file(new_file,bucket,directories+new_file)
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



def getClient(name, awsRegion=None):
    config = Config(
        retries = dict(
            max_attempts = 30
        )
    )
    if(awsRegion):
        return boto3.client(name, region_name=awsRegion, config=config)
    else:
        return boto3.client(name, config=config) 
            
def getResource(name, awsRegion=None):
    config = Config(
        retries = dict(
            max_attempts = 30
        )
    )

    if(awsRegion):
        return boto3.resource(name, region_name=awsRegion, config=config)
    else:
        return boto3.resource(name, config=config)

