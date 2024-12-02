######################################################################################################################
 #  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
 #                                                                                                                    #
 #  Licensed under the Apache License, Version 2.0 (the License). You may not use this file except in compliance    #
 #  with the License. A copy of the License is located at                                                             #
 #                                                                                                                    #
 #      http://www.apache.org/licenses/LICENSE-2.0                                                                    #
 #                                                                                                                    #
 #  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES #
 #  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
 #  and limitations under the License.                                                                                #
 #####################################################################################################################

import copy
import datetime
import json
import logging
import os
import traceback
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event):
    """
        Handles incoming events and extracts Textract job information from the SNS message.

        :param event: The incoming event containing SNS data.
        :return: None
        """
    print("Received event: " + json.dumps(event))
    
    for record in event['Records']:
        message = json.loads(record['Sns']['Message'])
        print("Message: {}".format(message))

        request = {}
    
        request["jobId"] = message['JobId']
        request["jobTag"] = message['JobTag']
        request["jobStatus"] = message['Status']
        request["jobAPI"] = message['API']
        request["bucketName"] = message['DocumentLocation']['S3Bucket']
        request["objectName"] = message['DocumentLocation']['S3ObjectName']
        request["outputBucketName"] = os.environ['OUTPUT_BUCKET']
        
        print("Full path of input file is {}/{}".format(request["bucketName"],
                                                        request["objectName"]))

        process_request(request)


def get_job_results(jobId):
    """
        Retrieves the Textract job results by job ID.

        :param jobId: The Textract job ID.
        :return: A list of document analysis results.
        """
    texttract_client = get_client('textract', 'us-east-1')
    blocks = []
    analysis = {}
    response = texttract_client.get_document_analysis(
        JobId=jobId
    )
    analysis = copy.deepcopy(response)
    while True:
        for block in response["Blocks"]:
            blocks.append(block)
        if ("NextToken" not in response.keys()):
            break
        next_token = response["NextToken"]
        response = texttract_client.get_document_analysis(
            JobId=jobId,
            NextToken=next_token
        )
    analysis.pop("NextToken", None)
    analysis["Blocks"] = blocks
    
    total_pages=response['DocumentMetadata']['Pages']
    final_json_allpage=[]
    print(total_pages)
    for i in range(total_pages):
        this_page=i+1
        this_page_json=parsejson_inorder_perpage(analysis,this_page)
        final_json_allpage.append({'Page':this_page,'Content':this_page_json})
        print(f"Page {this_page} parsed")
    return final_json_allpage
    
    
def process_request(request):
    """
        Processes a Textract job request and stores the results in S3
        and DynamoDB.

        :param request: A dictionary containing job details.
        :return: A response containing the status code and body.
        """
    s3_client = get_client('s3', 'us-east-1')

    print("Request : {}".format(request))

    job_id = request['jobId']
    job_api = request['jobAPI']
    bucketName = request['bucketName']
    outputBucketName = request['outputBucketName']
    objectName = request['objectName']
    
    
    directory = objectName.split('/')

    upload_path = ''
    for subdirectory in directory:
        if subdirectory != directory[-1]:
            upload_path += (subdirectory+'/')
            
    file_name = directory[-1]
    
    file_name_no_ext = file_name.rsplit( ".", 1 )[ 0 ]
    
    upload_path = upload_path + file_name_no_ext + '/textract/'
    
    finalJSON_allpage = get_job_results(job_api, job_id)
    
    analyses_bucket_name = outputBucketName
    analyses_bucket_key = "{}".format(objectName.replace('.PDF', '.json'))
    s3_client.put_object(
        Bucket=analyses_bucket_name,  
        Key=upload_path + analyses_bucket_key,
        Body=json.dumps(finalJSON_allpage).encode('utf-8')
    )
    
    _write_to_dynamo_db("pdf-to-json", objectName, bucketName + '/'
                        + objectName, finalJSON_allpage)
    
    return {
        'statusCode': 200,
        'body': json.dumps(finalJSON_allpage)
    }


def find_value_block(key_block, value_map):
    """
        Finds the value block corresponding to a given key block.

        :param key_block: The key block to search for.
        :param value_map: A map of value blocks.
        :return: The corresponding value block.
        """
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result, blocks_map):
    """
        Extracts text from a block result using a provided block map.

        :param result: The block result to extract text from.
        :param blocks_map: A map of blocks.
        :return: The extracted text.
        """
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '    
                                
    return text


def find_key_value_inrange(response, top, bottom, thisPage):
    """
        Finds key-value pairs within a specified bounding box
        in the Textract response.

        :param response: The Textract response containing blocks.
        :param top: The top coordinate of the bounding box.
        :param bottom: The bottom coordinate of the bounding box.
        :param thisPage: The page number to search on.
        :return: A dictionary of key-value pairs found in the range.
        """
    blocks=response['Blocks']
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        if block['Page'] == thisPage:  
            block_id = block['Id']
            block_map[block_id] = block
            if (block['BlockType'] == "KEY_VALUE_SET" or
                    block['BlockType']=='KEY' or block['BlockType']=='VALUE'):
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block
                
    ## find key-value pair within given bounding box:            
    kv_pair={}
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        if ( value_block['Geometry']['BoundingBox']['Top']>=top and   
             value_block['Geometry']['BoundingBox']['Top']+
                value_block['Geometry']['BoundingBox']['Height']<=bottom ):
            kv_pair[key]=val
    return kv_pair


def get_rows_columns_map(table_result, blocks_map):
    """
        Maps rows and columns from a table block result.

        :param table_result: The table block result.
        :param blocks_map: A map of blocks.
        :return: A dictionary mapping row indices to column values.
        """
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        rows[row_index] = {}
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows


def get_tables_from_json_inrange(response, top, bottom, thisPage):
    """
        Retrieves tables within a specified bounding box in the Textract response.

        :param response: The Textract response containing blocks.
        :param top: The top coordinate of the bounding box.
        :param bottom: The bottom coordinate of the bounding box.
        :param thisPage: The page number to search on.
        :return: A list of tables found in the range.
        """
    blocks=response['Blocks']
    blocks_map = {}
    table_blocks = []
    for block in blocks:
        if block['Page'] == thisPage:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                if ( block['Geometry']['BoundingBox']['Top']>=top and   
                       block['Geometry']['BoundingBox']['Top']+
                         block['Geometry']['BoundingBox']['Height']<=bottom ):
                    table_blocks.append(block)

    if len(table_blocks) <= 0:
        return 
    
    AllTables=[]
    for table_result in table_blocks:
        tableMatrix=[]
        rows=get_rows_columns_map(table_result, blocks_map)
        for row_index, cols in rows.items():
            thisRow=[]
            for col_index, text in cols.items():
                thisRow.append(text)
            tableMatrix.append(thisRow)
        AllTables.append(tableMatrix)
    return AllTables
    
    
### get tables coordinate in range:
def get_tables_coord_inrange(response,top,bottom,thisPage):
    """
        Retrieves the coordinates of tables within a specified bounding box
        in the Textract response.

        :param response: The Textract response containing blocks.
        :param top: The top coordinate of the bounding box.
        :param bottom: The bottom coordinate of the bounding box.
        :param thisPage: The page number to search on.
        :return: A list of table coordinates found in the range.
        """
    blocks=response['Blocks']
    blocks_map = {}
    table_blocks = []
    for block in blocks:
        if block['Page'] == thisPage:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                if ( block['Geometry']['BoundingBox']['Top']>=top and   
                       block['Geometry']['BoundingBox']['Top']+
                         block['Geometry']['BoundingBox']['Height']<=bottom ):
                    table_blocks.append(block)

    if len(table_blocks) <= 0:
        return 
    
    AllTables_coord=[]
    for table_result in table_blocks:
        
        AllTables_coord.append(table_result['Geometry']['BoundingBox'])
    return AllTables_coord


def box_within_box(box1,box2):
    """
        Checks if one bounding box is completely within another bounding box.

        :param box1: The first bounding box.
        :param box2: The second bounding box.
        :return: True if box1 is within box2, otherwise False.
        """
    if (box1['Top']>= box2['Top'] and box1['Left']>=box2['Left'] and
            box1['Top']+box1['Height']<=box2['Top']+box2['Height'] and
            box1['Left']+box1['Width']<=box2['Left']+box2['Width']) :
        return True 
    else:
        return False 


def find_Key_value_inrange_notInTable(response,top,bottom,thisPage):
    """
        Finds key-value pairs within a specified bounding box that are not in a table.

        :param response: The Textract response containing blocks.
        :param top: The top coordinate of the bounding box.
        :param bottom: The bottom coordinate of the bounding box.
        :param thisPage: The page number to search on.
        :return: A dictionary of key-value pairs found in the range.
        """
    blocks=response['Blocks']
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        if block['Page'] == thisPage:
            block_id = block['Id']
            block_map[block_id] = block
            if (block['BlockType'] == "KEY_VALUE_SET" or block['BlockType']=='KEY'
                    or block['BlockType']=='VALUE'):
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block
     
    # get all table coordicates in range:
    AllTables_coord=get_tables_coord_inrange(response,top,bottom,thisPage)
    
    ## find key-value pair within given bounding box:            
    kv_pair={}
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        if ( value_block['Geometry']['BoundingBox']['Top']>=top and   
             value_block['Geometry']['BoundingBox']['Top']+
                value_block['Geometry']['BoundingBox']['Height']<=bottom ) :
            
            kv_overlap_table_list=[]
            if AllTables_coord is not None: 
                for table_coord in AllTables_coord:
                    kv_overlap_table_list.append(box_within_box(
                        value_block['Geometry']['BoundingBox'],table_coord
                    ))
                if sum(kv_overlap_table_list) ==0:
                    kv_pair[key]=val
            else: 
                kv_pair[key]=val
    return kv_pair
    
    
### function: take response of multi-page Textract, and page_number
### return order sequence JSON for that page Text1->KV/Table->Text2->KV/Table..
def parsejson_inorder_perpage(response,thisPage):
    """
        Parses the Textract response for a specific page and returns the text in order.

        :param response: The Textract response containing blocks.
        :param thisPage: The page number to parse.
        :return: A list of dictionaries representing the parsed text and associated key-value pairs and tables.
        """
        id_list_kv_table=[]
        for block in response['Blocks']:
            if block['Page'] == thisPage:
                if (block['BlockType']=='TABLE' or block['BlockType']=='CELL' \
                    or block['BlockType']=='KEY_VALUE_SET' or
                        block['BlockType']=='KEY' or
                        block['BlockType']=='VALUE' or
                        block['BlockType']=='SELECTION_ELEMENT'):

                    kv_id=block['Id']
                    if kv_id not in id_list_kv_table:
                        id_list_kv_table.append(kv_id)

                    child_idlist=[]
                    if 'Relationships' in block.keys():
                        for child in block['Relationships']:
                            child_idlist.append(child['Ids'])
                        flat_child_idlist = [item for sublist in child_idlist
                                             for item in sublist]
                        for childid in flat_child_idlist:
                            if childid not in id_list_kv_table:
                                id_list_kv_table.append(childid)
        text_list=[]
        for block in response['Blocks']:
            if block['Page'] == thisPage:
                if block['BlockType'] == 'LINE':

                    thisline_idlist=[]
                    thisline_idlist.append(block['Id'])
                    child_idlist=[]
                    if 'Relationships' in block.keys():
                        for child in block['Relationships']:
                            child_idlist.append(child['Ids'])
                        flat_child_idlist = [item for sublist in child_idlist
                                             for item in sublist]
                        for childid in flat_child_idlist:
                            thisline_idlist.append(childid)

                    set_line_id=set(thisline_idlist)
                    set_all_kv_table_id=set(id_list_kv_table)
                    if len(set_line_id.intersection(set_all_kv_table_id)) == 0:
                        this_dict={'Line':block['Text'],
                                  'Left':block['Geometry']['BoundingBox']['Left'],
                                  'Top':block['Geometry']['BoundingBox']['Top'],
                                  'Width':block['Geometry']['BoundingBox']['Width'],
                                  'Height':block['Geometry']['BoundingBox']
                                  ['Height']}
                        text_list.append(this_dict)

        final_json=[]
        for i in range(len(text_list)-1):
            this_text=text_list[i]['Line']
            this_top=text_list[i]['Top']
            this_bottom=text_list[i+1]['Top']+text_list[i+1]['Height']
            this_text_kv=find_key_value_inrange(response, this_top,
                                                this_bottom, thisPage)
            this_text_table=get_tables_from_json_inrange(response,
                                                         this_top, this_bottom, thisPage)
            final_json.append({this_text:{'KeyValue':this_text_kv,
                                          'Tables':this_text_table}})

        if (len(text_list) > 0):
            ## last line Text to bottom of page:
            last_text=text_list[len(text_list)-1]['Line']
            last_top=text_list[len(text_list)-1]['Top']
            last_bottom=1
            this_text_kv=find_key_value_inrange(response, last_top,
                                                last_bottom, thisPage)
            this_text_table=get_tables_from_json_inrange(response,
                                                         last_top, last_bottom, thisPage)
            final_json.append({last_text:{'KeyValue':this_text_kv,
                                          'Tables':this_text_table}})

        return final_json


def _write_to_dynamo_db(dd_table_name, id, full_file_path, full_pdf_json):
    """
        Writes data to a DynamoDB table, creating the table if it does not exist.

        :param dd_table_name: The name of the DynamoDB table.
        :param id: The ID to use as the primary key.
        :param full_file_path: The full file path of the PDF.
        :param full_pdf_json: The JSON representation of the PDF data.
        :return: None
        """
    # Get the service resource.
    dynamodb = get_resource('dynamodb')

    dd_table_name = dd_table_name \
                    .replace(" ", "-") \
                    .replace("(", "-") \
                    .replace(")", "-") \
                    .replace("&", "-") \
                    .replace(",", " ") \
                    .replace(":", "-") \
                    .replace('/', '--') \
                    .replace("#", 'No') \
                    .replace('"', 'Inch') 

    if len(dd_table_name) <= 3:
        dd_table_name = dd_table_name + '-xxxx'
        
    print("DynamoDB table name is {}".format(dd_table_name))

    # Create the DynamoDB table.
    try:
        
        existing_tables = list([x.name for x in dynamodb.tables.all()])


        if dd_table_name not in existing_tables:
            table = dynamodb.create_table(
                TableName=dd_table_name,
                KeySchema=[
                    {
                        'AttributeName': 'Id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Id',
                        'AttributeType': 'S'
                    },
                ],
                BillingMode='PAY_PER_REQUEST',
            )
            # Wait until the table exists, this will take a minute or so
            table.meta.client.get_waiter('table_exists').wait(
                TableName=dd_table_name
            )
            # Print out some data about the table.
            print("Table successfully created. Item count is: " +
                  str(table.item_count))
    # except dynamodb_client.exceptions.ResourceInUseException:
    except ClientError as e:
        if (e.response['Error']['Code'] in
                ["ThrottlingException",
                 "ProvisionedThroughputExceededException"]):
            msg = (f"DynamoDB ] Write Failed from DynamoDB, Throttling"
                   f" Exception [{e}] [{traceback.format_exc()}]")
            logging.warning(msg)
            raise e
        else:
            msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}]"
                   f" [{traceback.format_exc()}]")
            logging.error(msg)
            raise e

    except Exception as e:
         msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}]"
                f" [{traceback.format_exc()}]")
         logging.error(msg)
         raise Exception(e)
    
    table = dynamodb.Table(dd_table_name)

    try :
        table.put_item(Item=
            {
                'Id': id,
                'FilePath': full_file_path,
                'PdfJsonRegularFormat': str(full_pdf_json),
                'PdfJsonDynamoFormat': full_pdf_json,
                'DateTime': datetime.datetime.utcnow().isoformat(),
            }
        )
    except ClientError as e:
        if e.response['Error']['Code'] in ["ThrottlingException",
                                           "ProvisionedThroughputExceededException"]:
            msg = (f"DynamoDB ] Write Failed from DynamoDB,"
                   f" Throttling Exception [{e}] [{traceback.format_exc()}]")
            logging.warning(msg)
            raise e

        else:
            msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}]"
                   f" [{traceback.format_exc()}]")
            logging.error(msg)
            raise e

    except Exception as e:
        msg = (f"DynamoDB Write Failed from DynamoDB Exception [{e}]"
               f" [{traceback.format_exc()}]")
        logging.error(msg)
        raise Exception(e)


def dict_to_item(raw):
    """
        Converts a Python dictionary or primitive types to a format suitable for DynamoDB.

        :param raw: The input data, which can be a dictionary, string, or integer.
        :return: A dictionary formatted for DynamoDB, where strings are mapped to 'S' and integers to 'I'.
        """
    if type(raw) is dict:
        resp = {}
        for k,v in raw.items():
            if type(v) is str:
                resp[k] = {
                    'S': v
                }
            elif type(v) is int:
                resp[k] = {
                    'I': str(v)
                }
            elif type(v) is dict:
                resp[k] = {
                    'M': dict_to_item(v)
                }
            elif type(v) is list:
                resp[k] = []
                for i in v:
                    resp[k].append(dict_to_item(i))
                    
        return resp
    elif type(raw) is str:
        return {
            'S': raw
        }
    elif type(raw) is int:
        return {
            'I': str(raw)
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
