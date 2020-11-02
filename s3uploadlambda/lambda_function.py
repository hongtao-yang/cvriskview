import json
import urllib.parse
import boto3
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.client('dynamodb')
events = boto3.client('events')

risk_queue_table_name = os.environ['DM_RISK_QUEUE_TABLE_NAME'] # Ris queue table name
risk_timer_name = os.environ['RISK_TIMER_EVENT_NAME'] # Risk event rule name

def saveToQueue(tableName,s3url):
    """
    Save s3url to queue table
    """
    dynamo.put_item(TableName=tableName, 
        Item={
            'id':{'S':str(s3url)}
            })

def lambda_handler(event, context):
   
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    if '/cv/' not in key:
        logger.info(key + 'is not cv csv')
        return {'status':'not a CV csv'}
    try:
        s3url = 's3://' + bucket + '/' + key
        #save url to queue
        logger.info('saving ' + s3url + ' to queue table')
        saveToQueue(risk_queue_table_name,s3url)
        #enable timer event
        events.enable_rule(Name=risk_timer_name)
        logger.info('timer event ' + risk_timer_name + ' is enabled.')
        return {'status':'finished'}
    except Exception as e:
        logger.error('Got exception:' + str(e))
        raise e
