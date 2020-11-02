import json
import boto3
import traceback
import logging
import os
import time
from botocore.exceptions import ClientError
#logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#aws client
client = boto3.client('glue')
dynamo = boto3.client('dynamodb')
events = boto3.client('events')

#Environment variables
risk_table_name = os.environ['DM_RISK_TABLE_NAME'] #Risk table name
risk_queue_table_name = os.environ['DM_RISK_QUEUE_TABLE_NAME'] # Ris queue table name
bucket = os.environ['S3_XFILESBUCKET']  #S3 bucket
glueJobName = os.environ['RISK_GLUE_JOB_NAME'] #Risk Glue job name
risk_timer_name = os.environ['RISK_TIMER_EVENT_NAME'] # Risk event rule name

#table field
jobIDfield = 'runningJobID'
allDonedFlag = 'alldoneflag'

def getDBItem(tableName,keyName,valueName):
    response = dynamo.get_item(TableName=tableName, Key={'s3url':{'S':str(keyName)}})
    try:
        response['Item']
    except Exception:#can't find in db
        return None
    else:
        return response['Item'][valueName]['S']

def getJobIDFromDB(tableName):
    """
    Get job id from tableName
    """
    return getDBItem(tableName,jobIDfield,'jobId')

def getAlldonefalgFromDB(tableName):
    """
    Get alldoneflag from tableName
    """
    return getDBItem(tableName,allDonedFlag,'result')

def saveJobIdToDB(tableName,jobId):
    """
    Save job Id to DB
    """
    timestamp = time.time()
    dynamo.put_item(TableName=tableName, 
        Item={
            's3url':{'S':str(jobIDfield)},
            'jobId':{'S':str(jobId)},
            'timestamp':{'N':str(timestamp)},
            'result':{'S':str('')}
            })


def hasQueueTableItem():
    """
    check cv queue table if it has item
    """
    response = dynamo.scan(TableName=risk_queue_table_name,Limit=1)
    return response['Count']

            

def launchGlueJob(JobName,bucket,isQueue = False):
    """
    Launch CV Risk Glue job, if bucket is not empty
    Glue job will convert all files in the bucket,
    if isQueue is True
    Glue will convert the files in queue table
    """
    convertBkt = 'True'
    if isQueue:
        convertBkt = 'False'
    try:
        response = client.start_job_run(JobName = glueJobName,
                Arguments = {'--risktable': risk_table_name,'--convertBucket':convertBkt,'--bucket':bucket  })
        return response['JobRunId']
    except Exception as e:
        logger.error('Launch Glue Job error:' + str(e))
        return None

def lambda_handler(event, context):

    #if job ID in DB, need to check its status
    jobId = getJobIDFromDB(risk_table_name)
    
    try:
        if jobId:
            #get job run status by job id
            response = client.get_job_run(JobName=glueJobName,RunId=jobId)
            jobStatus = response['JobRun']['JobRunState']
            logger.info('Job run status:'+jobStatus)
        
            if jobStatus == ' STARTING' or jobStatus == 'RUNNING':
                return { 'Status' : jobStatus }  
        #check alldoneflag, if it's not set, launch new job
        flag = getAlldonefalgFromDB(risk_table_name)
        if flag:
            #all cv were converted, check queue table
            #scaning queue table
            #if it has items, lanuch glue job to convert them by set bucket param as false
            #launch new job to convert files in queue table
            if hasQueueTableItem():
                logger.info('Start Queue Glue Job : ' + glueJobName)
                jobId = launchGlueJob(glueJobName,risk_queue_table_name,True)
                logger.info('Glue Job Run ID: ' + jobId)
            
                #save jobid to db
                logger.info('Saving job ID to DB')
                saveJobIdToDB(risk_table_name,jobId)
            else:
                #turn off the time event
                response = events.disable_rule(Name=risk_timer_name)
                logger.info('timer event was close')
        else:
            #launch new job to convert all bucket
            logger.info('Start Bucket Glue Job: ' + glueJobName)
            jobId = launchGlueJob(glueJobName,bucket)
            logger.info('Glue Job Run ID: ' + jobId)
            
            #save jobid to db
            logger.info('Saving job ID to DB')
            saveJobIdToDB(risk_table_name,jobId)

    except Exception as e:
        logger.error('Launch Glue Job error:' + str(e))
        return {'status':'exceptions'}

    return {'status':'finished'}
