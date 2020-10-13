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

#Environment variables
d_tablename = os.environ['DM_RISK_TABLE_NAME'] #Risk table name
bucket = os.environ['S3_XFILESBUCKET']  #S3 bucket
glueJobName = os.environ['RISK_GLUE_JOB_NAME'] #Risk Glue job name

#talbe field
jobIDfield = 'runningJobID'

def getJobIDFromDB(tableName):
    """
    Get job id from tableName
    """
    response = dynamo.get_item(TableName=tableName, Key={'s3url':{'S':str(jobIDfield)}})
    try:
        response['Item']
    except Exception:#can't find in db
        return None
    else:
        return response['Item']['jobId']['S']

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
            

def launchGlueJob(JobName,bucket):
    """
    Launch CV Risk Glue job
    """
    try:
        response = client.start_job_run(JobName = glueJobName,
                Arguments = {'--cvUrl': 'url','--convertBucket':'True','--bucket':bucket  })
        return response['JobRunId']
    except Exception as e:
        logger.error('Launch Glue Job error:' + str(e))
        return None

def lambda_handler(event, context):

    #if job ID in DB, need to check its status
    jobId = getJobIDFromDB(d_tablename)
    
    try:
        if jobId:
            #get job run status by job id
            response = client.get_job_run(JobName=glueJobName,RunId=jobId)
            jobStatus = response['JobRun']['JobRunState']
            logger.info('Job run status:'+jobStatus)
        
            if jobStatus == ' STARTING' or jobStatus == 'RUNNING':
                return { 'Status' : jobStatus }  
        #launch new job
        logger.info('Start Glue Job: ' + glueJobName)
        jobId = launchGlueJob(glueJobName,bucket)
        logger.info('Glue Job Run ID: ' + jobId)
        
        #save jobid to db
        logger.info('Saving job ID to DB')
        saveJobIdToDB(d_tablename,jobId)

    except Exception as e:
        logger.error('Launch Glue Job error:' + str(e))
        return {'status':'exceptions'}

    return {'status':'finished'}
