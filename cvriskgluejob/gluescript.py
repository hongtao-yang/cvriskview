# Copyright 2020 CaseWare International Inc. or its affiliates. All Rights Reserved.
# https://www.caseware.com/
#
# Sherlock
# Hongtao.yang@caseware.com 
# 2020-08-17
#

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField,StructType,StringType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import botocore
import time
import logging
import sys
import dateutil.parser


#Glue job and arguments code, commented for now
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','risktable','convertBucket','bucket'])
job.init(args['JOB_NAME'], args)


#print(args)
# Set up logging
logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s: %(levelname)s: %(message)s')
           
#logging.info('The job is launched to convert '+s3url+ ' at ' + bucket)

#
# Chunk of code is for Audit mapping -----------------------
#

class CVRiskMappingField:
    """
    CV - Risk mapping field class
    It contains field properties and value process
    function
    """    
    def __init__(self, name, desc,valueFun = None):
        """
        :param name - corresponding name
        :param desc - description of this field
        :param valueFun - customize function of field value 
        """
        self.name = name
        self.desc = desc
        self.valueFun = valueFun

    #return corresponding name of mapping id
    def getName(self):
        return self.name

    #return item description
    def getDesc(self):
        return self.desc
    

    def processValue(self,form,value,dataFrame):
        """
        Special logic of value function
        :parma form of the field
        :param value - of the field
        :param dataFrame - spark data frame
        """
        if self.valueFun is not None:
            return self.valueFun(form,value,dataFrame)
        else:
            return value



#
# Logic process functions for Audit

#process FAS and Assertions 
def processAFValue(value,df,bAssert = True):
    if value is None:
        return ""

    if not bAssert and value in ['111111111111','222222222222','333333333333','444444444444','555555555555',
                '666666666666','777777777777','888888888888','999999999999']:
        r = df.filter("(group_ = 'PERVASIVECAT' and id = 'PERVASIVECATNAME' and form = '" + value + "')").select(df.data).collect()
        if r:
            return r[0]['data']
    else:
        items = value.split('|')
        ret = ""
        for it in items:
            for s in it.split(','):
                if not bAssert and 'GRP' in s:
                    ret += df.filter("(group_ = 'FSA' and id = 'FSA_NAME' and form = '" + s + "')").select(df.data).collect()[0]['data'] + ','
                elif bAssert and  'AO' in s:
                    asrts = df.filter("(id = '" + s + "')").select(df.data).collect()[0]['data'].split('|')
                    ret += asrts[-2][3:] if asrts[-1] == 'Other' else asrts[-1] + ','
    
        return ','.join(list(filter(None,set(ret.split(','))))) if ret != '' else ''
    

#process Significant Risk Indicators
def processSRIValue(form,value,df):   
    if value:
        r =df.filter("(group_ = 'FRFTYPE' and id = 'FRFTYPENAME' and form = '" + value + "')").select(df.data).collect()
        if r:
            return r[0]['data']
    return value
    
#process Assertions
def processAssertValue(form,value,df):
    return processAFValue(value,df)

#process Financial Statement Areas
def processFsaValue(form,value,df):
    return processAFValue(value,df,False)

#return Yes or No if value is 1 or 0
def yesOrNo(form,value,df):
    if value is not None and value != '':
        return 'Yes' if value == '1' else 'No'
    return value

#return Yes,No or Some if value is 1 or 0 or 2
def yesNoOrSome(form,value,df):
    if value is not None and value != '':
        return 'Yes' if value == '1' else 'No' if value == '0' else 'Some'
    return value
#process Associated Controls
def processAssCtrl(form,value,df):
    return  _processCtrl('C_NAME','C2R',form,df)

#Associated Reportable Items
def processApiCtrl(form,value,df):
    return  _processCtrl('NAME','R2I',form,df)

#process control or prtitem
def _processCtrl(id,tp,form,df):
    if form:
        # Value of Linkage's MEMBER_B is Risk's form.
        r = df.filter("(id = 'MEMBER_B' and data = '" + form + "')").select(df.form).collect()
        if r:
            lf = r[0]['form'] # get Linkage form
            #check its RELATION_TYPE
            r = df.filter("(id = 'RELATION_TYPE' and data = '" + tp +"' and form = '" + lf + "')").select(df.form).collect()
            #if C2R is included
            if r:#getting Control or Rptitem form from MEMBER_A
                r = df.filter("(id = 'MEMBER_A' and form = '" + lf + "')").select(df.data).collect()
                if r:
                    cform = r[0]['data']# Control or Rptitem from
                    r = df.filter("(id = '"+ id +"' and form = '" + cform + "')").select(df.data).collect()
                    if r:
                        return r[0]['data'] # Control or Rptitem name
    return None



#date converter 
def dateConverter(form,value,df):
    if value is not None and value != '':   
        return dateutil.parser.parse(value).strftime("%Y-%m-%d")
    return value


#The Audit Risk group name in CV table
auditRiskGroupId = 'RISK'

auditRiskName = 'AuditINT'
#tAudit International risk ID mapping fields map
auditRiskMapping = {
    "r_id":CVRiskMappingField("R_IDENTIFIER","Identifier"),
    "r_name":CVRiskMappingField("R_NEWNAME","Risk Name"),
    "r_entity":CVRiskMappingField("R_ENTITY","Entities"),
    "r_dateId":CVRiskMappingField("ISMODDATE","Date Identified",dateConverter),
    "r_desc":CVRiskMappingField("R_NAME","Risk Description"),
    "r_wgw":CVRiskMappingField("R_WHATGOWRONG","What Can Go Wrong"),
    "r_fsa":CVRiskMappingField("R_FSA","Financial Statement Areas",processFsaValue),
    "r_assert":CVRiskMappingField("R_FSA","Assertions",processAssertValue),
    "r_bc":CVRiskMappingField("R_BC","Business Cycles"),
    "r_sri":CVRiskMappingField("R_FRF","Significant Risk Indicators",processSRIValue),
    "r_significant":CVRiskMappingField("R_SIGNIFICANT","Significant Risk",yesOrNo),
    "r_occur":CVRiskMappingField("R_LLHOCCUR","Likelihood to Occur"),
    "r_impact":CVRiskMappingField("R_DOLLARIMPACT","Monetary Impact"),
    "r_inherent":CVRiskMappingField("R_INHERENT","Inherent Risk"),
    "r_crisk":CVRiskMappingField("R_MANRESP","Control Risk"),
    "r_prmm":CVRiskMappingField("R_RMM","Potential RMM"),
    "r_rrisk":CVRiskMappingField("R_RRISK","Residual Risk"),
    "r_rmm":CVRiskMappingField("R_OVRISK","RMM"),
    "r_proc":CVRiskMappingField("R_REQSPEC","Procedures Other Than Substantive"),
    "r_mresponse":CVRiskMappingField("R_MRESPONSE","Management Response"),
    "r_treatment":CVRiskMappingField("R_MANRESPDD","Risk Treatment/Mitigation",yesNoOrSome),
    "r_aresponse":CVRiskMappingField("R_ARESPONSE","Audit Response"),
    "r_source":CVRiskMappingField("R_SOURCEREF","Source/Reference"),
    "r_addressed":CVRiskMappingField("R_DESTREF","Addressed"),
    "l_assocdctrl":CVRiskMappingField("MEMBER_A","Associated Controls",processAssCtrl),
    "l_assocditem":CVRiskMappingField("MEMBER_A","Associated Reportable Items",processApiCtrl),
    "r_audaddr":CVRiskMappingField("R_AUDRESPDD","Audit has properly addressed this risk",yesOrNo),
    "r_rforward":CVRiskMappingField("R_RFD","Roll forward",yesOrNo)
}

#---------------------------------------------------------------------------------------------------------------------------


#create spark 
spark = SparkSession.builder \
            .appName("xfiles-cv2risk") \
            .config('spark.sql.codegen.wholeStage', False) \
            .getOrCreate()
            

# AWS Athena 
athena = boto3.client('athena')
# AWS
dynamo = boto3.client('dynamodb')

#
# Chunk of code is for creating and querying Athena table -----------------------
#
#Athena table properties 
def get_table_prop(bucket, firmguid, table_name):
    location = 's3://%s/%s/%s/' % (bucket, firmguid, table_name)
    return '''ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES (
        'escapeChar'='\"',
        'separatorChar'=',')
      STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
      OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      LOCATION
        '%s'
      TBLPROPERTIES (
        'classification'='csv',
        'skip.header.line.count'='1',
        'typeOfData'='file')
    ''' % location

#Create cv risk table SQL
def get_query_create_cvriskview(bucket, firmguid):
    table_name = 'cvrisk'
    return '''CREATE EXTERNAL TABLE `%s`(
    `yearend` int,
    `entityid` int,
    `entityname` string,
    `bundleguid` string,    
    `filename` string,
    `lastmodified` string,
    `cwguid` string,
    `rid` string,
    `name` string,
    `rentity` string,
    `dateid` string,
    `desc` string,
    `whatwrong` string,
    `fsa` string,
    `assert` string,
    `bc` string,
    `sri` string,
    `significant` string,
    `likelihood` string,
    `m_impact` string,
    `inherent` string,
    `ctrl_r` string,
    `p_rmm` string,
    `residual` string,
    `rmm` string,
    `procedures` string,
    `m_response` string,
    `treatment` string,
    `a_response` string,
    `source` string,
    `addressed` string,
    `assocd_ctrl` string,
    `assocd_item` string,
    `aud_addressed` string,
    `r_forward` string )
      %s
    ''' % (table_name, get_table_prop(bucket, firmguid, table_name))


#Athena config 
ATHENA_OUTPUT_BUCKET = 'aws-athena-query-results-sherlock-api/'


#query Athena
def query_athena(query, db_name):
    """
    execute the specified query on the specified database
    :param query: the query string
    :param db_name: name of database to perform the query
    :param request_id: the requset id of this lambda execution
    """
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': db_name
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + ATHENA_OUTPUT_BUCKET,
        }
    )    

    query_execution_id = response['QueryExecutionId']

    for i in range(1, 10):
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            break
        if query_execution_status == 'FAILED':
            logging.error(' This query failed on database %s: %s' % ( db_name, query))
            raise Exception(query_status['QueryExecution']['Status']['StateChangeReason'])
        else:
            time.sleep(i)
    else:
        athena.stop_query_execution(QueryExecutionId=query_execution_id)
        logging.error(' This query timeout on database %s: %s' % ( db_name, query))
        raise Exception('Time out')

    return athena.get_query_results(QueryExecutionId=query_execution_id)

#create cv risk view table on Athena
def create_cv_risk(s3Bucket,firm_guid):
    db_name = "xfiles_" + firm_guid.replace('-', '')  # athena doesn't seem to like 'dash'.
    query = "CREATE DATABASE IF NOT EXISTS %s LOCATION 's3://%s' WITH DBPROPERTIES ('creator'='Sherlock')" % (
            db_name, s3Bucket)
    try:
        query = get_query_create_cvriskview(s3Bucket, firm_guid)
        query_athena(query, db_name)
    except AlreadyExistsException:
        logging.info('Table was created')
    except Exception as e:
        logging.error('query Athena got error:' + str(e))
    
#----------------------------------------------------------------------------------------------------------------------


#
# Chunk of code is for setting up which template needs to be converted -----------------------
#
#Set to convert the Audit International Template
#Risk Group ID
riskGroupId = auditRiskGroupId

#risk mapping table for Audit
riskMapping = auditRiskMapping

#setting template name 
riskName = auditRiskName

class RiskObject:    
    """
    risk objct class
    contains common columns and risk properties
    """
     # default constructor 
    def __init__(self): 
        self.filename = ""
        self.form = ""
        self.yearend = ""
        self.entityid = ""
        self.entityname = ""
        self.bundleguid = ""   
        self.lastmodified = ""
        self.cwguid = ""
        self.riskProperties = dict() # all risk properties 


class CVRiskConverter:    
    """
    CVRiskConverter class
    for converting CV table to CV Risk View
    """
    @staticmethod
    def checks3file(bucketName,objectKey):
        s3 = boto3.resource('s3')
        try:
            s3.Object(bucketName, objectKey).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise
        else:
            # The object does exist.
            return True
    #checking whether it is created by the template
    @staticmethod
    def isCreatedBy(metaUrl,templateName):
        #get s3 information
        s3bucket,firmGuid,objectKey = CVRiskConverter.getS3Info(metaUrl)
  
        #if file is generated, return it now
        if not CVRiskConverter.checks3file(s3bucket,objectKey):
            return False
            
        #Get metadata csv
        try:
            df = spark.read.csv(metaUrl, header=True, mode="DROPMALFORMED")
            #checking metadata to ensure the client file is created by the template
            #XF-875 If CWCustomProperty.CWAuditSource OR CWAuditSource exist (don't care about their value), then convert it.
            auditId = df.filter("propertyname == 'CWCustomProperty.CWAuditSource' or propertyname == 'CWAuditSource'").select(df.propertyname)

            if len(auditId.collect()) <= 0: 
                return False
            return True
        except Exception as e:
            logging.error('Spark read csv got error:' + str(e)) 
        return False
    
    #return bucket, firmGudi and object key
    @staticmethod
    def getS3Info(s3url):
        #CW s3url is s3://bucket/firmguid/table/filename.csv
        cvKeys = s3url.split('/')
        return cvKeys[2],cvKeys[3],cvKeys[3]+'/'+cvKeys[4]+'/'+cvKeys[5]
        
    #convert the Row list to risk list
    #param the cvCsvKey on the S3
    @staticmethod
    def convertToRisk(cvCsvkey, checkMetadata = False):
        logging.info('Staring converting '+cvCsvkey)        
        #get cvrisk object 
        riskKey = cvCsvkey.replace("cv","cvrisk")
        #get s3 information
        s3bucket,firmGuid,objectKey = CVRiskConverter.getS3Info(riskKey)
  
        #if file is generated, return it now
        if CVRiskConverter.checks3file(s3bucket,objectKey):
            logging.info('This file was already converted')
            return riskKey
            
        if checkMetadata:
            metaKey = cvCsvkey.replace("cv","metadata")
            if CVRiskConverter.isCreatedBy(metaKey,riskName) == False:
                logging.warning('The '+objectKey+' was not created by ' + riskName)
                return 'Was not created by ' + riskName

        logging.info('Loading CV csv file')
        formTable = dict()
        #Load cv.csv
        df = spark.read.csv(cvCsvkey, header=True, mode="DROPMALFORMED")

        #cvrisk = df.filter("(group_ == 'RISK' or group_ =='CONTROL') and id IS NOT NULL" ).select(df.form,df.id,df.data)
        cvrisk = df.filter("(group_ == '" + riskGroupId + "') and id IS NOT NULL" ).select(df.filename,df.yearend,df.entityid,df.entityname,df.bundleguid,df.lastmodified,df.cwguid,df.form,df.id,df.data)

        #iterate through the rows and save them into a hash table
        #key is the form, value is the RiskObject 
        for r in cvrisk.collect():
            if r["form"] not in formTable:
                formTable[r["form"]] = RiskObject()                  
            formTable[r["form"]].riskProperties[r["id"]] = r["data"]            
            formTable[r["form"]].form = r["form"]
            formTable[r["form"]].filename = r["filename"] 
            formTable[r["form"]].yearend = r["yearend"] 
            formTable[r["form"]].entityid = r["entityid"]
            formTable[r["form"]].entityname = r["entityname"]
            formTable[r["form"]].bundleguid = r["bundleguid"]
            formTable[r["form"]].cwguid = r["cwguid"]
            formTable[r["form"]].lastmodified = r["lastmodified"]    

        logging.info('Converting to risk view')
        #list for saving to csv
        riskData = []
        #process data to risk view
        for v in formTable.values():            
            riskData.append(Row(v.yearend,v.entityid,v.entityname,v.bundleguid,v.filename,v.lastmodified,v.cwguid,
            riskMapping["r_id"].processValue(v.form,v.riskProperties.get(riskMapping["r_id"].getName()),df), 
            riskMapping["r_name"].processValue(v.form,v.riskProperties.get(riskMapping["r_name"].getName()),df),
            riskMapping["r_entity"].processValue(v.form,v.riskProperties.get(riskMapping["r_entity"].getName()),df), 
            riskMapping["r_dateId"].processValue(v.form,v.riskProperties.get(riskMapping["r_dateId"].getName()),df),
            riskMapping["r_desc"].processValue(v.form,v.riskProperties.get(riskMapping["r_desc"].getName()),df), 
            riskMapping["r_wgw"].processValue(v.form,v.riskProperties.get(riskMapping["r_wgw"].getName()),df),
            riskMapping["r_fsa"].processValue(v.form,v.riskProperties.get(riskMapping["r_fsa"].getName()),df), 
            riskMapping["r_assert"].processValue(v.form,v.riskProperties.get(riskMapping["r_assert"].getName()),df),
            riskMapping["r_bc"].processValue(v.form,v.riskProperties.get(riskMapping["r_bc"].getName()),df), 
            riskMapping["r_sri"].processValue(v.form,v.riskProperties.get(riskMapping["r_sri"].getName()),df), 
            riskMapping["r_significant"].processValue(v.form,v.riskProperties.get(riskMapping["r_significant"].getName()),df),
            riskMapping["r_occur"].processValue(v.form,v.riskProperties.get(riskMapping["r_occur"].getName()),df), 
            riskMapping["r_impact"].processValue(v.form,v.riskProperties.get(riskMapping["r_impact"].getName()),df),
            riskMapping["r_inherent"].processValue(v.form,v.riskProperties.get(riskMapping["r_inherent"].getName()),df), 
            riskMapping["r_crisk"].processValue(v.form,v.riskProperties.get(riskMapping["r_crisk"].getName()),df),
            riskMapping["r_prmm"].processValue(v.form,v.riskProperties.get(riskMapping["r_prmm"].getName()),df), 
            riskMapping["r_rrisk"].processValue(v.form,v.riskProperties.get(riskMapping["r_rrisk"].getName()),df),
            riskMapping["r_rmm"].processValue(v.form,v.riskProperties.get(riskMapping["r_rmm"].getName()),df), 
            riskMapping["r_proc"].processValue(v.form,v.riskProperties.get(riskMapping["r_proc"].getName()),df),
            riskMapping["r_mresponse"].processValue(v.form,v.riskProperties.get(riskMapping["r_mresponse"].getName()),df), 
            riskMapping["r_treatment"].processValue(v.form,v.riskProperties.get(riskMapping["r_treatment"].getName()),df),
            riskMapping["r_aresponse"].processValue(v.form,v.riskProperties.get(riskMapping["r_aresponse"].getName()),df), 
            riskMapping["r_source"].processValue(v.form,v.riskProperties.get(riskMapping["r_source"].getName()),df),
            riskMapping["r_addressed"].processValue(v.form,v.riskProperties.get(riskMapping["r_addressed"].getName()),df), 
            riskMapping["l_assocdctrl"].processValue(v.form,'',df), 
            riskMapping["l_assocditem"].processValue(v.form,'',df),
            riskMapping["r_audaddr"].processValue(v.form,v.riskProperties.get(riskMapping["r_audaddr"].getName()),df), 
            riskMapping["r_rforward"].processValue(v.form,v.riskProperties.get(riskMapping["r_rforward"].getName()),df),          
            ))

        #CV risk view header schema
        riskSchema = StructType([
            StructField("yearend", StringType(),True),
            StructField("entityid", StringType(),True),
            StructField("entityname", StringType(),True),
            StructField("bundleguid", StringType(),True),
            StructField("filename", StringType(),True),
            StructField("lastmodified", StringType(),True),
            StructField("cwguid", StringType(),True),
            StructField("rid", StringType(),True),
            StructField("name", StringType(),True),
            StructField("rentity", StringType(),True),
            StructField("dateId", StringType(),True),
            StructField("desc", StringType(),True),
            StructField("whatwrong", StringType(),True),
            StructField("fsa", StringType(),True),
            StructField("assert", StringType(),True),
            StructField("bc", StringType(),True),
            StructField("sri", StringType(),True),
            StructField("significant", StringType(),True),
            StructField("likelihood", StringType(),True),
            StructField("m_impact", StringType(),True),
            StructField("inherent", StringType(),True),
            StructField("ctrl_r", StringType(),True),
            StructField("p_rmm", StringType(),True),
            StructField("residual", StringType(),True),
            StructField("rmm", StringType(),True),
            StructField("procedures", StringType(),True),
            StructField("m_response", StringType(),True),
            StructField("treatment", StringType(),True),
            StructField("a_response", StringType(),True),
            StructField("source", StringType(),True),        
            StructField("addressed", StringType(),True),
            StructField("assocd_ctrl", StringType(),True),
            StructField("assocd_item", StringType(),True),             
            StructField("aud_addressed", StringType(),True),
            StructField("r_forward", StringType(),True)
        ])
        #create risk df
        risk_df = spark.createDataFrame(riskData,riskSchema)
        logging.info('Saving to Risk csv')
        risk_df.toPandas().to_csv(riskKey, header=True,index=False)
       
        logging.info('Creating cvrisk table on Athena for firm:'+firmGuid)
        try:
            create_cv_risk(s3bucket,firmGuid)
        except Exception as e:
            logging.error('create Athena table got error:' + str(e)) 
            
        logging.info(cvCsvkey + ' was converted.')  
        return riskKey

#-----------------------------------------------------------------------------------------------


def getS3Keys(bucket, prefix='', suffix='', keyWord = '/cv/'):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    :param keyWord: Only fetch keys that contains keyWord (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    s3client = boto3.client('s3')
    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix) and keyWord in key:
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def saveToDB(tableName,s3url,s3bucket,result):
    timestamp = time.time()
    dynamo.put_item(TableName=tableName, 
        Item={
            's3url':{'S':str(s3url)},
            'bucket':{'S':str(s3bucket)},
            'timestamp':{'N':str(timestamp)},
            'result':{'S':str(result)}
            })
            
def checkDbItemExist(tableName,s3url):
    #check if key is converted
    response = dynamo.get_item(TableName=tableName, Key={'s3url':{'S':str(s3url)}})
    try:
        response['Item']
    except Exception:#can't find usr in db
        return False
    else:
        return True

#cvRsikQueue = 'cvriskqueue'
def getQueueTableItem(riskQueueName):
    """
    check cv queue table if it has item
    """
    response = dynamo.scan(TableName=riskQueueName)
    return response['Items']

def deleteQueueTableItem(riskQueueName,s3url):
    """
    delete item by id
    """
    try:
        response = dynamo.delete_item(
            Key={'id': {'S': s3url,}},TableName=riskQueueName)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logging.error('Delete queue table failed:' + e.response['Error']['Message'])
    


#parameters
risk_table_name = args['risktable']
bucketName = args['bucket']
bBucket = args['convertBucket'] == 'True'


#Convert CV csv to CV Risk csv on S3
if bBucket:
    logging.info('Starting job to convert bucket')
    # converting all cv.csvs in the bucket
    for key in getS3Keys(bucketName, '', '.csv'):
        s3url = 's3://'+ bucketName + '/'  + key
        #check if key is converted
        if checkDbItemExist(risk_table_name,s3url):
            logging.info(s3url + ' was converted')
        else:
            ret = CVRiskConverter.convertToRisk(s3url,True)
             #save it to db
            saveToDB(risk_table_name,s3url,bucketName,ret)
    #All CV were converted, set finished flag
    saveToDB(risk_table_name,'alldoneflag',bucketName,'done')
        
else:
    logging.info('Starting job to convert queue table')
    queueTable = bucketName # queue table name is saved in bucketName
    #convert the files in queue table
    items = getQueueTableItem(queueTable)
    for it in items:
        s3url = it['id']['S']
        ret = CVRiskConverter.convertToRisk(s3url,True)
        #remove it from queue table
        deleteQueueTableItem(queueTable,s3url)

logging.info('Job finished')
job.commit()