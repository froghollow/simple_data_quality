import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#from awsglue.dynamicframe import DynamicFrame
####

import os
import json
import zipfile
import gzip
import boto3
#from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from datetime import datetime, timedelta
import shutil
import re

# set environment variables
os.environ['AWS_DEFAULT_REGION'] = 'us-gov-west-1'
os.environ["SPARK_VERSION"] = '3.3'

#import awswrangler as wr
#import pandas as pd
#from pyspark.sql.functions import encode
#from pyspark.sql.types import StringType,BooleanType,DateType,TimestampType
#from awswrangler import _utils, exceptions

# import common python modules # <
if 'CodeBucket' not in os.environ.keys():
    os.environ['CodeBucket'] = '{env-prefix}-code'  

try:
    os.mkdir('/tmp/python')
except: 
    pass
sys.path.insert( 0, '/tmp/python') 

s3_client = boto3.client('s3')
for py in ["dq_common_2309.py", "batch_simple.py" ]:
    s3_client.download_file( os.environ['CodeBucket'], f"common/{py}", f"/tmp/python/{py}")
import batch_simple as bat
import dq_common_2309 as dq_comm

# >

# Initialize Job 
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ProcessParms"])
job.init(args["JOB_NAME"], args)

# Initialize spark
from pyspark import SparkConf
conf = (SparkConf()
        .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")        
       )

spark = glueContext.spark_session.builder.config( conf=conf ).getOrCreate()
'''
# uncomment to inspect spark configuration
spark_conf = spark.sparkContext._conf.getAll()
filename = 'glue_spark_conf.txt'
with open(filename, 'w') as outfile:
    outfile.write('\n'.join(str(i) for i in spark_conf))
s3_client.upload_file( filename, '{env-prefix}-datawork', f'rmyers07/{filename}')
'''
# Instantiate DQ 
dq = dq_comm.SimpleDQ(spark)

# Get Input passed in Job Parameters (from Step Function Exec)
event = json.loads( args['ProcessParms'] )
print("Received event: " + json.dumps(event))
parms = event['process_parms']

file_dt = event['BatchId'].split('.')[-1]
s3_bucket = parms['S3Datalake']['Bucket']
s3_folder = parms['S3Datalake']['Output']
partition = parms['GluePartition'].replace('Dyymmdd', file_dt)

# Get or Create Batch # <
batch = bat.get_batch( parms["BatchId"] )
batch_rec = batch["Batch"]
if batch_rec == {}:
    batch_rec = {
        "BatchId" : parms["BatchId"],
        "ObjectId" : parms["BatchId"],
        "ProcessParms" : parms
    }
    bat.put_batch(batch_rec)
    for object_id in event["NameList"]:
        bat.put_batch( {
            "BatchId" : parms["BatchId"],
            "ObjectId" : object_id }
        )
    batch = bat.get_batch( parms["BatchId"] )
# update any previous batch with current parms
if batch_rec["ProcessParms"] != event["process_parms"]:
    batch_rec["ProcessParms"].update( event["process_parms"])
    bat.put_batch(batch_rec)
# >

batch_id = batch_rec['BatchId']
#for tablename in event['NameList']:
for batch_item in batch['BatchObjects']:
    tablename = batch_item['ObjectId']    
    arg = {
        'DataSource': {'S3Url': f's3://{s3_bucket}/{s3_folder}/{tablename}/{partition}/'}
    }
    
    try:
        run_id = dq.start_data_quality_profile_run( **arg )
        status_msg = run_id['RunId']
        batch_item_status = "COMPLETED"
        
    except Exception as error:
        status_msg = f"{tablename}\n{str(error)}"
        batch_item_status = "ERROR"

    print(f"{batch_item_status}:  {status_msg}")
    batch_step_id = f"Step-{args['JOB_NAME'].split('-')[-1]}"
    batch_item.update ( {
        batch_step_id : {
            "GlueJobId" : args['JOB_RUN_ID'],
            "Status" : batch_item_status,
            "StatusMsg" : status_msg,
            "DataSource" : arg['DataSource'],
            "ProfileRunId" : run_id['RunId'],
            "SfnExecName" : parms['ExecName'],
            "TimeStamp" : str(datetime.now()),
        }
    } )
    resp = bat.put_batch(batch_item)


####
job.commit()