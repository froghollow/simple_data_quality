## dq_common_2309.py -- Common code module for Data Quality
#  (by RAMyers, daab.bssd.ss.ddm@fiscal.treasury.gov)
#  aws s3 cp dq_common_2309.py s3://{env-prefix}-code/common/dq_common_2309.py

import os
if 'AWS_DEFAULT_REGION' not in os.environ.keys():
    os.environ['AWS_DEFAULT_REGION'] = 'us-gov-west-1'
if 'SPARK_VERSION' not in os.environ.keys():
    os.environ["SPARK_VERSION"] = '3.3'

import awswrangler as wr
import pandas as pd

import datetime
import re
import boto3
import json
import pydeequ

from decimal import Decimal
#from pydeequ.profiles import *


class SimpleDQ: 
    ''' Data Quality functionality similar to boto3 Glue.Client '''

    glue_client = boto3.client('glue')
    ddb_resource = boto3.resource('dynamodb')
    
    # DynamoDb Tables for PyDeeQu data stores similar to Glue Data Quality API
    dqrulesets_table = ddb_resource.Table('dtl-prd-SMPL0-dqrulesets') 
    dqruleset_eval_runs_table = ddb_resource.Table('dtl-prd-SMPL0-dqruleset-eval-runs') 
    dqresults_table = ddb_resource.Table('dtl-prd-SMPL0-dqresults') 
    dqsuggestion_runs_table = ddb_resource.Table('dtl-prd-SMPL0-dqsuggestion_runs') 
    dqprofiles_table = ddb_resource.Table('dtl-prd-SMPL0-dqprofiles') 
    #dqsuggestions_table = ddb_resource.Table('dtl-prd-SMPL0-dqsuggestions') 
    
    mode='PyDeeQu'  

    def __init__(self, spark, mode='PyDeeQu', **kwargs) -> None:
        if mode == 'GlueDQ':
            # pass-thru wrapper for boto3 class Glue.Client *data_quality* methods
            print('Glue Data Quality implementation pending availability on Govcloud')
        self.mode = mode
        self.spark = spark
        
        job_name = spark.sparkContext._conf.get('spark.glue.JOB_NAME')
        if job_name:
            self.job_name = job_name
            self.job_run_id = spark.sparkContext._conf.get('spark.glue.JOB_RUN_ID')
        else:
            self.job_name = 'interactive'
            self.job_run_id = 'ad-hoc'

    # PyDeequ Interface functions
    def parse_dqdl_rule(self, rule_text): 
        """ Transform a DQDL-like rule from string to dict """
        import json
        import re

        s = rule_text.split(' ', 2)

        rule = {
            'Type' : s[0],
            'ColName' : '',
            'Expression' : '',
            'Lambda' : None,
            'Text' : rule_text
        }
        if '"' in s[1]:
            rule['ColName'] = s[1].replace('"','')
            if len(s) == 3:
                rule['Expression'] = s[2]
        else:
            rule['Expression'] = f"{s[1]} {s[2]}"

        # transform the Expression into a lambda assertion
        if rule['Expression'] == '':
            pass

        elif rule['Expression'].startswith('in'): 
            xpr = rule['Expression'].split() # no spaces between list values, please!
            inlist = xpr[1][1:-1].replace('"','').split(',')
            #rule['Lambda'] = lambda x: x in inlist
            rule['Lambda'] = inlist

        elif re.search("[<=>]", rule['Expression']):
            xpr = rule['Expression'].split()
            op =  xpr[0]
            val = float(xpr[1])
            if op == "=":
                rule['Lambda'] = lambda x: x == val
            elif op == ">":
                rule['Lambda'] = lambda x: x > val
            elif op == "<":
                rule['Lambda'] = lambda x: x < val
            elif op == ">=":
                rule['Lambda'] = lambda x: x >= val
            elif op == "<=":
                rule['Lambda'] = lambda x: x <= val

        elif rule['Expression'].startswith('between'):
            xpr = rule['Expression'].split()
            lo = xpr[1]
            hi = xpr[3]
            rule['Lambda'] = lambda x: lo < x < hi

        else:
            print("Can't Parse Expression")

        #print(json.dumps(rule, indent=2, default=str))

        return rule

    def run_pydeequ_checks(self, df, ruleset_name, rules_list ): 
        from pydeequ.checks import Check,CheckLevel
        from pydeequ.verification import VerificationSuite,VerificationResult
        import datetime
        
        print (f'Run PyDeeQu Check for Ruleset {ruleset_name}')
        
        beg_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        dq_result_id = f'dqresult-{beg_time}-{ruleset_name}'

        # create check object representing a set of constraints 
        check = Check(self.spark, CheckLevel.Error, ruleset_name)
        print (f'Check created for Ruleset {ruleset_name}')

        parsed_rules = []
        for rule_text in rules_list:
            rule = self.parse_dqdl_rule(rule_text)
            rule['status'] = 'processed'

            if rule['Type'] == 'HasSize':
                check.hasSize( rule['Lambda'] )
            elif rule['Type'] == 'HasMin':
                check.hasMin( rule['ColName'], rule['Lambda'] )
            elif rule['Type'] == 'IsComplete':
                check.isComplete( rule['ColName'] )        
            elif rule['Type'] == 'IsUnique':
                check.isUnique( rule['ColName'] )        
            elif rule['Type'] == 'IsContainedIn':  
                check.isContainedIn(rule['ColName'], rule['Lambda'])
            elif rule['Type'] == 'IsNonNegative':
                check.isNonNegative( rule['ColName'] )
            else:
                rule['status'] = 'skipped'
                msg = f"Skipping Check -- Rule Type '{rule['Type']}' is not implemented."
                print( msg )
            parsed_rules.append( rule )

        # apply constraints to data frame
        checkResult = VerificationSuite(self.spark).onData(df).addCheck(check).run()

        # get DeeQu results and customize to resemble Glue Data Quality API
        check_results = checkResult.checkResults # dict
        j=-1
        rule_results = []

        for i in range(len(parsed_rules)):
            #print(i,j)
            rule_result = {
                "Name" : f"Rule_{i}",
                "Description" : parsed_rules[i]["Text"]
            }
            if parsed_rules[i]['status'] == 'processed':
                j += 1
                rule_result.update ( {
                    "EvaluationMessage" : check_results[j]["constraint_message"],
                    "EvaluatedMetrics" : {
                        "Constraint" : check_results[j]["constraint"]
                    }                        
                } )
                if check_results[j]['constraint_status'] == 'Success':
                    rule_result['Result'] = 'PASS'
                elif check_results[j]['constraint_status'] == 'Failure':
                    rule_result['Result'] = 'FAIL'
                else:
                    rule_result['Result'] = 'ERROR'

            elif parsed_rules[i]['status'] == 'skipped':
                rule_result['Result'] = 'SKIP'
                rule_result.update ( {
                    "EvaluationMessage" : f"Rule Type '{parsed_rules[i]['Type']}' is not implemented.",
                    "EvaluatedMetrics" : {},
                    "Result" : rule_result['Result']
                } )   

            rule_results.append( rule_result )

            end_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        df_dq_result = pd.DataFrame.from_dict(rule_results, orient='columns')

        tally = { 
            'RULES' : len(df_dq_result),
            'PASS' : 0,
            'FAIL' : 0 }
        ls = list(df_dq_result['Result'])
        x = set(ls)
        for item in x:
            tally.update( {item : ls.count(item)} )

        print(tally)
        score = tally['PASS'] / ( tally['PASS'] + tally['FAIL'] )
        #tally, score

        result = {
            "ResultId" : dq_result_id,
            "Score" : score ,
            "Tally" : tally ,
            "DataSource" : "DataFrame",
            "RulesetName" : ruleset_name,
            "StartedOn" : beg_time,
            "CompletedOn" : end_time,
            "RulesetEvaluationRunId": "unknown",
            "RuleResults": rule_results
        }

        return result # dict


    def get_dataframe_from_datasource(self, DataSource ):

        # Three options for DataSource ...
        if 'GlueTable' in DataSource.keys(): # GlueDQ (like Glue Data Quality API)
            table = DataSource['GlueTable']
            s3_url = wr.catalog.get_table_location( database=table['DatabaseName'], table=table['TableName'])
            s3_url = s3_url + '/*/*'
            print (f"DataSource from GlueTable '{table['DatabaseName']}.{table['TableName']}'")

        elif 'S3Url' in DataSource.keys():   # DataSource Alt #1 (custom)
            s3_url = DataSource['S3Url']
            # table = DataSource['TableName'] # ToDo substring of S3Url
            print (f"DataSource from S3 Url '{s3_url}'")
        '''    
        elif 'SQL' in DataSource.keys():  # ToDo DataSource Alt #2 (custom)  
            s3_url = None
            sql = DataSource['Athena']['SQL']
            dbname = DataSource['Athena']['DataBase']

            # ToDo read Athena and/or Redshift (Spectrum) into Spark df
            df_pd = wr.athena.read_sql_query(sql, database=dbname)
            print('Convert Pandas to Spark')
            df = spark.createDataFrame(df_pd) 
        '''    
        if s3_url:
            if self.spark.sparkContext._conf.get('fs.s3a.endpoint'):
                s3_url = s3_url.replace( 's3://', 's3a://')
            try:
                df = self.spark.read.parquet( s3_url )
            except Exception as error:
                print( error )
                df = None

        return df


    def get_tablename_from_datasource(self, DataSource ):
        if 'GlueTable' in DataSource.keys(): # GlueDQ (like Glue Data Quality API)
            tablename = DataSource['GlueTable']['TableName']

        elif 'S3Url' in DataSource.keys():   # DataSource Alt #1 (custom)
            s3_url = DataSource['S3Url']
            tablename = s3_url.split('/')[4] # ToDo improve
        '''    
        elif 'SQL' in DataSource.keys():  # ToDo DataSource Alt #2 (custom)  
        '''    
        return tablename

    
    def run_pydeequ_suggestions(self, df ): 
        from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT

        constraint_suggestions = ConstraintSuggestionRunner(self.spark) \
                 .onData(df) \
                 .addConstraintRule(DEFAULT()) \
                 .run()
        
        constraint_suggestions = constraint_suggestions["constraint_suggestions"]
        constraint_suggestions = sorted(constraint_suggestions, key = lambda d: d['column_name'])
        
        # transform pydeequ code into DQDL rule 
        for item in constraint_suggestions:
            x = item["code_for_constraint"]
            if item['suggesting_rule'].startswith('FractionalCategoricalRangeRule'):
                x = x[:x.find(', lambda')]
            elif item['suggesting_rule'].startswith('RetainCompletenessRule'):
                y = x.split(', ')
                x = f"{y[0]} {y[1].replace('lambda x: x ','')}"            
            for y in re.findall('\[.*?\]', x): 
                x = x.replace( y, y.replace(', ', ','))
            x = re.sub(r'[()]', ' ', (x[1].upper() + x[2:])).replace( ', ', ' in ').strip() 
            item.update( {
                'dqdl_rule' : x
            })

        return constraint_suggestions
    
    def run_pydeequ_profile( self, df):
        from pydeequ.profiles import ColumnProfilerRunner

        result = ColumnProfilerRunner(self.spark) \
            .onData(df) \
            .run()
        
        dataset_profile = []
        for col, profile in result.profiles.items():
            s = str(profile).split(': ',2)
            profile_type = s[0]
            profile_type[: profile_type.find('Prof')]
            colprofile = {
                'column_name' : col ,
                'profile_type' : profile_type[: profile_type.find('Prof')],
            }
            #colinfo = json.loads(json.dumps(col), parse_float=Decimal)
            colprofile.update ( json.loads( s[2], parse_float=Decimal ) ) 
            dataset_profile.append( colprofile )

        dataset_profile = sorted(dataset_profile, key = lambda d: d['column_name'])
        
        return dataset_profile
    
    # Glue Data Quality wrapper functions
    def batch_get_data_quality_result(self, **kwargs): pass # ToDo 
    def cancel_data_quality_rule_recommendation_run(self, **kwargs): pass # ToDo 
    def cancel_data_quality_ruleset_evaluation_run(self, **kwargs): pass # ToDo

    def create_data_quality_ruleset( self, **kwargs):
        if type(kwargs['Ruleset']) == list:
            ", ".join(kwargs['Ruleset'])

        if self.mode == "GlueDQ":
            response = self.glue_client.create_data_quality_ruleset(
                Name = kwargs['Name'],       # str Reqd
                Ruleset = kwargs['Ruleset'], # str Reqd 
                Description = kwargs['Description'],
                Tags = kwargs['Tags'],       # dict
                TargetTable = kwargs['TargetTable'], # dict
                ClientToken = kwargs['ClientTokens']
            )
            return response   # { 'Name': 'string' }
        elif self.mode == "PyDeeQu":
            now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            kwargs.update( {
                'CreatedOn' : now,
                'LastModifiedOn' : now
            })
            self.dqrulesets_table.put_item(
                Item = kwargs
            )
            return { 'Name' : kwargs['Name'] }

    def delete_data_quality_ruleset(self, **kwargs): pass # ToDo

    def get_data_quality_profile(self, **kwargs): 
        print(f"Getting Data Quality Profile {kwargs['RunId']}")
        response = self.dqprofiles_table.get_item(
            Key = { 'RunId' : kwargs['RunId'] }
        )
        return response['Item']


    
    def get_data_quality_result(self, **kwargs): 
        print(f"Getting Result {kwargs['ResultId']}")
        response = self.dqresults_table.get_item(
            Key = { 'ResultId' : kwargs['ResultId'] }
        )
        return response['Item']
    
    def get_data_quality_rule_recommendation_run(self, **kwargs): 
        print(f"Getting Suggestion Run {kwargs['RunId']}")
        response = self.dqsuggestion_runs_table.get_item(
            Key = { 'RunId' : kwargs['RunId'] }
        )
        return response['Item']

    def get_data_quality_suggestions(self, **kwargs): 
        return self.get_data_quality_rule_recommendation_run(**kwargs)

    def get_data_quality_ruleset(self, **kwargs): 
        print(f"Getting Ruleset {kwargs['Name']}")
        if self.mode == "GlueDQ":
            print('Glue Data Quality implementation pending availability on Govcloud')
            '''response = self.glue_client.get_data_quality_ruleset(
                Name=kwargs['Name']
            )
            return response'''
        elif self.mode == "PyDeeQu":
            response = self.dqrulesets_table.get_item(
                Key = { 'Name' : kwargs['Name'] }
            )
            return response['Item']
       
    def get_data_quality_ruleset_evaluation_run(self, **kwargs): 
        print(f"Getting Eval Run {kwargs['RunId']}")
        if self.mode == "GlueDQ":
            print('Glue Data Quality implementation pending availability on Govcloud')

        elif self.mode == "PyDeeQu":
            response = self.dqruleset_eval_runs_table.get_item(
                Key = { 'RunId' : kwargs['RunId'] }
            )
            return response['Item']
    
    def list_data_quality_results(self, **kwargs): pass # ToDo
    def list_data_quality_rule_recommendation_runs(self, **kwargs): pass # ToDo
    def list_data_quality_ruleset_evaluation_runs(self, **kwargs): pass # ToDo

    def list_data_quality_rulesets(self, **kwargs): pass # ToDo

    def start_data_quality_profile_run(self, **kwargs):
        # profiles are not implemented in Glue Data Quality
        beg_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        dqprofileid = f'dqprofile-{beg_time}-{self.job_name}'

        process_parms = kwargs            
        process_parms.update ({
            'RunId' : dqprofileid,
            'Status' : 'RUNNING',
            'StartedOn' : beg_time,
            'DqJobName' : self.job_name,
            'DqRunId' : self.job_run_id,
        })
        # Store in DynDb table
        self.dqprofiles_table.put_item( Item = process_parms )

        df = self.get_dataframe_from_datasource( process_parms['DataSource'] )

        if df != None:
            dq_profiles = self.run_pydeequ_profile( df )

            process_parms.update ({
                'ColumnProfiles' : dq_profiles, 
                'Status' : 'SUCCEEDED',
                'CompletedOn' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            })
        else:
            process_parms.update ({
                'Status' : 'NODATA',
                'StatusMsg' : 'Empty Dataframe',
                'CompletedOn' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            })
        
        # Store in DynDb table
        self.dqprofiles_table.put_item( Item = process_parms )
            
        return { 'RunId': process_parms['RunId'] }

    def start_data_quality_rule_recommendation_run(self, **kwargs):
        if self.mode == "GlueDQ":
            print('Glue Data Quality implementation pending availability on Govcloud')

        elif self.mode == "PyDeeQu":

            beg_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            dqrunid = f'dqrecrun-{beg_time}-{self.job_name}'
            
            process_parms = kwargs            
            process_parms.update ({
                'RunId' : dqrunid,
                'Status' : 'RUNNING',
                'StartedOn' : beg_time,
                'DqJobName' : self.job_name,
                'DqRunId' : self.job_run_id,
            })
            # Store in DynDb table
            self.dqsuggestion_runs_table.put_item( Item = process_parms )
            
            df = self.get_dataframe_from_datasource( process_parms['DataSource'] )

            if df != None:
                dq_suggestions = self.run_pydeequ_suggestions( df )
                process_parms.update ({
                    'ConstraintSuggestions' : dq_suggestions, 
                    'Status' : 'SUCCEEDED',
                    'CompletedOn' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                })
            else:
                process_parms.update ({
                    'Status' : 'NODATA',
                    'StatusMsg' : 'Empty Dataframe',
                    'CompletedOn' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                })

            # Store in DynDb table
            self.dqsuggestion_runs_table.put_item( Item = process_parms )
            
            return { 'RunId': process_parms['RunId'] }
    
    def start_data_quality_ruleset_evaluation_run(self, **kwargs): 
        import datetime

        if self.mode == "GlueDQ":
            print('Glue Data Quality implementation pending availability on Govcloud')

        elif self.mode == "PyDeeQu":

            beg_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            dqrunid = f'dqrun-{beg_time}-{self.job_name}'
            
            process_parms = kwargs
            process_parms.update ({
                'RunId' : dqrunid,
                'ResultIds' : [],
                'Status' : 'RUNNING',
                'StartedOn' : beg_time,
                'DqJobName' : self.job_name,
                'DqRunId' : self.job_run_id                
            })
            
            # Store in DynDb table
            self.dqruleset_eval_runs_table.put_item( Item = process_parms )
            
            df = self.get_dataframe_from_datasource( process_parms['DataSource'] )
                
            if df != None:
                for ruleset_name in process_parms['RulesetNames']:
                    ruleset = self.get_data_quality_ruleset( Name = ruleset_name ) 
                    rules_list = ruleset['Ruleset'].replace('\n','').split(', ')

                    dq_result = self.run_pydeequ_checks( df, ruleset_name, rules_list ) 
                    dq_result.update( {
                        'DataSource' : process_parms['DataSource'],
                        'RulesetEvaluationRunId' : process_parms['RunId']
                    })

                    #import json
                    #from decimal import Decimal
                    dq_result = json.loads(json.dumps(dq_result), parse_float=Decimal)
                    self.dqresults_table.put_item(
                        Item = dq_result
                    )

                    process_parms['ResultIds'].append( dq_result['ResultId'] )

                process_parms.update ({
                    'Status' : 'SUCCEEDED',
                    'CompletedOn' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                })
            else:
                process_parms.update ({
                    'Status' : 'NODATA',
                    'StatusMsg' : 'Empty Dataframe',
                    'CompletedOn' : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                })
                
            # Store in DynDb table
            self.dqruleset_eval_runs_table.put_item( Item = process_parms )
            
            return { 'RunId': process_parms['RunId'] }
    
    def update_data_quality_ruleset(self, **kwargs): 
        if self.mode == "GlueDQ":
            response = self.glue_client.update_data_quality_ruleset(
                Name = kwargs['Name'],
                Description = kwargs['Description'],
                Ruleset = kwargs['Ruleset']
            )
            return response
        elif self.mode == "PyDeeQu":
            # 'put_item' works for both create and update
            now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            kwargs.update( {
                'LastModifiedOn' : now
            })
            self.dqrulesets_table.put_item(
                Item = kwargs
            )
            return { 'Name' : kwargs['Name'] }


        
