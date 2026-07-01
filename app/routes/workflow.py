# Copyright © 2026 Bull® SAS. All rights reserved
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from flask import request
from flask_restx import Namespace, Resource
from models.workflow_parser import WorkflowParser
from dotenv import dotenv_values
from flask import current_app
import requests
import jwt
import json
from io import BytesIO
from base64 import b64encode, b64decode, decodebytes
from app.routes.utils import token_required, DATA_CHECK_IN, DATA_TRANSFORMATION, DATA_TRANSFORMATION_NEW, INSIGHTS_GENERATOR, DATA_CHECK_IN_FILE_METHOD, DATA_CHECK_IN_FTP_METHOD, DATA_CHECK_IN_API_METHOD
import os
from dotenv import load_dotenv
from minio import Minio
from jsoninja import Jsoninja
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import pickle
import uuid

# Load env vars
load_dotenv()


api = Namespace('Workflow',"APIs related to workflows orchestration")
wf_model=WorkflowParser(api)
run_wf_parser = WorkflowParser(api).run_workflow_expected_payload()
fetch_wf_parser = WorkflowParser(api).fetch_workflow_expected_payload()
wf_executions_parser = WorkflowParser(api).fetch_workflow_executions_expected_payload()
wf_executions_paginated_parser = WorkflowParser(api).fetch_workflow_executions_paginated_expected_payload()
simplified_run_wf_parser = WorkflowParser(api).simplified_run_workflow_expected_payload()
minio_url = os.getenv('MINIO_URL')
root_user = os.getenv('MINIO_ROOT_USER')
root_pass = os.getenv('MINIO_ROOT_PASSWORD')
pistis_bucket_name = os.getenv('MINIO_BUCKET_NAME')
client = Minio(minio_url, access_key=root_user, secret_key=root_pass, secure=False)
jsoninja = Jsoninja()

@api.route('/run')
class DataRun(Resource):

    @api.expect(wf_model.run_workflow_expected_payload())
    @api.marshal_with(wf_model.run_workflow())
    @token_required
    def post(self):
        "Run Workflow"
        
        current_app.logger.info(" ### Processing Job Configurator POST request received: WORKFLOW_RUN method.")
        
        # Getting credentials
        current_app.logger.info(" ### Getting credentials from .env file ")
        user = os.getenv('AIRFLOW_USER')
        pwd = os.getenv('AIRFLOW_PASS')
        token = b64encode(f"{user}:{pwd}".encode('utf-8')).decode("ascii")

        current_app.logger.info(" ### Processing Job Configurator POST request received: RUN method.")
        
        # Bearer token
        auth_token = request.headers.get('Authorization')
        access_token =  auth_token if auth_token is None else auth_token.split()[1]  
        current_app.logger.info(" ### Token received: " + str(access_token))

        args = run_wf_parser.parse_args()
        ds = args["dataset"]
       
        if (ds):
            dataset_content = ds.read()
            current_app.logger.info("File received: %s", ds.filename)

        workflow = json.loads(args["workflow"])

        # DS Name and Description
        dataset_name = args["dataset_name"]
        dataset_description = args["dataset_description"]   

        # Validate RDF (metadata) 
        #if not validate_json_ld(metadata):
        #    raise Exception("Metadata no validated: Missing required key @context or @type in JSON-LD data. ")

        #workflow = request.json.get("job_list")
        current_app.logger.info(" ### Dag Conf: " + str(workflow))
        deployment_url = os.getenv('AIRFLOW_URL')
        dag_id = os.getenv('WF_DAG_ID')
        
        # Calling to airflow REST API to run the workflow DAG
        current_app.logger.info(" ### Calling to airflow url to run the workflw DAG ")

        json_data = {"conf":{"workflow": workflow["job_lists"], "dataset_name": dataset_name, "dataset_description": dataset_description, "access_token": access_token }}

        if (ds):
            dataset_object_name = "dag_" + uuid.uuid4().hex + "_" + ds.filename
            client.put_object(pistis_bucket_name, dataset_object_name, data=BytesIO(dataset_content), length=len(dataset_content))
            dataset_minio_path = "s3://" + pistis_bucket_name + "/" + dataset_object_name
            current_app.logger.info(" ### Dataset uploaded to MinIO: " + dataset_minio_path)
            json_data['conf']['dataset'] = {"name": ds.filename, "minio_path": dataset_minio_path}

        current_app.logger.info(" ### JSON Data: " + str(json_data))

        response = requests.post(
            url=f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns",
            headers={
                "Authorization": f'Basic {token}',
                "Content-Type" : "application/json",
                "Accept": "application/json"
            },
            json = json_data 
        )

        current_app.logger.info(" ### Response Content: " + str(response.content))
        json_data = response.json()
        current_app.logger.info(" ### Airflor Response: " + str(json_data))

        # Initialze JSON workflow resukts 
        root_run_id = json_data['dag_run_id']
        wf_results = { "runId": root_run_id, "status": "executing", "catalogue_dataset_endpoint": "none" }
        wf_s3_endpoint =  "s3://" + pistis_bucket_name + "/" + root_run_id + ".json"
        persist_in_minio(wf_results, wf_s3_endpoint)
        
        return {"data":{"dag_id": json_data['dag_id'], "dag_run_id": json_data['dag_run_id']}, "message": f"workflow with run_dag_id= {json_data['dag_run_id']} started.", "status": json_data['state']}, 200    

@api.route('/fetchWorkflowRun')
class DataFetch(Resource):

    @api.expect(wf_model.fetch_workflow_expected_payload())
    # @token_required
    def post(self):
        "Feching Workflow Execution"
        
        current_app.logger.info(" ### Processing Job Configurator POST request received: FETCH_WORKFLOW_EXECUTION method.")
        args = fetch_wf_parser.parse_args()

        runId = args["runId"]
        object_name = runId + ".json"
        bucket_name = os.getenv('MINIO_BUCKET_NAME')
        current_app.logger.info(" ### Getting S3 Object with bucket = " + bucket_name + " and object_name = " + object_name)
        s3_object = client.get_object(bucket_name, object_name)
        
        return json.loads(s3_object.data.decode("utf-8")), 200

@api.route('/stopWorkflowRun')
class DataFetch(Resource):

    @api.expect(wf_model.fetch_workflow_expected_payload())
    # @token_required
    def delete(self):
        "Deleting Workflow Execution"
                
        current_app.logger.info(" ### Processing Job Configurator POST request received: STOP_WORKFLOW_EXECUTION method.")
        args = fetch_wf_parser.parse_args()
        dag_run_id = args["runId"]

        dag_id = os.getenv('WF_DAG_ID')

        # Getting credentials
        current_app.logger.info(" ### Getting credentials from .env file ")
        user = os.getenv('AIRFLOW_USER')
        pwd = os.getenv('AIRFLOW_PASS')
        token = b64encode(f"{user}:{pwd}".encode('utf-8')).decode("ascii")

        # Bearer token
        auth_token = request.headers.get('Authorization')
        access_token =  auth_token if auth_token is None else auth_token.split()[1]  
        current_app.logger.info(" ### Token received: " + str(access_token))

        deployment_url = os.getenv('AIRFLOW_URL')

        dag_ids_list = ["pistis_workflow_template", "pistis_periodic_workflow"]

        for did in dag_ids_list:

            # Call to airflow rest api for deleting dag run
            response = requests.delete(
                url=f"{deployment_url}/api/v1/dags/{did}/dagRuns/{dag_run_id}",
                headers={
                    "Authorization": f'Basic {token}',
                    "Content-Type" : "application/json",
                    "Accept": "application/json"
                }
            )

            json_data = response.json()
            
            try:
              json_data = response.json()
            except ValueError:
              current_app.logger.info("Content-Type is not JSON ...")

            current_app.logger.info(" ### Airflor Response: " + str(json_data))
        
        return json_data, 200      



# Function to safely parse date strings
def parse_date(date_str):
    if date_str is None:
        return None
    try:
        # Normalize 'Z' to '+00:00' for compatibility
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None

@api.route('/getWorkflowRunList')
class DagRunsFetch(Resource):

    @api.expect(wf_model.fetch_workflow_executions_expected_payload())
    # @token_required
    def post(self):
        "Feching Workflow Execution List"
        
        current_app.logger.info(" ### Processing Job Configurator POST request received: FETCH_WORKFLOW_EXECUTION_LIST method.")
        args = wf_executions_parser.parse_args()
        dag_id = args['workflow_id']

        # Getting credentials
        current_app.logger.info(" ### Getting credentials from .env file ")
        user = os.getenv('AIRFLOW_USER')
        pwd = os.getenv('AIRFLOW_PASS')
        token = b64encode(f"{user}:{pwd}".encode('utf-8')).decode("ascii")

        # Bearer token
        auth_token = request.headers.get('Authorization')
        access_token =  auth_token if auth_token is None else auth_token.split()[1]  
        current_app.logger.info(" ### Token received: " + str(access_token))

        deployment_url = os.getenv('AIRFLOW_URL')

        dag_ids_list = ["pistis_workflow_template", "pistis_periodic_workflow"]

        if dag_id not in dag_ids_list:
            dag_ids_list.append(dag_id)

        dag_runs_list = [] 
        for did in dag_ids_list:

            json_data = {
                "dag_ids": [ str(did) ],
                "order_by": "-end_date",
                "page_limit": 150
            }

            if (did.lower()=="pistis_periodic_workflow"):
                json_data["states"] = ["queued", "running"]

            response = requests.post(
                url=f"{deployment_url}/api/v1/dags/~/dagRuns/list",
                headers={
                    "Authorization": f'Basic {token}',
                    "Content-Type" : "application/json",
                    "Accept": "application/json"
                },
                json = json_data
            )

            json_rundags = response.json()
            current_app.logger.info(" ### Dags Run List got it for " + did + " ... ")
            if (json_rundags["dag_runs"]):
                current_app.logger.info(" ### Dags Run found:  " + str(len(json_rundags)))
                filtered_dags_run = [
                    item for item in json_rundags.get("dag_runs", [])
                    if "conf" in item
                    and "dataset_name" in item["conf"]
                    and any(
                        job.get("job_name") == "job1"
                        for job in item["conf"].get("raw_wf", [])
                    )
                    and (
                        not item["conf"].get("workflow") or
                        any(job.get("job_name", "") > "job1" for job in item["conf"]["workflow"])
                    )
                ]
                dag_runs_list.extend(filtered_dags_run)

        #sorted_data = sorted(dag_runs_list, key=lambda x: datetime.strptime(x["end_date"], "%Y-%m-%dT%H:%M:%S.%fZ" ), reverse=True)
        
        # Sort the list: None dates first, then descending by date
        #json_rundags_sorted = sorted(dag_runs_list, key=lambda x: (x["end_date"] != "1970-01-01T00:00:00.000Z", x["end_date"] is not None, parse_date(x["end_date"]) if x["end_date"] else datetime.min), reverse=True)
        json_rundags_sorted = sorted(dag_runs_list, key=lambda x: x["dag_run_id"], reverse=True)
        json_data = {"dag_runs": json_rundags_sorted[:200]}
        
        return json_data, 200    

@api.route('/getWorkflowRunListPaginated')
class DagRunsFetchPaginated(Resource):

    @api.expect(wf_model.fetch_workflow_executions_paginated_expected_payload())
    # @token_required
    def post(self):
        "Feching Workflow Execution List (DB version)"
        

        current_app.logger.info(" ### Processing Job Configurator POST request received: FETCH_WORKFLOW_EXECUTION_LIST method (DB version).")
        args = wf_executions_paginated_parser.parse_args()
        dag_id = args['workflow_id']
        limit = args['workflow_limit']
        offset = args['workflow_offset']
        order_by = "end_date"  # default order field
        order_dir = "DESC"  # default order direction

        if limit <= 0:
            return {"message": "Invalid limit, it must be positive"}, 400
        if offset < 0:
            return {"message": "Invalid offset, it must be positive"}, 400

        # Getting credentials
        current_app.logger.info(" ### Getting credentials from env")
        # DB connection details from env
        db_host = os.getenv('PG_HOST')
        db_port = os.getenv('PG_PORT')
        db_user = os.getenv('PG_USER')
        db_pass = os.getenv('PG_PASSWORD')
        db_name = os.getenv('PG_DATABASE')

        # Allow order_by param from request if present (optional)
        req_order_by = args.get('order_by') if 'order_by' in args else None
        if req_order_by in ["dag_id", "run_id", "state", "start_date"]:
            order_by = req_order_by

        dag_ids_list = ["pistis_workflow_template", "pistis_periodic_workflow"]
        if dag_id not in dag_ids_list:
            dag_ids_list.append(dag_id)

        

        # Compose SQL query
        sql = """
            SELECT
                dr.state,
                dr.run_id AS dag_run_id,
                dr.conf,
                drn.content AS note,
                dr.start_date,
                dr.end_date
            FROM dag_run dr
            LEFT JOIN dag_run_note drn
                ON drn.dag_run_id = dr.id
            WHERE (%(dag_ids_list)s IS NULL OR dr.dag_id = ANY(%(dag_ids_list)s))
            ORDER BY {} {}
            LIMIT %(limit)s OFFSET %(offset)s
        """.format(order_by, order_dir)

        current_app.logger.debug(" ### SQL Query: " + sql)

        params = {
            "dag_ids_list": dag_ids_list if dag_ids_list else None,
            "limit": limit,
            "offset": offset
        }

        sql2 = """
            SELECT count(*) as total
            FROM dag_run"""

        try:
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_pass,
                dbname=db_name
            )
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, params)
            rows = cur.fetchall()
            cur.execute(sql2, params)
            total = cur.fetchone()["total"]
            cur.close()
            conn.close()
        except Exception as e:
            current_app.logger.exception(" ### Error querying PostgreSQL: " + str(e))
            return {"message": "Error querying database", "details": str(e)}, 500

        
        # Convert to expected output format
        json_rundags = []
        for row in rows:
            conf_data = pickle.loads(row["conf"].tobytes())            
            # IMPORTANT: DECIDE WHAT DATASET NAME TO PROVIDE
            # 1) The filename of the uploaded dataset
            #dataset_name = conf_data.get("dataset", "none").get("name", "none")
            # 2) The dataset name given for the metadata in the persistence layer
            dataset_name = conf_data.get("dataset_name", "none")
            dataset_description = conf_data.get("dataset_description", "none")
            periodicity = conf_data.get("periodicity", "none")
            # remove dataset content to save memory?
            conf_data.get("dataset", {}).pop("content", None)
            conf_data.pop("access_token", None)
            
            #-----------light version of the response
            """
            row.pop("conf", None)
            json_rundags.append({
                "dag_run_id": row["run_id"],
                "state": row["state"],
                "conf": {"periodicity": periodicity, "dataset_name": dataset_name, "dataset_description": dataset_description},
                "note": row["note"],
                "start_date": row["start_date"].isoformat() if row["start_date"] else None,
                "end_date": row["end_date"].isoformat() if row["end_date"] else None,
                    })
            """
            #---------Full version of the response (but without the dataset content)

            row["conf"] = conf_data
            row = {
                k: (v.isoformat() if isinstance(v, datetime) else v) 
                for k, v in row.items()
            }
            json_rundags.append(row)


        current_app.logger.info(" ### Dags Run found:  " + str(len(json_rundags)))   

        json_rundags = [
            item for item in json_rundags
            if "conf" in item
            and "dataset_name" in item["conf"]
            and any(
                job.get("job_name") == "job1"
                for job in item["conf"].get("raw_wf", [])
            )
            and (
                not item["conf"].get("workflow") or
                any(job.get("job_name", "") > "job1" for job in item["conf"]["workflow"])
            )
        ]     

        # Sort by dag_run_id descending (if needed)
        json_rundags = sorted(json_rundags, key=lambda x: x["dag_run_id"], reverse=True)
        json_data = {"dag_runs": json_rundags, "total": total, "limit": limit, "offset": offset}
        return json_data, 200

@api.route('/simplifiedRun')
class DataSimpRun(Resource):

    @api.expect(wf_model.simplified_run_workflow_expected_payload())
    @api.marshal_with(wf_model.run_workflow())
    # @token_required
    def post(self):
        "Simplified Workflow Execution"
        
        current_app.logger.info(" ### Processing Job Configurator POST request received: SIMPLIFIED_WORKFLOW_EXECUTION method.")
        # Getting credentials
        current_app.logger.info(" ### Getting credentials from .env file ")
        user = os.getenv('AIRFLOW_USER')
        pwd = os.getenv('AIRFLOW_PASS')
        token = b64encode(f"{user}:{pwd}".encode('utf-8')).decode("ascii")

        current_app.logger.info(" ### Processing Job Configurator POST request received: RUN method.")
        
        # Bearer token
        auth_token = request.headers.get('Authorization')
        access_token =  auth_token if auth_token is None else auth_token.split()[1]  
        current_app.logger.info(" ### Token received: " + str(access_token))

        args = simplified_run_wf_parser.parse_args()

        current_app.logger.info(" ### ARGS: " + str(args.keys()))

        ds = args["dataset"]  # This is a FileStorage instance
        
        #current_app.logger.info(" ### Dataset: " + str(ds))
        
        if (ds):
            dataset_content = ds.read()
            current_app.logger.info("File received: %s", ds.filename)

        workflow = json.loads(args["workflow"])
        dagWorkflow = translateToDagFormat (workflow)
  
        # DS Name, Description and Execution Time, Encryption flag and Periodicity
        dataset_name = args["dataset_name"]
        dataset_description = args["dataset_description"] 
        scheduledTime: datetime = args["scheduled_execution_time"]
        encryption =  args["encrytion"]
        periodicity = args["periodicity"]
        dataset_category = args["dataset_category"]
        dataset_keywords = args["dataset_keywords"]

        # Validate RDF (metadata) 
        #if not validate_json_ld(metadata):
        #    raise Exception("Metadata no validated: Missing required key @context or @type in JSON-LD data. ")

        #workflow = request.json.get("job_list")
        current_app.logger.info(" ### Workflow Definiton: " + str(dagWorkflow))
        deployment_url = os.getenv('AIRFLOW_URL')
        dag_id = os.getenv('WF_DAG_ID')
        
        json_data = {"conf":{"workflow": dagWorkflow, "raw_wf": dagWorkflow, "dataset_name": dataset_name, "dataset_description": dataset_description, "access_token": access_token, "encryption": encryption, "periodicity": periodicity, "dataset_keywords": dataset_keywords, "dataset_category": dataset_category }}

        # Calling to airflow REST API to run the workflow DAG
        current_app.logger.info(" ### Calling to airflow url to run the workflw DAG with the following JSON: " + str(json_data))

        if (ds):
            dataset_object_name = "dag_" + uuid.uuid4().hex + "_" + ds.filename
            client.put_object(pistis_bucket_name, dataset_object_name, data=BytesIO(dataset_content), length=len(dataset_content))
            dataset_minio_path = "s3://" + pistis_bucket_name + "/" + dataset_object_name
            current_app.logger.info(" ### Dataset uploaded to MinIO: " + dataset_minio_path)
            json_data['conf']['dataset'] = {"name": ds.filename, "minio_path": dataset_minio_path}

        if (scheduledTime):
            json_data['logical_date'] = scheduledTime.isoformat() 

        response = requests.post(
            url=f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns",
            headers={
                "Authorization": f'Basic {token}',
                "Content-Type" : "application/json",
                "Accept": "application/json"
            },
            json=json_data
        )

        current_app.logger.info(" ### Response Content: " + str(response.content))
        json_data = response.json()
        current_app.logger.info(" ### Airflor Response: " + str(json_data))

        # Initialze JSON workflow resukts 
        root_run_id = json_data['dag_run_id']
        wf_results = { "runId": root_run_id, "status": "executing", "catalogue_dataset_endpoint": "none" }
        wf_s3_endpoint =  "s3://" + pistis_bucket_name + "/" + root_run_id + ".json"
        persist_in_minio(wf_results, wf_s3_endpoint)
        
        response_json = {"data":{"dag_id": json_data['dag_id'], "dag_run_id": json_data['dag_run_id']}, "message": f"workflow with run_dag_id= {json_data['dag_run_id']} started.", "status": json_data['state']}    
        current_app.logger.info(" ### SimplifiedRun Response: " + str(response_json))
        return response_json, 200   


def validate_json_ld(json_ld_data):
    """
    Validates JSON-LD data in a Python dictionary.

    Args:
        json_ld_data (dict): A dictionary containing the JSON-LD data.

    Returns:
        bool: True if the JSON-LD data is valid, False otherwise.
    """
    # Check if the required keys are present
    required_keys = ["@context", "@graph"]
    for key in required_keys:
        if key not in json_ld_data:
            current_app.logger.info(f"Error: Missing required key '{key}' in JSON-LD data.")
            return False

    # Additional validation logic can be added here

    # If all checks pass, return True
    return True 

def persist_in_minio(field_value, source):
        current_app.logger.info(" ### Persisting object in MINIO ... ")
        object_url = field_value
        if (type(field_value) is dict):
        
            s3_path = source[len("s3://"):]
            s3_list = s3_path.split('/')
            index = len(s3_list) - 2
            if (len(s3_list) > 0):
               bucket_name = s3_list[index]
               object_name = s3_list[index + 1]
            
            json_encode_data = json.dumps(field_value).encode('utf-8')

            # Put  data in the bucket
            result = client.put_object(bucket_name, object_name, data=BytesIO(json_encode_data), length=len(json_encode_data)) 
            object_url = "s3://" + bucket_name + "/" + result.object_name
            current_app.logger.info(" ### Object persisted in MINIO. URL: " + object_url)

        return object_url 


def applyReplacements (job, index):
   if (job['name'] == DATA_TRANSFORMATION_NEW):
       job['name'] = DATA_TRANSFORMATION
   template = getTemplateData(job['name'], job['method'])
   replacements = getReplacements(job, index)

   current_app.logger.info(" pistis_job_template#applyReplacements: TEMPLATE GENERATED = " + str(template)) 
   return jsoninja.replace(template, replacements)  

def getTemplateData(service_name, service_method):
    
    file_path = ""
    templates_path = os.path.join(current_app.root_path, current_app.template_folder)

    if (service_name == DATA_CHECK_IN): 
         if (service_method == DATA_CHECK_IN_FILE_METHOD):
            file_path = "data_check_in_template.json"
         elif (service_method == DATA_CHECK_IN_FTP_METHOD):
            file_path = "data_check_in_ftp_template.json"
         elif (service_method == DATA_CHECK_IN_API_METHOD):
            file_path = "data_check_in_api_template.json"    
    elif (service_name == DATA_TRANSFORMATION):
         file_path = "data_transformation_template.json"
    elif (service_name == INSIGHTS_GENERATOR):
         file_path = "insights_generator_template.json"

    #Read json file template     
    with open(templates_path + "/" + file_path, 'r') as file:
        data = json.load(file)
    return data 

def getReplacements(job, index):
    
    #replacements = {"job_id" : str(job['id'])}
    replacements = {"job_id" : str(index)}    

    if (job['id'] > 1):
           replacements['source_id'] = str(index-1)

    for param in job['params']:
       if (param['type'] == "json"):
           replacements[str(param['name'])] = str(param['value'])
          
         
    current_app.logger.info(" pistis_job_template#getReplacements: REPLACEMENTS GENERATED = " + str(replacements))     
    return replacements

def translateToDagFormat (workflow):
    
    current_app.logger.info(" pistis_job_template#translateToDagFormat: WORKFLOW = " + str(workflow))
    dagWf = []
    for index, job in enumerate(workflow):
        current_app.logger.info(" pistis_job_template#translateToDagFormat: JOB = " + str(job))
        dagWf.append(applyReplacements(json.loads(json.dumps(job)), index+1))
    return dagWf

def get_dag_id (periodicity):
    dag_id = os.getenv('WF_DAG_ID') 
    """ if (periodicity == "hourly"):
        dag_id = os.getenv('WF_DAG_ID_HOURLY')
    elif (periodicity == "daily"):
        dag_id = os.getenv('WF_DAG_ID_DAILY')
    elif (periodicity == "montly")
        dag_id = os.getenv('WF_DAG_ID_MONTHLY')  """

    return dag_id   