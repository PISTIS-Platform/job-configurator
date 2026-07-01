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

from flask_restx import fields, inputs
from werkzeug.datastructures import FileStorage
from werkzeug.datastructures import ImmutableDict
import json
import datetime


class WorkflowParser:
    def __init__(self, namespace):
        self.namespace=namespace      
    
    def simplified_run_workflow_expected_payload(self):
        run_wf_parser = self.namespace.parser()  #  api.parser()
        run_wf_parser.add_argument(
            "dataset", location="files", type=FileStorage, required=False
        )
        
        run_wf_parser.add_argument(
            "workflow", required=True, location="form", type=str, help=""" Json Schema = {"workflow":{"type": "array","items": {"type": "object", "properties": {"name": {"type": "string"},"id": {"type": "number"},"params": {"type": "array","items": {"type": "object","properties": {"name": {"type": "string"},"type": {"type": "string"},"value": {"type": "string"}},"required": ["name", "type", "value"]}}},"required": ["name","id","params"]}}}  """ 
        )

        run_wf_parser.add_argument(
            "dataset_name", required=False, location="form", type=str, help=""" Dataset Name""", default=""
        )

        run_wf_parser.add_argument(
            "dataset_description", required=False, location="form", type=str, help=""" Dataset Description""", default=""
        )

        run_wf_parser.add_argument(
            "scheduled_execution_time", required=False, location="form", type=inputs.datetime_from_iso8601, help=""" Scheduled Execution Time""", default=""
        )

        run_wf_parser.add_argument(
            "encrytion", required=False, location="form", type=str, help=""" Encryption Flag""", default="false"
        )

        run_wf_parser.add_argument(
            "gdpr_checker", required=False, location="form", type=str, help=""" GDPR Flag""", default="false"
        )

        run_wf_parser.add_argument(
            "periodicity", required=False, location="form", type=str, help=""" Periodicity """, default=""
        )

        run_wf_parser.add_argument(
            "dataset_category", required=False, location="form", type=str, help=""" Dataset Category """, default=""
        )

        run_wf_parser.add_argument(
            "dataset_keywords", required=False, location="form", type=str, help=""" Dataset Keywords """, default=""
        )

        return run_wf_parser
    
    def run_workflow_expected_payload(self):
        run_wf_parser = self.namespace.parser()  #  api.parser()
        run_wf_parser.add_argument(
            "dataset", location="files", type=FileStorage, required=False
        )
        
        run_wf_parser.add_argument(
            "workflow", required=True, location="form", type=str, help=""" Json Schema = {"workflow":{"type":"array","minItems":0,"items":{"type":"object","properties":{"prev_run":{"type":"string"},"job_name":{"type":"string"},"source":{"type":"string","pattern":"^(?:https?://|ftp://|none|job|workflow)"},"endpoint":{"type":"string","format":"uri","pattern":"^(https?|wss?|ftp)://"},"input_data":{"type":"array","minItems":0,"items":{"type":"object","properties":{"name":{"type":"string"},"value":{"type":"string"}},"required":["name","value"]}},"method":{"type":"string","enum":["get","post","put","delete"]},"destination_type":{"type":"string","enum":["memory","factory_storage","nifi"]}},"required":["source","endpoint","input_data","method","destination_type"]}}}  """ 
        )

        run_wf_parser.add_argument(
            "dataset_name", required=False, location="form", type=str, help=""" Dataset Name""", default=""
        )

        run_wf_parser.add_argument(
            "dataset_description", required=False, location="form", type=str, help=""" Dataset Description""", default=""
        )

        return run_wf_parser
        """ data_model={
            "job_list": fields.List(
                fields.Nested(self._a_job())
            )
        }
        return self.namespace.model("run_workflow_expected_payload", data_model) """


    def fetch_workflow_expected_payload(self):
        run_wf_parser = self.namespace.parser()  #  api.parser()
        
        run_wf_parser.add_argument(
            "runId", required=False, location="form", type=str, help=""" Workflow Run Id """, default=""
        )

        return run_wf_parser
    
    def fetch_workflow_executions_expected_payload(self):
        run_wf_parser = self.namespace.parser()  #  api.parser()
        
        run_wf_parser.add_argument(
            "workflow_id", required=True, location="args", type=str, help=""" Workflow Run Id """, default=""
        )

        return run_wf_parser

    
    def fetch_workflow_executions_paginated_expected_payload(self):
        run_wf_parser = self.namespace.parser()  #  api.parser()
        
        run_wf_parser.add_argument(
            "workflow_id", required=True, location="args", type=str, help=""" Workflow Run Id """, default=""
        )
        run_wf_parser.add_argument(
            "workflow_limit", required=False, location="args", type=int, help=""" Workflow Runs amount """, default=30
        )
        run_wf_parser.add_argument(
            "workflow_offset", required=False, location="args", type=int, help=""" Workflow Runs offset """, default=0
        )

        return run_wf_parser

    def run_workflow(self):
        data_model={
            "status":fields.String(),
            "message":fields.String(),
            "data": fields.Nested(self._a_run_data()) 
        }
        return self.namespace.model("run_workflow", data_model)
    
    def _a_job(self):    
        data_model={
            "job_name": fields.String(),
            "source":fields.String(pattern="^(?:https?://|ftp://|none|job|workflow)$"),
            "metadata": fields.String(pattern="^(?:https?://|ftp://|none|job|workflow)$"),
            "endpoint":fields.String(pattern="^(https?|wss?|ftp)://", format="uri"),
            "input_data": fields.List(
                fields.Nested(self._a_job_param())
            ),
            "method": fields.String(enum=["get", "post", "put", "delete"]),
            "destination_type": fields.String(enum=["memory", "factory_storage", "nifi"]),
            "response_dataset_field_path": fields.String(),
            "response_metadata_field_path": fields.String(),
            "lineage_tracker": fields.Boolean()
        }
        return self.namespace.model('a_job_model',data_model)
    
    def _a_job_param(self):
        data_model={
            #"type":fields.String(enum=["web", "body"]),
            "name": fields.String(), 
            "value":fields.String()
        }
        return self.namespace.model('a_job_param', data_model)
    
    # def _a_job_mapping(self):
    #     data_model={
    #         "target_field":fields.String(),
    #         "source_field":fields.String()
    #     }
    #     return self.namespace.model('a_job_mapping', data_model)
    
    def _a_run_data(self):
        data_model={
            "dag_id":fields.String(),
            "dag_run_id":fields.String()
        }
        return self.namespace.model('a_run_data_model', data_model)