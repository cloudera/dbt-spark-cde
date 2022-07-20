# Copyright 2022 Cloudera Inc.
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

"""CDE API session integration."""

# API based CDE access
from urllib.parse import urlencode
import requests
import json
import io
import time
import datetime as dt
from types import TracebackType
from typing import Any
from requests_toolbelt.multipart.encoder import MultipartEncoder

import dbt.exceptions
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS

logger = AdapterLogger("Spark")

DEFAULT_POLL_WAIT = 2 # seconds
DEFAULT_LOG_WAIT = 10 # seconds
DEFAULT_RETRIES = 10 # max number of retries for fetching log
MIN_LINES_TO_PARSE = 3 # minimum lines in the logs file before we can start to parse the sql output 
NUMBERS = DECIMALS + (int, float)

class CDEApiCursor:
    def __init__(self) -> None:
        self._cde_connection = None
        self._cde_api_helper = None

    def __init__(self, cde_connection) -> None:
        self._cde_connection = cde_connection

        self._cde_api_helper = CDEApiHelper()

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc_val,
        exc_tb,
    ) -> bool:
        self._cde_connection.close()
        return True

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        if self._schema is None:
            description = list()
        else:
            description = [
                (
                    field['name'],
                    field['type'], # field['dataType'],
                    None,
                    None,
                    None,
                    None,
                    field['nullable'],
                )
                for field in self._schema
            ]
        return description

    def close(self) -> None:
        self._rows = None

        # TODO: kill the running job?

    def _generateJobName(self):
        time_ms = round(time.time()*1000)
        job_name = "cde_api_session_job" + "-" + repr(time_ms)
        return job_name

    def execute(self, sql: str, *parameters: Any) -> None:
        if len(parameters) > 0:
            sql = sql % parameters
        
        # TODO: handle parameterised sql
        # print("Job execute", sql)

        # 0. generate a job name
        job_name = self._generateJobName()

        # 1. create resource
        self._cde_connection.deleteResource(job_name)
        self._cde_connection.createResource(job_name, "files")

        sql_resource = self._cde_api_helper.generateSQLResource(job_name, sql)
        py_resource  = self._cde_api_helper.getPythonWrapperResource(sql_resource)

        # 2. upload the resource
        self._cde_connection.uploadResource(job_name, sql_resource)
        self._cde_connection.uploadResource(job_name, py_resource)

        # 2. submit the job
        self._cde_connection.deleteJob(job_name)
        self._cde_connection.submitJob(job_name, job_name, sql_resource, py_resource)
        job = self._cde_connection.runJob(job_name).json()
        self._cde_connection.getJobStatus(job_name)
    
        # 3. run the job
        job_status = self._cde_connection.getJobRunStatus(job).json()
        
        # 4. wait for the result       
        while job_status["status"] != CDEApiConnection.JOB_STATUS['succeeded']:
            time.sleep(DEFAULT_POLL_WAIT)
            job_status = self._cde_connection.getJobRunStatus(job).json()

            if (job_status["status"] == CDEApiConnection.JOB_STATUS['failed']):
                print("Job Failed", sql, job_status)
                raise dbt.exceptions.raise_database_error(
                        'Error while executing query: ' + repr(job_status)
                    ) 

        # 5. fetch and populate the results 
        time.sleep(DEFAULT_LOG_WAIT) # wait before trying to access the log - so as to ensure that the logs are written to the bucket
        # print("execute - sql", job, sql)
        # log_types = self._cde_connection.getJobLogTypes(job)
        # print("execute - log_types", log_types)
        schema, rows = self._cde_connection.getJobOutput(job)

        self._rows = rows
        self._schema = schema 

        # 6. cleanup resources
        self._cde_connection.deleteResource(job_name)
        self._cde_connection.deleteJob(job_name)
        
    def fetchall(self):
        return self._rows

    def fetchone(self):
        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row

class CDEApiHelper:
    def __init__(self) -> None:
        pass

    def generateSQLResource(self, job_name, sql):
        time_ms = round(time.time()*1000)
        file_name = job_name + "-" + repr(time_ms) + ".sql"
        file_obj = io.StringIO(sql)
        return { "file_name": file_name, "file_obj": file_obj, "job_name": job_name }

    def getPythonWrapperResource(self, sql_resource):
        time_ms = round(time.time()*1000)
        file_name = sql_resource['job_name'] + "-" + repr(time_ms) + ".py"

        py_file = "import pyspark\nfrom pyspark.sql import SparkSession\nspark=SparkSession.builder.appName('" + sql_resource['job_name'] + "').enableHiveSupport().getOrCreate()\n"
        py_file += "sql=open('/app/mount/" + sql_resource['file_name'] + "', 'r').read()\ndf = spark.sql(sql)\ndf.show(n=1000000,truncate=False)\n"

        # print(py_file)

        file_obj = io.StringIO(py_file)
        
        return { "file_name": file_name, "file_obj": file_obj, "job_name": sql_resource['job_name'] }

class CDEApiConnection:
    
    JOB_STATUS = { 'starting': "starting", 'running': "running", 'succeeded': "succeeded", 'failed': "failed" }

    def __init__(self, base_api_url, access_token, api_header) -> None:
        self.base_api_url = base_api_url
        self.access_token = access_token
        self.api_header   = api_header

    def getJobRunList(self):
        params = {'latestjob': False, 'limit': 20, 'offset': 0, 'orderby': 'ID', 'orderasc': True}
        res = requests.get(self.base_api_url + "job-runs", params=params, headers=self.api_header)
        return res

    def getResources(self):
        params = {'includeFiles': False, 'limit': 20, 'offset': 0, 'orderby': 'name', 'orderasc': True}
        res = requests.get(self.base_api_url + "resources", params=params, headers=self.api_header)
        # print("getResources", res.text)
        return res

    def createResource(self, resource_name, resource_type):
        params = {'hidden': False, 'name': resource_name, 'type': resource_type}
        # print('createResource - params', params)
        res = requests.post(self.base_api_url + "resources", data=json.dumps(params), headers=self.api_header)
        # print("createResource", res.text)
        return res

    def deleteResource(self, resource_name):
        res = requests.delete(self.base_api_url + "resources" + "/" + resource_name, headers=self.api_header)
        # print("deleteResource", res.text)
        return res

    def uploadResource(self, resource_name, file_resource):
        file_put_url = self.base_api_url + "resources" + "/" + resource_name + "/" + file_resource['file_name']
        
        encoded_file_data = MultipartEncoder(
            fields={
                'file': (file_resource['file_name'], file_resource['file_obj'], 'text/plain')
            }
        )

        # print("uploadResource - encoded", encoded_file_data, encoded_file_data.content_type)
        
        header = {'Authorization': "Bearer " + self.access_token, 'Content-Type': encoded_file_data.content_type}
        
        res = requests.put(file_put_url, data=encoded_file_data, headers=header)
        # print("uploadResource", res.text)
        return res

    def submitJob(self, job_name, resource_name, sql_resource, py_resource):
        params = {}

        params["name"] = job_name

        params["mounts"] = [{ "dirPrefix": "/", "resourceName": resource_name }]

        params["type"] = "spark"
       
        params["spark"] = {}

        params["spark"]["file"] = py_resource['file_name']
        params["spark"]["files"] = [sql_resource['file_name']]
        
        params["spark"]["conf"] = { "spark.pyspark.python": "python3" }

        # print("submitJob - params", params)
        # print("submitJob - auth header", self.api_header)

        res = requests.post(self.base_api_url + "jobs", data=json.dumps(params), headers=self.api_header)

        # print('submitJob - res', res)
        # print('submitJob - res.text', res.text)
        
        return res

    def getJobStatus(self, job_name):
        res = requests.get(self.base_api_url + "jobs" + "/" + job_name, headers=self.api_header)

        # print("getJobStatus - res", res)
        # print("getJobStatus - res.text", res.text)

        return res

    def getJobRunStatus(self, job):
        res = requests.get(self.base_api_url + "job-runs" + "/" + repr(job["id"]), headers=self.api_header)

        # print("getJobRunStatus - res", res)
        # print("getJobRunStatus - res.text", res.text)

        return res

    def getJobLogTypes(self, job):
        res = requests.get(self.base_api_url + "job-runs" + "/" + repr(job["id"]) + "/log-types", headers=self.api_header)

        # print("getJobLogTypes - res", res)
        # print("getJobLogTypes - res.text", res.text)

        return res

    def getJobOutput(self, job):
        res_lines = []
        schema = []
        rows = []

        no_of_retries = 0

        while len(res_lines) <= MIN_LINES_TO_PARSE:  # ensure that enough lines are present to start parsing
            req_url = self.base_api_url + "job-runs" + "/" + repr(job["id"]) + "/logs?type=driver%2Fstdout&follow=true"
            res = requests.get(req_url, headers=self.api_header)

            # print("getJobOutput - url", req_url)
            # print("getJobOutput - job", job)
            # print("getJobOutput - res", res)
            # print("getJobOutput - res.text", res.text)

            schema = []
            rows = []

            # parse the o/p for data
            res_lines = list(map(lambda x: x.strip(), res.text.split("\n")))

            # print("getJobOutput - lines", res_lines, len(res_lines))

            n_lines = len(res_lines)
            if (n_lines > MIN_LINES_TO_PARSE): break  # we have some o/p to process, break out - what happens for CREATE stm?

            no_of_retries += 1
            # if (len(res_lines) <= MIN_LINES_TO_PARSE):
            #     print("getJobOutput - retrying ", no_of_retries, " of ", DEFAULT_RETRIES, " (", job, ")")

            if (no_of_retries > DEFAULT_RETRIES): 
                # print("getJobOutput - exceeded retries")
                break

            time.sleep(DEFAULT_LOG_WAIT)
            
        line_number = 0
        for line in res_lines:
            line_number += 1

            # print("parse o/p", line_number, line)

            if (line.strip().startswith("+-")):
                break

        if (line_number == len(res_lines)): return schema, rows

        # TODO: this following needs cleanup, this is assuming every column is a string
        schema = list(map(lambda x: {"name": x.strip(), "type": "string", "nullable": False}, list(filter(lambda x: x.strip() != "", res_lines[line_number].split("|")))))
        # print("getJobOutput - schema ", schema)

        if (len(schema) == 0): return schema, rows

        rows = []
        for data_line in res_lines[line_number+2:]:
            data_line = data_line.strip()
            # print("parse o/p", data_line)
            if (data_line.startswith("+-")): break
            row = list(map(lambda x: x.strip(), list(filter(lambda x: x.strip() != "", data_line.split("|")))))
            # print("parsed row", row)
            rows.append(row)
        # print("getJobOutput - rows ", rows)

        # extract datatypes based on data in first row (string, number or boolean)
        if (len(rows) > 0):
            try:
                schema, rows = self.extractDatatypes(schema, rows)
            except Exception as e:
                import traceback
                print(traceback.format_exc())

        return schema, rows

    # since CDE API output of job-runs/{id}/logs doesn't return schema type, but only the SQL output, 
    # we need to infer the datatype of each column and update it in schema record. currently only number 
    # and bolean type information is inferred and the rest is defaulted to string.
    def extractDatatypes(self, schema, rows):
        first_row = rows[0]
        
        # if we do not have full schema info, do not attempt to extract datatypes
        if (len(schema) != len(first_row)):
            return schema, rows

        # TODO: do we handle date types separately ?

        is_number = lambda x: x.isnumeric()  # check numeric type
        is_logical = lambda x: x == "true" or x == "false" or x == "True" or x == "False" # check boolean type
        is_true = lambda x: x == "true" or x == "True" # check if the value is true
        
        convert_number = lambda x: float(x) # convert to number
        convert_logical = lambda x: is_true(x) # convert to boolean

        # conversion map
        convert_map = { "number": convert_number, "boolean": convert_logical, "string": lambda x: x } 

        # convert a row entry based on column type mapping
        def convertType(row, col_types):
            for idx in range(len(row)):
                col = row[idx]
                col_type = col_types[idx]
                row[idx] = convert_map[col_type](col)
        
        # extact type info based on column data
        def getType(col_data):
            if (is_number(col_data)): return "number"
            elif (is_logical(col_data)): return "boolean"
            else: return "string"  

        col_types = list(map(lambda x: getType(x), first_row))

        # for each row apply the type conversion 
        for row in rows:
            convertType(row, col_types)    

        # record the type info into schema dict
        n_cols = len(col_types)
        for idx in range(n_cols):
            schema[idx]["type"] = col_types[idx]

        return schema, rows

    def deleteJob(self, job_name):
        res = requests.delete(self.base_api_url + "jobs" + "/" + job_name, headers=self.api_header)
        
        # print("deleteJob - res", res)
        # print("deleteJob - res.text", res.text)

        return res

    def runJob(self, job_name):
        spec = {}

        res = requests.post(self.base_api_url + "jobs" + "/" + job_name + "/" + "run", data=json.dumps(spec), headers=self.api_header)

        # print("runJob - res", res)
        # print("runJob - res.text", res.text)

        return res

    def cursor(self):
        return CDEApiCursor(self)

class CDEApiConnectionManager:

    def __init__(self) -> None:
        self.base_auth_url = ""
        self.base_api_url = ""
        self.user_name = ""
        self.password = ""
        self.access_token = ""
        self.api_headers = {}

    def getBaseAuthURL(self):
        return self.base_auth_url

    def getBaseAPIURL(self): 
        return self.base_api_url

    def getAuthEndpoint(self):
        return self.getBaseAuthURL() + "gateway/authtkn/knoxtoken/api/v1/token"

    def connect(self, user_name, password, base_auth_url, base_api_url):
        self.base_auth_url = base_auth_url
        self.base_api_url = base_api_url
        self.user_name = user_name
        self.password = password

        auth_endpoint = self.getAuthEndpoint()
        auth = requests.auth.HTTPBasicAuth(self.user_name, self.password)

        # print("auth_endpoint", auth_endpoint)
        # print("auth", auth)

        res = requests.get(auth_endpoint, auth=auth)
        # print("res", res)

        self.access_token = res.json()['access_token']
        self.api_headers = {'Authorization': "Bearer " + self.access_token, 'Content-Type': 'application/json'}

        # print("access_token", self.access_token)

        connection = CDEApiConnection(self.base_api_url, self.access_token, self.api_headers)

        return connection

class CDEApiSessionConnectionWrapper(object):
    """Connection wrapper for the CDE API sessoin connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        logger.debug("NotImplemented: cancel")

    def close(self):
        if self._cursor:
            self._cursor.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        else:
            return f"'{value}'"

