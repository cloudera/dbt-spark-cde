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

# fmt: off
import datetime as dt
import dbt.exceptions
import io
import json
import random
import requests
import time
import traceback

from dbt.adapters.spark_cde.adaptertimer import AdapterTimer
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS
from requests_toolbelt.multipart.encoder import MultipartEncoder
from typing import Any

# fmt: on
logger = AdapterLogger("Spark")
adapter_timer = AdapterTimer()

DEFAULT_POLL_WAIT = 30  # time to sleep in seconds before re-fetching job status
DEFAULT_LOG_WAIT = 40  # time to wait in seconds for logs to be populated after job run
DEFAULT_CDE_JOB_TIMEOUT = (
    900  # max amount of time(in secs) to keep retrying for fetching job status
)
NUMBERS = DECIMALS + (int, float)


class CDEApiCursor:
    def __init__(self) -> None:
        self._schema = None
        self._rows = None
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
                    field["name"],
                    field["type"],
                    None,
                    None,
                    None,
                    None,
                    field["nullable"],
                )
                for field in self._schema
            ]
        return description

    def close(self) -> None:
        self._rows = None

    # TODO: kill the running job?

    @staticmethod
    def generate_job_name():
        time_ms = round(time.time() * 1000)
        job_name = (
            "dbt-job-" + repr(time_ms) + "-" + str(random.randint(0, 1000)).zfill(8)
        )
        return job_name

    def execute(self, sql: str, *parameters: Any) -> None:
        if len(parameters) > 0:
            sql = sql % parameters

        # TODO: handle parameterised sql

        # 0. generate a job name
        job_name = self.generate_job_name()
        logger.debug(
            "{}: Job created with id: {} for SQL statement:\n{}".format(
                job_name, job_name, sql
            )
        )

        # 1. create resource
        logger.debug("{}: Create resources: files".format(job_name))
        self._cde_connection.create_resource(job_name, "files")
        logger.debug("{}: Done create resource: files".format(job_name))

        # 2. upload the resources
        sql_resource = self._cde_api_helper.generate_sql_resource(job_name, sql)
        py_resource = self._cde_api_helper.get_python_wrapper_resource(sql_resource)
        logger.debug(
            "{}: Upload resource: SQL resource: {}".format(
                job_name, sql_resource["file_name"]
            )
        )
        self._cde_connection.upload_resource(job_name, sql_resource)
        logger.debug(
            "{}: Done upload resources: SQL resource: {}".format(
                job_name, sql_resource["file_name"]
            )
        )
        logger.debug(
            "{}: Upload resource: py resource: {}".format(
                job_name, py_resource["file_name"]
            )
        )
        self._cde_connection.upload_resource(job_name, py_resource)
        logger.debug(
            "{}: Done upload resource: py resource: {}".format(
                job_name, py_resource["file_name"]
            )
        )

        # 3. submit the job
        logger.debug("{}: Submit job".format(job_name))
        self._cde_connection.submit_job(job_name, job_name, sql_resource, py_resource)
        logger.debug("{}: Done submit job".format(job_name))

        # 4. run the job
        logger.debug("{}: Run job".format(job_name))
        job = self._cde_connection.run_job(job_name).json()
        logger.debug("{}: Done run job".format(job_name))

        # 5. wait for the result
        total_time_spent_in_get_job_status = 0
        logger.debug("{}: Get job status".format(job_name))
        job_status = self._cde_connection.get_job_run_status(job).json()
        logger.debug(
            "{}: Current Job status: {}".format(job_name, job_status["status"])
        )
        logger.debug("{}: Done get job status".format(job_name))

        while job_status["status"] != CDEApiConnection.JOB_STATUS["succeeded"]:
            logger.debug("{}: Sleep for {} seconds".format(job_name, DEFAULT_POLL_WAIT))
            total_time_spent_in_get_job_status += DEFAULT_POLL_WAIT
            time.sleep(DEFAULT_POLL_WAIT)
            logger.debug(
                "{}: Done sleep for {} seconds".format(job_name, DEFAULT_POLL_WAIT)
            )

            logger.debug("{}: Get job status".format(job_name))
            job_status = self._cde_connection.get_job_run_status(job).json()
            logger.debug(
                "{}: Current Job status: {}".format(job_name, job_status["status"])
            )
            logger.debug("{}: Done get job status".format(job_name))

            # throw exception and print to console for failed job.
            if job_status["status"] == CDEApiConnection.JOB_STATUS["failed"]:
                logger.debug("{}: Get job output".format(job_name))
                schema, rows, failed_job_output = self._cde_connection.get_job_output(
                    job_name, job
                )
                logger.debug("{}: Done get job output".format(job_name))
                logger.error(
                    "{}: Failed job details: {}".format(job_name, failed_job_output.text)
                )
                raise dbt.exceptions.raise_database_error(
                    "Error while executing query: " + repr(job_status)
                )
            # timeout to avoid resource starvation
            if total_time_spent_in_get_job_status >= DEFAULT_CDE_JOB_TIMEOUT:
                logger.error(
                    "{}: Failed getting job status in: {} seconds".format(
                        job_name, DEFAULT_CDE_JOB_TIMEOUT
                    )
                )
                raise dbt.exceptions.RPCTimeoutException(DEFAULT_CDE_JOB_TIMEOUT)

        # 6. fetch and populate the results
        logger.debug("{}: Get job output".format(job_name))
        schema, rows, success_job_output = self._cde_connection.get_job_output(job_name, job)
        logger.debug("{}: Done get job output".format(job_name))
        logger.debug("{}: Job output details: {}".format(job_name, success_job_output.text))
        self._rows = rows
        self._schema = schema

        # 7. cleanup resources
        logger.debug("{}: Delete job".format(job_name))
        self._cde_connection.delete_job(job_name)
        logger.debug("{}: Done delete job".format(job_name))
        logger.debug("{}: Delete resource".format(job_name))
        self._cde_connection.delete_resource(job_name)
        logger.debug("{}: Done delete resource".format(job_name))

    def get_spark_job_events(self, job_name, job):
        logger.debug("{}: Get spark job events".format(job_name))
        events, job_output = self._cde_connection.get_job_output(
            job_name, job, log_type="event"
        )
        logger.debug("{}: Done get spark job events".format(job_name))
        for r in events:
            if "Timestamp" in r:
                event_time_in_secs = r["Timestamp"] / 1000
            else:
                event_time_in_secs = r["time"] / 1000

            logger.debug(
                "{}: {:<40} {:<40}".format(
                    job_name,
                    r["Event"],
                    dt.datetime.utcfromtimestamp(event_time_in_secs)
                    .time()
                    .strftime("%H:%M:%S.%f"),
                )
            )

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

    @staticmethod
    def generate_sql_resource(job_name, sql):
        time_ms = round(time.time() * 1000)
        file_name = job_name + "-" + repr(time_ms) + ".sql"
        file_obj = io.StringIO(sql)
        return {"file_name": file_name, "file_obj": file_obj, "job_name": job_name}

    @staticmethod
    def get_python_wrapper_resource(sql_resource):
        time_ms = round(time.time() * 1000)
        file_name = sql_resource["job_name"] + "-" + repr(time_ms) + ".py"

        py_file = (
            "import pyspark\nfrom pyspark.sql import SparkSession\nspark=SparkSession.builder.appName('"
            + sql_resource["job_name"]
            + "').enableHiveSupport().getOrCreate()\n"
        )
        py_file += (
            "sql=open('/app/mount/"
            + sql_resource["file_name"]
            + "', 'r').read()\ndf = spark.sql(sql)\ndf.show(n=1000000,truncate=False)\n"
        )

        file_obj = io.StringIO(py_file)
        return {
            "file_name": file_name,
            "file_obj": file_obj,
            "job_name": sql_resource["job_name"],
        }


class CDEApiConnection:
    JOB_STATUS = {
        "starting": "starting",
        "running": "running",
        "succeeded": "succeeded",
        "failed": "failed",
    }

    def __init__(self, base_api_url, access_token, api_header) -> None:
        self.base_api_url = base_api_url
        self.access_token = access_token
        self.api_header = api_header

    def create_resource(self, resource_name, resource_type):
        params = {"hidden": False, "name": resource_name, "type": resource_type}
        res = requests.post(
            self.base_api_url + "resources",
            data=json.dumps(params),
            headers=self.api_header,
        )
        return res

    def delete_resource(self, resource_name):
        res = requests.delete(
            self.base_api_url + "resources" + "/" + resource_name,
            headers=self.api_header,
        )
        return res

    def upload_resource(self, resource_name, file_resource):
        file_put_url = (
            self.base_api_url
            + "resources"
            + "/"
            + resource_name
            + "/"
            + file_resource["file_name"]
        )

        encoded_file_data = MultipartEncoder(
            fields={
                "file": (
                    file_resource["file_name"],
                    file_resource["file_obj"],
                    "text/plain",
                )
            }
        )

        header = {
            "Authorization": "Bearer " + self.access_token,
            "Content-Type": encoded_file_data.content_type,
        }

        res = requests.put(file_put_url, data=encoded_file_data, headers=header)
        return res

    def submit_job(self, job_name, resource_name, sql_resource, py_resource):
        params = {
            "name": job_name,
            "mounts": [{"dirPrefix": "/", "resourceName": resource_name}],
            "type": "spark",
            "spark": {},
        }

        params["spark"]["file"] = py_resource["file_name"]
        params["spark"]["files"] = [sql_resource["file_name"]]
        params["spark"]["conf"] = {"spark.pyspark.python": "python3"}

        res = requests.post(
            self.base_api_url + "jobs", data=json.dumps(params), headers=self.api_header
        )

        return res

    def get_job_status(self, job_name):
        res = requests.get(
            self.base_api_url + "jobs" + "/" + job_name, headers=self.api_header
        )

        return res

    def get_job_run_status(self, job):
        res = requests.get(
            self.base_api_url + "job-runs" + "/" + repr(job["id"]),
            headers=self.api_header,
        )

        return res

    def get_job_log_types(self, job):
        res = requests.get(
            self.base_api_url + "job-runs" + "/" + repr(job["id"]) + "/log-types",
            headers=self.api_header,
        )

        return res

    def parse_query_result(self, res_lines):
        schema = []
        rows = []

        line_number = 0
        for line in res_lines:
            line_number += 1

            if line.strip().startswith("+-"):
                break

        if line_number == len(res_lines):
            return schema, rows

        # TODO: this following needs cleanup, this is assuming every column is a string
        schema = list(
            map(
                lambda x: {"name": x.strip(), "type": "string", "nullable": False},
                list(
                    filter(lambda x: x.strip() != "", res_lines[line_number].split("|"))
                ),
            )
        )

        if len(schema) == 0:
            return schema, rows

        rows = []
        for data_line in res_lines[line_number + 2 :]:
            data_line = data_line.strip()
            if data_line.startswith("+-"):
                break
            row = list(
                map(
                    lambda x: x.strip(),
                    list(filter(lambda x: x.strip() != "", data_line.split("|"))),
                )
            )
            rows.append(row)

        # extract datatypes based on data in first row (string, number or boolean)
        if len(rows) > 0:
            try:
                schema, rows = self.extract_datatypes(schema, rows)
            except Exception:
                logger.error(traceback.format_exc())

        return schema, rows

    @staticmethod
    def parse_event_result(res_lines):
        events = []

        for event_line in res_lines:
            if len(event_line.strip()):
                json_rec = json.loads(event_line)
                if "Timestamp" in json_rec or "time" in json_rec:
                    events.append(json_rec)

        return events

    def get_job_output(
        self, job_name, job, log_type="stdout"
    ):  # log_type can be "stdout", "stderr", "event"

        logger.debug("{}: Sleep for {} seconds".format(job_name, DEFAULT_LOG_WAIT))
        # Introducing a wait as job logs can take few secs to be populated after job completion.
        time.sleep(DEFAULT_LOG_WAIT)
        logger.debug("{}: Done sleep for {} seconds".format(job_name, DEFAULT_LOG_WAIT))
        req_url = self.base_api_url + "job-runs" + "/" + repr(job["id"]) + "/logs"
        params = {"type": "driver" + "/" + log_type, "follow": "true"}
        res = requests.get(req_url, params=params, headers=self.api_header)
        # parse the o/p for data
        res_lines = list(map(lambda x: x.strip(), res.text.split("\n")))
        if log_type == "stdout":
            schema, rows = self.parse_query_result(res_lines)
            return schema, rows, res
        elif log_type == "event":
            return self.parse_event_result(res_lines), res
        else:
            return res_lines, res

    # since CDE API output of job-runs/{id}/logs doesn't return schema type, but only the SQL output,
    # we need to infer the datatype of each column and update it in schema record. currently only number
    # and boolean type information is inferred and the rest is defaulted to string.
    @staticmethod
    def extract_datatypes(schema, rows):
        first_row = rows[0]

        # if we do not have full schema info, do not attempt to extract datatypes
        if len(schema) != len(first_row):
            return schema, rows

        # TODO: do we handle date types separately ?
        is_number = lambda x: x.isnumeric()  # check numeric type
        is_logical = (
            lambda x: x == "true" or x == "false" or x == "True" or x == "False"
        )  # check boolean type
        is_true = lambda x: x == "true" or x == "True"  # check if the value is true

        convert_number = lambda x: float(x)  # convert to number
        convert_logical = lambda x: is_true(x)  # convert to boolean

        # conversion map
        convert_map = {
            "number": convert_number,
            "boolean": convert_logical,
            "string": lambda x: x,
        }

        # convert a row entry based on column type mapping
        def convert_type(row, col_types):
            for idx in range(len(row)):
                col = row[idx]
                col_type = col_types[idx]
                row[idx] = convert_map[col_type](col)

        # extract type info based on column data
        def get_type(col_data):
            if is_number(col_data):
                return "number"
            elif is_logical(col_data):
                return "boolean"
            else:
                return "string"

        col_types = list(map(lambda x: get_type(x), first_row))

        # for each row apply the type conversion
        for row in rows:
            convert_type(row, col_types)

        # record the type info into schema dict
        n_cols = len(col_types)
        for idx in range(n_cols):
            schema[idx]["type"] = col_types[idx]

        return schema, rows

    def delete_job(self, job_name):
        res = requests.delete(
            self.base_api_url + "jobs" + "/" + job_name, headers=self.api_header
        )

        return res

    def run_job(self, job_name):
        spec = {}

        res = requests.post(
            self.base_api_url + "jobs" + "/" + job_name + "/" + "run",
            data=json.dumps(spec),
            headers=self.api_header,
        )

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

    def get_base_auth_url(self):
        return self.base_auth_url

    def get_base_api_url(self):
        return self.base_api_url

    def get_auth_end_point(self):
        return self.get_base_auth_url() + "gateway/authtkn/knoxtoken/api/v1/token"

    def connect(self, user_name, password, base_auth_url, base_api_url):
        self.base_auth_url = base_auth_url
        self.base_api_url = base_api_url
        self.user_name = user_name
        self.password = password

        auth_endpoint = self.get_auth_end_point()
        auth = requests.auth.HTTPBasicAuth(self.user_name, self.password)

        res = requests.get(auth_endpoint, auth=auth)

        self.access_token = res.json()["access_token"]
        self.api_headers = {
            "Authorization": "Bearer " + self.access_token,
            "Content-Type": "application/json;charset=UTF-8",
            "accept": "text/plain; charset=utf-8",
        }

        connection = CDEApiConnection(
            self.base_api_url, self.access_token, self.api_headers
        )

        return connection


class CDEApiSessionConnectionWrapper(object):
    """Connection wrapper for the CDE API sessoin connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    @staticmethod
    def cancel():
        logger.debug("NotImplemented: cancel")

    def close(self):
        if self._cursor:
            self._cursor.close()

    @staticmethod
    def rollback(*args, **kwargs):
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
