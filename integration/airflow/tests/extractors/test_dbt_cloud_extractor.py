# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import logging
import unittest
import uuid
import pytz
import json
from datetime import datetime

# import mock
from unittest.mock import patch
from airflow.models import TaskInstance, DAG
from airflow.utils import timezone
from airflow.utils.state import State
from openlineage.airflow.utils import try_import_from_string
from openlineage.airflow.extractors.dbt_cloud_extractor import DbtCloudExtractor

log = logging.getLogger(__name__)


DbtCloudRunJobOperator = try_import_from_string(
    "airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator"
)


def get_dbt_artifact(*args, **kwargs):
    json_file = None
    if kwargs['path'].endswith("catalog.json"):
        json_file = "tests/extractors/dbt_data/catalog.json"
    elif kwargs['path'].endswith("manifest.json"):
        json_file = "tests/extractors/dbt_data/manifest.json"
    elif kwargs['path'].endswith("run_results.json"):
        json_file = "tests/extractors/dbt_data/run_results.json"

    if json_file is not None:
        return MockResponse(read_file_json(json_file))
    return None


def read_file_json(file):
    f = open(
        file=file,
        mode="r"
    )
    json_data = json.loads(f.read())
    f.close()
    return json_data


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data


class TestDbtCloudExtractorE2E(unittest.TestCase):

    @patch("airflow.models.TaskInstance")
    @patch("openlineage.airflow.listener.run_data_holder")
    @patch("airflow.hooks.base.BaseHook")
    @patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook")
    def test_extract(self, dbt_cloud_hook, base_hook, run_data_holder, task_mock):
        log.info("test_extractor")

        base_hook.get_connection.return_value.login = 117664
        dbt_cloud_hook.get_project.return_value.json.return_value = {
            "data": {
                "connection": {
                    "type": "snowflake"
                }
            }
        }
        dbt_cloud_hook.get_job.return_value.json.return_value = {
            "data": {
                "project_id": 177370,
                "job": {
                    "account_id": 117664,
                    "execute_steps": [
                        "dbt run --select my_first_dbt_model"
                    ]
                }
            }
        }
        dbt_cloud_hook.get_job_run.return_value.json.return_value = {
            "data": {
                "run_steps": [
                    {
                        "name": "dbt run --select my_first_dbt_model",
                        "index": 4
                    }
                ]
            }
        }
        dbt_cloud_hook.get_job_run_artifact.side_effect = get_dbt_artifact
        run_data_holder.get_active_run.return_value.run_id = "f8e07aae-d70f-40b1-bc45-5612be0e7295"
        task_mock.xcom_pull.return_value = "b3a814a3-ab93-414c-a5b1-6bfc74576356"

        execution_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        dag = DAG(dag_id='TestDBTCloudExtractor')
        dag.create_dagrun(
            run_id=str(uuid.uuid4()),
            state=State.QUEUED,
            execution_date=execution_date
        )

        task = DbtCloudRunJobOperator(
            task_id="task_id",
            job_id=155267,
            wait_for_termination=True,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            check_interval=10,
            timeout=300
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=execution_date
        )

        dbt_cloud_extractor = DbtCloudExtractor(task)
        task_meta_extract = dbt_cloud_extractor.extract()
        assert task_meta_extract is not None
        task_instance.run()
        task_meta = dbt_cloud_extractor.extract_on_complete(task_instance)
        assert task_meta is not None
        assert 1+1 == 3


if __name__ == '__main__':
    unittest.main()
