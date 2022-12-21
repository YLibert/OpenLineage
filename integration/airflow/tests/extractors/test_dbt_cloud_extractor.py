# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import logging
import unittest
from mock import MagicMock
import uuid
import pytz
import json
from datetime import datetime
import mock
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


def emit_event(event):
    print(event)
    assert event.run.facets['parent'].job['name'] == "TestDBTCloudExtractor.task_id"
    assert event.run.facets['parent'].job['namespace'] == "default"
    assert event.job.namespace == "default"
    assert event.job.name.startswith("SANDBOX.HOWARDYOO.my_new_project")

    if len(event.inputs) > 0:
        assert event.inputs[0].facets['dataSource'].name == "snowflake://gp21411.us-east-1"
        assert event.inputs[0].facets['dataSource'].uri == "snowflake://gp21411.us-east-1"
        assert event.inputs[0].facets['schema'].fields[0].name.upper() == "ID"
        if event.inputs[0].name == "SANDBOX.HOWARDYOO.my_first_dbt_model":
            assert event.inputs[0].facets['schema'].fields[0].type.upper() == "NUMBER"
    if len(event.outputs) > 0:
        assert event.outputs[0].facets['dataSource'].name == "snowflake://gp21411.us-east-1"
        assert event.outputs[0].facets['dataSource'].uri == "snowflake://gp21411.us-east-1"
        assert event.outputs[0].facets['schema'].fields[0].name.upper() == "ID"
        if event.outputs[0].name == "SANDBOX.HOWARDYOO.my_first_dbt_model":
            assert event.outputs[0].facets['schema'].fields[0].type.upper() == "NUMBER"


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

    @mock.patch("tests.extractors.test_dbt_cloud_extractor.TaskInstance")
    @mock.patch("openlineage.airflow.extractors.dbt_cloud_extractor.OpenLineageClient")
    @mock.patch("openlineage.airflow.extractors.dbt_cloud_extractor.run_data_holder")
    @mock.patch("openlineage.airflow.extractors.dbt_cloud_extractor.BaseHook")
    @mock.patch("openlineage.airflow.extractors.dbt_cloud_extractor.DbtCloudHook")
    def test_extractor(self, dbt_cloud_hook, base_hook, run_data_holder, ol_client, task_mock):
        log.info("dbt_cloud_extractor_test")
        base_hook.get_connection.return_value.login = 117664
        mock_hook = MagicMock()
        dbt_cloud_hook.return_value = mock_hook
        mock_client = MagicMock()
        ol_client.from_environment.return_value = mock_client
        mock_client.emit.side_effect = emit_event
        mock_hook.get_project.return_value.json.return_value = {
            "data": {
                "connection": {
                    "type": "snowflake",
                    "details": {
                        "account": "gp21411.us-east-1",
                        "database": "SANDBOX",
                        "warehouse": "HUMANS",
                        "allow_sso": False,
                        "client_session_keep_alive": False,
                        "role": None
                    }
                }
            }
        }
        mock_hook.get_job.return_value.json.return_value = {
            "data": {
                "project_id": 177370,
                "account_id": 117664,
                "execute_steps": [
                    "dbt run --select my_first_dbt_model"
                ]
            }
        }
        mock_hook.get_job_run.return_value.json.return_value = {
            "data": {
                "run_steps": [
                    {
                        "name": "Invoke dbt with `dbt run --select my_first_dbt_model`",
                        "index": 4
                    }
                ]
            }
        }
        mock_hook.get_job_run_artifact.side_effect = get_dbt_artifact
        run_data_holder.get_active_run.return_value.run_id = "f8e07aae-d70f-40b1-bc45-5612be0e7295"
        task_mock.return_value.xcom_pull.return_value = "b3a814a3-ab93-414c-a5b1-6bfc74576356"

        execution_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        default_args = {"dbt_cloud_conn_id": "dbt_cloud"}
        dag = DAG(dag_id='TestDBTCloudExtractor', default_args=default_args)
        dag.create_dagrun(
            run_id=str(uuid.uuid4()),
            state=State.QUEUED,
            execution_date=execution_date
        )

        task = DbtCloudRunJobOperator(
            dag=dag,
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

        assert dbt_cloud_extractor.context['connection']['type'] == 'snowflake'
        assert dbt_cloud_extractor.context['job']['project_id'] == 177370
        assert dbt_cloud_extractor.context['job']['account_id'] == 117664
        assert dbt_cloud_extractor.context['job']['execute_steps'][0] \
            == "dbt run --select my_first_dbt_model"

        task_meta_extract_complete = dbt_cloud_extractor.extract_on_complete(task_instance)
        assert task_meta_extract_complete is not None


if __name__ == '__main__':
    unittest.main()
