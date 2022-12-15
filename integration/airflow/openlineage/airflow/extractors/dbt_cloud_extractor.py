# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging, re, os, uuid
from datetime import datetime, timezone
import logging, traceback
from typing import List, Dict, Optional
from urllib.parse import urlparse

# for concurrent processing
from concurrent.futures import ThreadPoolExecutor

from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)

from openlineage.common.dataset import Dataset, Source, Field
from openlineage.client.facet import SqlJobFacet, ErrorMessageRunFacet
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.client import OpenLineageClient
from openlineage.common.provider.dbt import ParentRunMetadata
from openlineage.airflow.listener import run_data_holder

from airflow.hooks.base import BaseHook
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook

# Our version of DbtCloudArtifactProcessor
from dbt_utils import DbtCloudArtifactProcessor

log = logging.getLogger(__name__)

__version__ = "0.17.0"
PRODUCER = f'https://github.com/OpenLineage/OpenLineage/tree/{__version__}/integration/dbt'

_DAG_DEFAULT_NAMESPACE = 'default'
_DAG_NAMESPACE = os.getenv('OPENLINEAGE_NAMESPACE', None)
if not _DAG_NAMESPACE:
    _DAG_NAMESPACE = os.getenv(
        'MARQUEZ_NAMESPACE', _DAG_DEFAULT_NAMESPACE
    )


def dbt_run_event(
    state: RunState,
    job_name: str,
    job_namespace: str,
    run_id: str = str(uuid.uuid4()),
    parent: Optional[ParentRunMetadata] = None
) -> RunEvent:
    return RunEvent(
        eventType=state,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(
            runId=run_id,
            facets={
                "parent": parent.to_openlineage()
            } if parent else {}
        ),
        job=Job(
            namespace=parent.job_namespace if parent else job_namespace,
            name=job_name
        ),
        producer=PRODUCER
    )


def dbt_run_event_start(job_name: str, job_namespace: str, parent_run_metadata: ParentRunMetadata) -> RunEvent:
    return dbt_run_event(
        state=RunState.START,
        job_name=job_name,
        job_namespace=job_namespace,
        parent=parent_run_metadata
    )


def dbt_run_event_end(
    run_id: str,
    job_namespace: str,
    job_name: str,
    parent_run_metadata: Optional[ParentRunMetadata]
) -> RunEvent:
    return dbt_run_event(
        state=RunState.COMPLETE,
        job_namespace=job_namespace,
        job_name=job_name,
        run_id=run_id,
        parent=parent_run_metadata
    )


# class DbtCloudExtractor(BaseExtractor):
class DbtCloudExtractor(BaseExtractor):
    default_schema = 'public'
    
    def __init__(self, operator):
        super().__init__(operator)
        self.context = {}


    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['DbtCloudRunJobOperator', 'DbtCloudJobRunSensor']


    def get_task_metadata(self):
        try:
            operator = self.operator
            task_name = f"{operator.dag_id}.{operator.task_id}"
            run_facets: Dict = {}
            job_facets: Dict = {}

        except Exception as e:
            log.exception("Exception has occurred in extract()")
            error = ErrorMessageRunFacet(str(e), 'python')
            error.stackTrace = traceback.format_exc()
            run_facets['errorMessage'] = error
            raise Exception(e)
        finally:
            # finally return the task metadata
            return TaskMetadata(
                name=task_name,
                inputs=[],
                outputs=[],
                run_facets=run_facets,
                job_facets=job_facets,
            )


    def extract(self) -> TaskMetadata:
        operator = self.operator
        # according to the job type, pre-perform the necessary
        # fetch (job and project) of data so that it will save
        # time when creating lineage data at the completion
        if operator.__class__.__name__ == 'DbtCloudRunJobOperator':
            job_id = operator.job_id
            connection = BaseHook.get_connection(operator.dbt_cloud_conn_id)
            account_id = connection.login
            hook = DbtCloudHook(operator.dbt_cloud_conn_id)
            # use job_id to get connection
            job = hook.get_job(account_id=account_id, job_id=job_id).json()['data']
            project_id = job['project_id']
            project = hook.get_project(project_id=project_id, account_id=account_id).json()['data']
            connection = project['connection']
            self.context['connection'] = connection
            self.context['job'] = job

        elif operator.__class__.__name__ == 'DbtCloudJobRunSensor':
            run_id = operator.run_id
            connection = BaseHook.get_connection(operator.dbt_cloud_conn_id)
            account_id = connection.login
            hook = DbtCloudHook(operator.dbt_cloud_conn_id)
            job_run = hook.get_job_run(account_id=account_id, run_id=run_id, include_related=['job']).json()['data']
            project_id = job_run['project_id']
            job = job_run['job']
            project = hook.get_project(project_id=project_id, account_id=account_id).json()['data']
            connection = project['connection']
            self.context['connection'] = connection
            self.context['job'] = job

        return self.get_task_metadata()


    # internal method to extract dbt lineage and send it to
    # OL backend
    def extract_dbt_lineage(self, operator, run_id, parent_run_id=None, job_name=None):
        job = self.context['job']
        account_id = job['account_id']
        hook = DbtCloudHook(operator.dbt_cloud_conn_id)
        execute_steps = job['execute_steps']
        job_run = hook.get_job_run(run_id=run_id, account_id=account_id, include_related=['run_steps']).json()['data']
        run_steps = job_run['run_steps']
        connection = self.context['connection']
        steps = []

        for run_step in run_steps:
            name = run_step['name']
            if name.startswith("Invoke dbt with `"):
                regex_pattern = "Invoke dbt with `([^`.]*)`"
                m = re.search(regex_pattern, name)
                command = m.group(1)
                if command in execute_steps:
                    steps.append(run_step['index'])

        # RUN ARTIFACTS
        for step in steps:
            artifacts = {}
            self.run_io_tasks_in_parallel([
                lambda: self.get_dbt_artifacts(hook=hook, run_id=run_id, path="manifest.json", account_id=account_id, artifacts=artifacts, key="manifest", step=step),
                lambda: self.get_dbt_artifacts(hook=hook, run_id=run_id, path="run_results.json", account_id=account_id, artifacts=artifacts, key="run_results", step=step),
                lambda: self.get_dbt_artifacts(hook=hook, run_id=run_id, path="catalog.json", account_id=account_id, artifacts=artifacts, key="catalog", step=None)
            ])
            artifacts['connection'] = connection
            # process manifest
            manifest = artifacts['manifest']
            run_reason = manifest['metadata']['env']['DBT_CLOUD_RUN_REASON']
            # ex: Triggered via Apache Airflow by task 'trigger_job_run1' in the astronomy DAG.
            # regex pattern: Triggered via Apache Airflow by task '[a-zA-Z0-9_]+' in the [a-zA-Z-0-9_]+ DAG.
            regex_pattern = "Triggered via Apache Airflow by task '([a-zA-Z0-9_]+)' in the ([a-zA-Z0-9_]+) DAG\."
            m = re.search(regex_pattern, run_reason)
            task_id = m.group(1)
            dag_id = m.group(2)

            processor = DbtCloudArtifactProcessor(
                producer=PRODUCER,
                job_namespace=_DAG_NAMESPACE,
                skip_errors=False,
                logger=log,
                artifacts=artifacts
            )
            # if parent run id exists, set it as parent run metadata - so that
            # the DBT job can contain parent information (which is the current task)
            if parent_run_id is not None:
                parent_job = ParentRunMetadata(
                    run_id = parent_run_id,
                    job_name = job_name if job_name is not None else f"{dag_id}.{task_id}",
                    job_namespace = _DAG_NAMESPACE
                )
                processor.dbt_run_metadata = parent_job
            
            events = processor.parse().events()
            client = OpenLineageClient.from_environment()
            for event in events:
                client.emit(event)


    def get_dbt_artifacts(self, hook, run_id, path, account_id, step, artifacts, key):
        response = None
        try:
            response = hook.get_job_run_artifact(run_id=run_id, path=path, account_id=account_id, step=step).json()
        except:
            log.error(f"Error has occurred when getting {path} on {account_id} in {run_id} with step {step}. Will skip the file")
        
        if response is not None:
            artifacts[key] = response


    def run_io_tasks_in_parallel(self, tasks):
        with ThreadPoolExecutor() as executor:
            running_tasks = [executor.submit(task) for task in tasks]
            for running_task in running_tasks:
                running_task.result()


    # extract that happens on the 'complete side' 
    # we should try to see if we can induce any error here.
    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        task_meta_data = self.get_task_metadata()
        operator = self.operator
        # if the wait for termination is true, then this means it will wait
        # for the DBT job to complete or timeout.
        if operator.__class__.__name__ == 'DbtCloudRunJobOperator':
            run_data = run_data_holder.get_active_run(task_instance)
            parent_run_id = run_data.run_id
            run_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key='return_value')
            if operator.wait_for_termination is True:
                # extract for dbt lineage with given operator
                self.extract_dbt_lineage(operator=operator, run_id=run_id, parent_run_id=parent_run_id, job_name=f"{task_instance.dag_id}.{task_instance.task_id}")

        # if the operator is DbtCloudJobRunSensor, then we will try to emit it, as part of the sensor
        elif operator.__class__.__name__ == 'DbtCloudJobRunSensor':
            run_data = run_data_holder.get_active_run(task_instance)
            parent_run_id = run_data.run_id
            run_id = operator.run_id
            # extract for dbt lineage with given operator
            self.extract_dbt_lineage(operator=operator, run_id=run_id, parent_run_id=parent_run_id, job_name=f"{task_instance.dag_id}.{task_instance.task_id}")

        return task_meta_data

