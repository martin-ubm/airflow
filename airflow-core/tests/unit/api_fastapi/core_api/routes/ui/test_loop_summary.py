# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
from uuid import uuid4

import pytest
from sqlalchemy import select

from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.loop_clear import clear_loop_task_instances
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.sdk import task, task_group
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import capture_orm_selects

pytestmark = pytest.mark.db_test


@pytest.fixture
def completed_loop(dag_maker):
    @task
    def work():
        return 1

    @task_group
    def body():
        """Repeat the work in this run's definition."""
        work()

    @task
    def finish():
        return "done"

    with dag_maker(serialized=False) as dag:
        body.loop(max_iterations=3) >> finish()
    return dag.test()


def test_fixed_loop_summary_reads_live_iterations(test_client, completed_loop, session):
    run = completed_loop
    path = f"/grid/loop/{run.dag_id}/{run.run_id}/body"
    with capture_orm_selects("task_instance") as statements:
        response = test_client.get(path)
    assert response.status_code == 200, response.text
    member_queries = [statement for statement in statements if "task_instance.region_index" in statement]
    assert len(member_queries) == 1
    assert "task_instance.task_id IN (" in member_queries[0]
    assert "'body.work'" in member_queries[0]
    assert "finish" not in member_queries[0]
    assert not any("task_instance_note" in statement for statement in statements)
    summary = response.json()
    assert summary["status"] == "ran_to_cap"
    assert summary["reason"] == "cap_reached"
    assert summary["exit_criteria_name"] is None
    assert summary["iterations_ran"] == 3
    assert [iteration["index"] for iteration in summary["iterations"]] == [0, 1, 2]
    assert all(iteration["criteria"] is None for iteration in summary["iterations"])
    family = summary["loop_region_id"]
    assert summary["loop_regions"] == [{"region_id": family, "parent_iterations": []}]
    assert test_client.get(path, params={"loop_region_id": str(uuid4())}).status_code == 404

    gate = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.run_id == run.run_id,
            TaskInstance.task_id == summary["exit_task_id"],
            TaskInstance.region_index == 1,
        )
    )
    clear_loop_task_instances([gate], session=session, later_loop_iterations=True)
    session.commit()
    current = test_client.get(path).json()
    assert [iteration["index"] for iteration in current["iterations"]] == [0, 1]
    assert current["loop_region_id"] == family
    assert current["status"] == "running"


def test_summary_reports_one_invocation_across_a_fork_after_gate_clear(test_client, completed_loop, session):
    run = completed_loop
    path = f"/grid/loop/{run.dag_id}/{run.run_id}/body"
    initial = test_client.get(path).json()
    family, gate_task_id = initial["loop_region_id"], initial["exit_task_id"]
    gate = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.run_id == run.run_id,
            TaskInstance.task_id == gate_task_id,
            TaskInstance.region_index == 0,
        )
    )
    clear_loop_task_instances([gate], session=session, later_loop_iterations=True)
    fork = session.scalar(select(DynamicRegion).where(DynamicRegion.forked_from_region_id.is_not(None)))
    dag = DBDagBag().get_dag(run.created_dag_version_id, session=session)
    for index in (1, 2):
        for task_id in ("body.work", gate_task_id):
            session.add(
                TaskInstance(
                    task=dag.get_task(task_id),
                    run_id=run.run_id,
                    dag_version_id=run.created_dag_version_id,
                    region_id=fork.id,
                    region_index=index,
                    state=TaskInstanceState.SUCCESS,
                )
            )
    session.commit()

    summary = test_client.get(path).json()

    assert summary["loop_region_id"] == family
    assert summary["loop_regions"] == [{"region_id": family, "parent_iterations": []}]
    assert [iteration["index"] for iteration in summary["iterations"]] == [0, 1, 2]
    assert test_client.get(path, params={"loop_region_id": str(fork.id)}).status_code == 404


@pytest.mark.parametrize(("gate_state", "expected"), [("skipped", "skipped"), ("failed", "failed")])
def test_summary_does_not_guess_condition_failure(test_client, completed_loop, session, gate_state, expected):
    run = completed_loop
    path = f"/grid/loop/{run.dag_id}/{run.run_id}/body"
    summary = test_client.get(path).json()
    gate = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.run_id == run.run_id,
            TaskInstance.task_id == summary["exit_task_id"],
            TaskInstance.region_index == 2,
        )
    )
    gate.state = TaskInstanceState(gate_state)
    session.commit()
    result = test_client.get(path).json()
    assert result["status"] == expected
    assert result["reason"] == ("iteration_failed" if gate_state == "failed" else None)


def test_loop_history_and_structure_use_pinned_contract(test_client, completed_loop):
    run = completed_loop
    response = test_client.get(f"/grid/loop-history/{run.dag_id}/body")
    assert response.status_code == 200, response.text
    result = response.json()["runs"][-1]
    assert result["iterations_ran"] == 3
    assert result["max_iterations"] == 3
    response = test_client.get(f"/grid/structure/{run.dag_id}")
    assert response.status_code == 200, response.text
    assert response.json()[0]["is_loop"] is True
    assert response.json()[0].get("is_mapped") is None
    assert response.json()[0]["loop_max_iterations"] == 3
    assert response.json()[0].get("loop_exit_task_id") is None


def test_loop_summary_missing_run(test_client):
    assert test_client.get("/grid/loop/absent/run/body").status_code == 404


def test_manually_completed_gate_does_not_report_running_forever(test_client, dag_maker, session):
    @task
    def work():
        return 1

    @task_group
    def body():
        work()

    with dag_maker():
        body.loop(max_iterations=3)
    run = dag_maker.create_dagrun()
    for ti in run.get_task_instances(session=session):
        ti.state = TaskInstanceState.SUCCESS
    session.commit()
    response = test_client.get(f"/grid/loop/{run.dag_id}/{run.run_id}/body")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "stopped_early"
    assert response.json()["reason"] is None
    assert response.json()["iterations"][0]["decision"] is None


def test_loop_summary_and_grid_do_not_count_mapped_slots_as_iterations(test_client, dag_maker):
    @task
    def work(value):
        return value

    @task_group
    def body():
        work.expand(value=[1, 2, 3])

    with dag_maker(serialized=False) as dag:
        body.loop(max_iterations=2)
    run = dag.test()
    response = test_client.get(f"/grid/loop/{run.dag_id}/{run.run_id}/body")
    assert response.status_code == 200, response.text
    assert response.json()["iterations_ran"] == 2
    assert [iteration["index"] for iteration in response.json()["iterations"]] == [0, 1]
    response = test_client.get(f"/grid/ti_summaries/{run.dag_id}", params={"run_ids": [run.run_id]})
    assert response.status_code == 200, response.text
    tasks = json.loads(response.text)["task_instances"]
    assert next(ti for ti in tasks if ti["task_id"] == "body")["loop_iterations_count"] == 2
    assert next(ti for ti in tasks if ti["task_id"] == "body.work")["loop_iterations_count"] is None


def test_summary_keeps_pinned_loop_after_run_moves_to_definition_without_loop(
    test_client, completed_loop, dag_maker, session
):
    run = completed_loop

    @task
    def finish():
        return "new definition"

    with dag_maker(dag_id=run.dag_id):
        finish()
    dag_maker.create_dagrun(run_id="new_definition")
    outside = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == run.dag_id,
            TaskInstance.run_id == run.run_id,
            TaskInstance.task_id == "finish",
        )
    )
    old_version = outside.dag_version_id
    clear_task_instances([outside], session=session, run_on_latest_version=True)
    session.commit()
    run = session.get(DagRun, run.id, populate_existing=True)
    assert run.created_dag_version_id != old_version
    path = f"/grid/loop/{run.dag_id}/{run.run_id}/body"
    response = test_client.get(path)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "removed"
    assert response.json()["max_iterations"] == 3
    assert response.json()["doc_md"] == "Repeat the work in this run's definition."
    assert [iteration["state"] for iteration in response.json()["iterations"]] == ["removed"] * 3


def test_condition_docstring_reaches_loop_summary_and_structure(test_client, dag_maker):
    @task
    def work():
        return 1

    @task_group
    def body():
        work()

    def converged(*, loop):
        """Stop when the result is ready."""
        return True

    with dag_maker(serialized=False) as dag:
        body.loop(max_iterations=3, until=converged)
    run = dag.test()
    response = test_client.get(f"/grid/loop/{run.dag_id}/{run.run_id}/body")
    assert response.status_code == 200, response.text
    assert response.json()["exit_criteria_doc"] == "Stop when the result is ready."
    assert response.json()["exit_criteria_name"] == "converged"
    assert response.json()["status"] == "stopped_early"
    response = test_client.get(f"/grid/structure/{run.dag_id}")
    assert response.json()[0]["loop_exit_criteria_doc"] == "Stop when the result is ready."
