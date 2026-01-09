"""Unit tests for DAG structure and configuration validation."""
from datetime import timedelta
from pathlib import Path

import pytest


DAG_PATH = Path(__file__).parent.parent.parent / "dags" / "etl_pipeline.py"
DAG_AVAILABLE = DAG_PATH.exists()

pytestmark = pytest.mark.skipif(
    not DAG_AVAILABLE,
    reason="DAG file not available in this environment"
)


class TestETLDagStructure:
    @pytest.fixture
    def dag(self):
        """Load the DAG without Airflow context."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("etl_pipeline", DAG_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.dag

    def test_dag_loads_without_errors(self, dag):
        """DAG file parses and loads correctly."""
        assert dag is not None
        assert dag.dag_id == "etl_pipeline"

    def test_dag_has_correct_schedule(self, dag):
        """DAG runs every 30 seconds as documented."""
        assert dag.schedule_interval == timedelta(seconds=30)

    def test_dag_has_correct_tags(self, dag):
        """DAG has expected tags for filtering."""
        assert "etl" in dag.tags
        assert "kafka" in dag.tags
        assert "postgres" in dag.tags

    def test_dag_catchup_disabled(self, dag):
        """Catchup disabled to prevent backfill storms."""
        assert dag.catchup is False

    def test_dag_max_active_runs(self, dag):
        """Only one DAG run at a time to prevent race conditions."""
        assert dag.max_active_runs == 1

    def test_dag_not_paused_on_creation(self, dag):
        """DAG starts active, not paused."""
        assert dag.is_paused_upon_creation is False

    def test_dag_has_retries_configured(self, dag):
        """Default args include retry configuration."""
        assert dag.default_args["retries"] == 3
        assert dag.default_args["retry_delay"] == timedelta(seconds=30)


class TestETLDagTasks:
    @pytest.fixture
    def dag(self):
        """Load the DAG without Airflow context."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("etl_pipeline", DAG_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.dag

    def test_dag_has_four_tasks(self, dag):
        """DAG has extract, transform, load, commit_offsets tasks."""
        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == 4
        assert "extract" in task_ids
        assert "transform" in task_ids
        assert "load" in task_ids
        assert "commit_offsets" in task_ids

    def test_task_dependencies_correct(self, dag):
        """Task flow: extract → transform → load → commit_offsets."""
        extract = dag.get_task("extract")
        transform = dag.get_task("transform")
        load = dag.get_task("load")
        commit = dag.get_task("commit_offsets")

        assert transform.task_id in [t.task_id for t in extract.downstream_list]
        assert load.task_id in [t.task_id for t in transform.downstream_list]
        assert commit.task_id in [t.task_id for t in load.downstream_list]

    def test_commit_offsets_trigger_rule(self, dag):
        """Commit offsets only runs if all upstream tasks succeed."""
        commit = dag.get_task("commit_offsets")
        assert commit.trigger_rule == "all_success"

    def test_extract_task_is_python_operator(self, dag):
        """Extract task uses PythonOperator."""
        extract = dag.get_task("extract")
        assert extract.__class__.__name__ == "PythonOperator"

    def test_all_tasks_are_python_operators(self, dag):
        """All tasks use PythonOperator."""
        for task in dag.tasks:
            assert task.__class__.__name__ == "PythonOperator"
