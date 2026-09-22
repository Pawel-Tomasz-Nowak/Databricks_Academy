"""Provision ephemeral compute, submit an ad-hoc job run, and monitor status via Databricks SDK."""
import sys
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    ClusterSpec,
    DataSecurityMode,
)
from databricks.sdk.service.jobs import (
    JobCluster,
    RunLifeCycleState,
    RunResultState,
    SparkPythonTask,
    SubmitTask,
)

POLL_INTERVAL_SECONDS = 10


def run_ephemeral_workload() -> None:
  w = WorkspaceClient()

  # Define lightweight single-node compute spec for ephemeral execution
  job_cluster_spec = ClusterSpec(
      spark_version="15.4.x-scala2.12",
      node_type_id="Standard_D4ds_v5",
      spark_conf={
          "spark.master": "local[*]",
          "spark.databricks.cluster.profile": "singleNode",
      },
      custom_tags={"ResourceClass": "SingleNode", "Environment": "Automation"},
      data_security_mode=DataSecurityMode.SINGLE_USER,
      num_workers=0,
  )

  # Define task running the setup module to verify dynamic execution
  task = SubmitTask(
      task_key="ephemeral_setup_task",
      new_cluster = job_cluster_spec,
      spark_python_task=SparkPythonTask(
          python_file=(
              f"/Workspace/Users/{w.current_user.me().user_name}/.bundle/music_popularity_spike_detection/prod/files/src/setup/music_pipeline_setup.py"
          ),
          parameters=[
              "--catalog",
              "dbr_dev",
              "--schema",
              "music_analytics_prod",
              "--volume",
              "landing_zone_prod",
          ],
      ),
  )

  print("[PROVISION] Submitting one-time job with ephemeral compute...")
  submitted_run = w.jobs.submit(
      run_name="lab9_ephemeral_compute_run",
      tasks=[task]
  )

  run_id = submitted_run.run_id
  print(f"[SUBMITTED] Run ID: {run_id}")

  # Polling loop to monitor run progress
  while True:
    run_info = w.jobs.get_run(run_id=run_id)
    state = run_info.state
    life_cycle = state.life_cycle_state if state else None
    result = state.result_state if state else None

    print(
        f"[STATUS] Life cycle: {life_cycle.value if life_cycle else 'UNKNOWN'}"
        f" | Result: {result.value if result else 'PENDING'}"
    )

    if life_cycle in [
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
        RunLifeCycleState.INTERNAL_ERROR,
    ]:
      print(f"[REPORT] Run URL: {run_info.run_page_url}")
      if result == RunResultState.SUCCESS:
        print("[SUCCESS] Ephemeral compute workload completed successfully.")
        sys.exit(0)
      else:
        print(f"[FAILED] Ephemeral workload ended with result: {result}")
        sys.exit(1)

    time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
  run_ephemeral_workload()