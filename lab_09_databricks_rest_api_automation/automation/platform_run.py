"""Automate platform operations via Databricks SDK: trigger job, monitor run, and report status."""
import sys
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

TARGET_JOB_NAME = "[prod] Full Music ETL Workflow"
POLL_INTERVAL_SECONDS = 10 # Arbitrary time elapsing between two consecutive calls for job status


def trigger_and_monitor_workflow() -> None:
  w = WorkspaceClient()

  # Locate target job by name
  print(f"[SEARCH] Looking for job: '{TARGET_JOB_NAME}'...")
  target_job = None
  for job in w.jobs.list(expand_tasks=False):
    if job.settings and job.settings.name == TARGET_JOB_NAME:
      target_job = job
      break

  if not target_job:
    print(f"[ERROR] Job '{TARGET_JOB_NAME}' not found in workspace.")
    sys.exit(1)

  print(f"[FOUND] Job ID: {target_job.job_id}")

  # Trigger the job run programmatically
  print(f"[TRIGGER] Triggering job {target_job.job_id} via API...")
  run_response = w.jobs.run_now(job_id=target_job.job_id)
  run_id = run_response.run_id
  print(f"[TRIGGERED] Run ID: {run_id}")

  # Monitor run status 
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
        print("[SUCCESS] Pipeline completed successfully via API automation.")
        sys.exit(0)
      else:
        print(f"[FAILED] Pipeline terminated with state: {result}")
        sys.exit(1)

    time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
  trigger_and_monitor_workflow()