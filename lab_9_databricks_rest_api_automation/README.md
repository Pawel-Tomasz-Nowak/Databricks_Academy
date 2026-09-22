# Lab 9: Databricks REST API Automation

[![Build](https://img.shields.io/badge/build-passing-2ea44f)](../../actions)
[![Platform](https://img.shields.io/badge/platform-Azure%20Databricks-0078D4)](https://azure.microsoft.com/products/databricks)
[![Tooling](https://img.shields.io/badge/tooling-Databricks%20SDK%20Python-orange)](https://docs.databricks.com/en/dev-tools/sdk-python.html)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF)](https://docs.github.com/en/actions)

Lab 9 operationalizes the `music_popularity_spike_detection` lakehouse through the Databricks Jobs API and the official `databricks-sdk` Python client. It demonstrates how to provision short-lived compute, dispatch production workflows, observe execution state, and return meaningful process exit codes without depending on browser-based Databricks UI actions.

## Executive Summary

The YouTube Music Popularity Lakehouse already contains the data platform components required to ingest, transform, validate, and publish music engagement data. This lab adds the platform control plane: automation that can be invoked by a local operator, a scheduled runner, or GitHub Actions.

The distinction is operationally important:

- **UI-driven execution** depends on a human session, browser state, manual navigation, and visual interpretation of run status.
- **API-driven execution** is repeatable, auditable, scriptable, and composable with CI/CD. The caller can discover resources, submit work, poll lifecycle state, expose the run URL, and fail a deployment when the Databricks run fails.

For enterprise data platforms, this moves orchestration from an implicit operator habit into an explicit, version-controlled contract. Credentials are supplied non-interactively, compute intent is declared in code, and the pipeline result becomes a machine-readable process outcome.

## Architecture

```mermaid
flowchart LR
    A[GitHub Actions\n or Local Runner] --> B[databricks-sdk\n WorkspaceClient]
    B --> C[Databricks REST API]
    C --> D[Jobs API]
    D --> E[Ephemeral Jobs Compute\n SubmitTask + new_cluster]
    D --> F[Persistent Orchestrator\n [prod] Full Music ETL Workflow]
    E --> G[Setup Python Task]
    F --> H[Setup -> YouTube Producer\n -> Lakeflow Pipeline -> Reconciliation]
    B --> I[Run status + run page URL\n exit code]
```

### Control-plane components

| Component | Responsibility | Compute model | Completion contract |
| --- | --- | --- | --- |
| `automation/run_brief_compute.py` | Submit an ad-hoc setup workload with a declared cluster specification | Ephemeral single-node Jobs compute | Polls the submitted run and exits `0` only for `SUCCESS` |
| `automation/platform_run.py` | Discover the production job, trigger it, and monitor the run | Compute configured by the deployed production job | Exits `0` only when `[prod] Full Music ETL Workflow` succeeds |
| `.github/workflows/deploy.yml` | Deploy the production bundle and invoke the SDK runner | GitHub-hosted runner plus Databricks-managed compute | Fails the CD job on a non-zero Python exit code |
| `databricks.yml` and `resources/jobs_dlt.yml` | Define the bundle, targets, pipeline, and orchestrator job | `dev` and `prod` target configuration | Provides the named job that the SDK discovers |

### Ephemeral versus persistent orchestration

`run_brief_compute.py` is intended for a bounded, ad-hoc operation. It submits a `SubmitTask` containing `new_cluster`, so the task owns its compute definition and does not require an already-running interactive cluster. The cluster is created for the run and is not kept idle for future work.

`platform_run.py` is the production control path. It does not recreate the production job definition or hard-code a cluster for every task. Instead, it locates the bundle-managed job by name, calls `run_now()`, and observes the execution until Databricks reports a terminal state. The deployed job then coordinates the setup task, YouTube producer, Lakeflow pipeline, and reconciliation gate defined in `resources/jobs_dlt.yml`.

## Project Structure

```text
lab_9_databricks_rest_api_automation/
├── automation/
│   ├── platform_run.py       # Discovers, triggers, and polls the production job
│   ├── run_brief_compute.py  # Submits a one-time task with ephemeral compute
│   └── test_connection.py    # Verifies SDK authentication and workspace access
├── resources/
│   └── jobs_dlt.yml          # Lakeflow pipeline and Full Music ETL Workflow resources
├── src/                      # Pipeline, producer, setup, transformation, and quality code
├── tests/                    # Pytest coverage for transformations and reconciliation
├── screenshots/              # Execution evidence from compute and GitHub Actions
├── databricks.yml            # Bundle name, variables, and dev/prod targets
└── README.md                 # This module guide
```

## Automation Deep-Dive

### Ephemeral compute provisioning

`run_brief_compute.py` constructs a `ClusterSpec` directly in Python. The specification is deliberately small and single-node for a short setup workload:

```python
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
```

The task binds that specification to a Python entry point through `SubmitTask`:

```python
task = SubmitTask(
    task_key="ephemeral_setup_task",
    new_cluster=job_cluster_spec,
    spark_python_task=SparkPythonTask(
        python_file=setup_path,
        parameters=[
            "--catalog", "dbr_dev",
            "--schema", "music_analytics_prod",
            "--volume", "landing_zone_prod",
        ],
    ),
)

submitted_run = w.jobs.submit(
    run_name="lab9_ephemeral_compute_run",
    tasks=[task],
)
```

This pattern is useful for controlled batch actions where a persistent cluster would add idle cost or operational overhead. The script resolves the current workspace user through `w.current_user.me()` and points the task at the deployed bundle file path.

### Production workflow discovery and dispatch

The production runner avoids relying on a copied numeric job ID. It enumerates workspace jobs and selects the bundle-managed job by its stable business name:

```python
for job in w.jobs.list(expand_tasks=False):
    if job.settings and job.settings.name == TARGET_JOB_NAME:
        target_job = job
        break

run_response = w.jobs.run_now(job_id=target_job.job_id)
run_id = run_response.run_id
```

The target name is defined as:

```python
TARGET_JOB_NAME = "[prod] Full Music ETL Workflow"
```

This keeps the invocation coupled to the declared deployment contract in `resources/jobs_dlt.yml`, while allowing Databricks to manage the job ID in each workspace.

### Polling and exit-code enforcement

Both automation scripts use the same state model. They fetch the run with `w.jobs.get_run()`, report lifecycle and result state, and stop when the run reaches `TERMINATED`, `SKIPPED`, or `INTERNAL_ERROR`:

```python
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
        sys.exit(0 if result == RunResultState.SUCCESS else 1)

    time.sleep(POLL_INTERVAL_SECONDS)
```

The run URL is printed for operator investigation. A successful Databricks result returns process exit code `0`; all other terminal results return `1`. GitHub Actions therefore receives the platform outcome directly instead of treating API invocation as successful merely because a run was submitted.

## GitHub Actions Integration

The repository-level workflow at `.github/workflows/deploy.yml` implements the production CD path in four relevant stages:

1. Checks out the repository and provisions Python 3.11.
2. Installs `databricks-sdk` and the Databricks CLI.
3. Runs `databricks bundle deploy -t prod` from `lab_9_databricks_rest_api_automation`.
4. Runs `python automation/platform_run.py` from the same directory.

The SDK step receives the workspace credentials as environment variables:

```yaml
- name: Trigger & Monitor Production Workflow via SDK
  working-directory: ./lab_9_databricks_rest_api_automation
  env:
    DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
    DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  run: |
    python automation/platform_run.py
```

The CD job is gated by the CI job and runs only for a push to `main`. The deployment and execution logs are emitted into the Actions console, including the discovered job ID, run ID, lifecycle transitions, terminal result, and Databricks run page URL.

### Required GitHub secrets

| Secret | Purpose |
| --- | --- |
| `DATABRICKS_HOST` | Azure Databricks workspace URL, for example `https://adb-...azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Non-interactive credential with permission to deploy the bundle and run the production job |

Use a repository or environment secret with the minimum workspace permissions required by the deployment policy. Do not commit tokens, personal access tokens, or local configuration files.

## Local Setup

### Prerequisites

- Python 3.11 or a compatible supported Python version.
- Access to the target Azure Databricks workspace.
- Permission to deploy or run the target bundle resources.
- The Databricks Python SDK installed in the active environment.
- A deployed `prod` bundle before running the ephemeral script, because its Python path references the deployed workspace files.

Install the SDK from the Lab 9 directory:

```powershell
cd lab_9_databricks_rest_api_automation
python -m pip install --upgrade pip
python -m pip install databricks-sdk
```

### Option A: user-level Databricks configuration

Create or update `%USERPROFILE%\.databrickscfg`:

```ini
[DEFAULT]
host = https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net
token = dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

`WorkspaceClient()` reads the default profile when no explicit host or token is passed. Keep this file outside source control. For a named profile, select it before execution using the SDK's supported configuration environment, or export the host and token directly as shown below.

### Option B: environment variables

PowerShell:

```powershell
$env:DATABRICKS_HOST = "https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net"
$env:DATABRICKS_TOKEN = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

Command Prompt:

```cmd
set DATABRICKS_HOST=https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net
set DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

Environment variables are the preferred local pattern for ephemeral shells and the required pattern used by GitHub Actions. Avoid placing credentials in command history or committed scripts.

### Verify authentication

Run the connection check before invoking a job:

```powershell
python automation/test_connection.py
```

Expected output includes the authenticated user and workspace host. A failure at this stage indicates a credential, host, network, or permission issue rather than a pipeline execution failure.

## Local Execution

### Deploy the production bundle

From the Lab 9 directory, authenticate the Databricks CLI using the same configuration source and deploy the target:

```powershell
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

The bundle target uses the production schema and volume values declared in `databricks.yml`, including `music_analytics_prod` and `landing_zone_prod`.

### Trigger and monitor the production workflow

```powershell
python automation/platform_run.py
```

The command discovers `[prod] Full Music ETL Workflow`, starts it with `jobs.run_now()`, polls every 10 seconds, prints the run URL, and returns a non-zero process code when the Databricks run does not finish successfully.

### Submit an ephemeral setup workload

```powershell
python automation/run_brief_compute.py
```

This submits `lab9_ephemeral_compute_run` with a single-node `Standard_D4ds_v5` cluster using Databricks Runtime `15.4.x-scala2.12`. It is intended for a bounded ad-hoc execution and returns after the submitted run reaches a terminal state.

## Operational Notes

- The production runner searches by exact job name. If the bundle target name changes, update `TARGET_JOB_NAME` or preserve the current resource name.
- The ephemeral script references the deployed bundle path under the current workspace user's `.bundle` directory. Deploy the bundle first and confirm that the current identity is the identity expected by the path.
- `POLL_INTERVAL_SECONDS` is set to 10 seconds in both scripts. Increase it for very long-running workloads if API call volume needs to be reduced.
- The scripts do not retry failed API calls. Treat authentication, permissions, throttling, and network errors as automation failures and investigate them at the runner or workspace boundary.
- Use the printed run page URL and the captured Actions logs as the first operational evidence when diagnosing a failed run.

## Related Assets

- [Bundle definition](databricks.yml)
- [Job and pipeline resources](resources/jobs_dlt.yml)
- [Production SDK runner](automation/platform_run.py)
- [Ephemeral compute runner](automation/run_brief_compute.py)
- [Connection check](automation/test_connection.py)
- [GitHub Actions workflow](../.github/workflows/deploy.yml)
