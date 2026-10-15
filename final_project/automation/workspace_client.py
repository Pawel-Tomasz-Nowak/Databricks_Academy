"""Shared Databricks workspace client configuration for local automation."""
import os

from databricks.sdk import WorkspaceClient


PROFILE = "adb-7405618361895520"


def get_workspace_client() -> WorkspaceClient:
  """Create a client for the selected workspace profile.

  Environment variables containing an explicit host or token take precedence
  over the SDK profile, so remove those stale process-level overrides first.
  """
  for variable in (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_OAUTH_TOKEN",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
  ):
    os.environ.pop(variable, None)

  return WorkspaceClient(profile=PROFILE)
