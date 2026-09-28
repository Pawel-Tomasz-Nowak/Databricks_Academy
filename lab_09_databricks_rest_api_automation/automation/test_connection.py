"""Verify Databricks SDK authentication against the target workspace."""
import os
from databricks.sdk import WorkspaceClient


def verify_connection() -> None:
  # Automatically picks up DATABRICKS_HOST and DATABRICKS_TOKEN from environment
  w = WorkspaceClient()

  user = w.current_user.me()
  print(f"[AUTH SUCCESS] Connected as: {user.user_name}")
  print(f"[WORKSPACE] Host URL: {w.config.host}")


if __name__ == "__main__":
  verify_connection()