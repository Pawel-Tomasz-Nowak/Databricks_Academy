"""Verify Databricks SDK authentication against the target workspace."""
from workspace_client import get_workspace_client


def verify_connection() -> None:
  w = get_workspace_client()

  user = w.current_user.me()
  print(f"[AUTH SUCCESS] Connected as: {user.user_name}")
  print(f"[WORKSPACE] Host URL: {w.config.host}")


if __name__ == "__main__":
  verify_connection()