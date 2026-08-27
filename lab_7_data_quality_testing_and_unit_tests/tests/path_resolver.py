import sys
import os

# BULLETPROOF PATH RESOLUTION FOR STANDARD JOBS
# Databricks asset bundles (DABs) deploy code into a /files/ directory structure.
# We check multiple environmental markers to safely resolve the project root.
def _init_bundle_path() -> None:
    """Resolve the project root and prepend it to ``sys.path``.

    Attempts several strategies in order to locate the root that contains the
    ``src/`` package directory:

    1. Current working directory (``os.getcwd()``).
    2. Directory of the executing script (``__file__``) when available.
    3. Directory of the first ``sys.argv`` entry.

    When the resolved path is inside a Declarative Automation Bundle
    deployment (a ``/files`` segment is present), the function anchors to
    the ``/files`` root so that ``src.*`` package imports resolve correctly.
    Falls back to a direct ``src/`` directory check for non-bundle contexts.

    This function must be called before any ``src.*`` import statement.
    """
    possible_roots = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else "",
    ]
    
    if sys.argv and sys.argv[0]:
        possible_roots.append(os.path.dirname(os.path.abspath(sys.argv[0])))
        
    for base_path in possible_roots:
        if not base_path:
            continue
            
        # Handle standard DABs structure by anchoring to the "/files" root
        if "/files" in base_path:
            root = base_path.split("/files")[0] + "/files"
            if root not in sys.path:
                sys.path.insert(0, root)
            return
            
        # Fallback: look for the 'src' directory explicitly if /files is missing
        if os.path.isdir(os.path.join(base_path, "src")):
            if base_path not in sys.path:
                sys.path.insert(0, base_path)
            return