import sys
import os

# Tests run outside the pipeline runtime, so they need their own lightweight
# resolver that can find the bundle root before importing ``src.*`` modules.
def _init_bundle_path() -> None:
    """Resolve the project root and prepend it to ``sys.path``.

    The helper checks a few common execution anchors and prefers the bundle's
    ``/files`` root when present so tests behave the same way as deployed code.
    It must run before any ``src.*`` import statement.
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

        if "/files" in base_path:
            root = base_path.split("/files")[0] + "/files"
            if root not in sys.path:
                sys.path.insert(0, root)
            return

        if os.path.isdir(os.path.join(base_path, "src")):
            if base_path not in sys.path:
                sys.path.insert(0, base_path)
            return