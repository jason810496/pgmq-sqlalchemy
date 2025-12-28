"""
config file for documentation(sphinx)
"""

import time
import os
import sys

# Handle Python 3.9+ compatibility for tomllib
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

# path setup
sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("../pgmq_sqlalchemy"))

# Read version from pyproject.toml
_pyproject_path = os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
try:
    with open(_pyproject_path, "rb") as f:
        _pyproject_data = tomllib.load(f)
        _version = _pyproject_data["project"]["version"]
except (FileNotFoundError, KeyError, OSError) as e:
    # Fallback to a default version if pyproject.toml is missing or invalid
    print(f"Warning: Could not read version from pyproject.toml: {e}")
    _version = "0.0.0"

extensions = [
    "sphinx_copybutton",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
]

html_theme = "sphinx_rtd_theme"
project = "pgmq-sqlalchemy"
copyright = f'2024-{time.strftime("%Y")}, the pgmq-sqlalchemy developers'

# Version information
# The short X.Y version
_version_parts = _version.split(".")
version = ".".join(_version_parts[:2]) if len(_version_parts) >= 2 else _version
# The full version, including alpha/beta/rc tags
release = _version

source_suffix = {
    ".rst": "restructuredtext",
}

master_doc = "index"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "conf.py"]
