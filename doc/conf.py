"""
config file for documentation(sphinx)
"""

import time
import os
import sys
import tomllib

# path setup
sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("../pgmq_sqlalchemy"))

# Read version from pyproject.toml
_pyproject_path = os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
with open(_pyproject_path, "rb") as f:
    _pyproject_data = tomllib.load(f)
    _version = _pyproject_data["project"]["version"]

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
version = ".".join(_version.split(".")[:2])
# The full version, including alpha/beta/rc tags
release = _version

source_suffix = {
    ".rst": "restructuredtext",
}

master_doc = "index"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "conf.py"]
