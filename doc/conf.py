"""
config file for documentation(sphinx)
"""

import time
import os
import sys

# path setup
sys.path.insert(0, os.path.abspath("../pgmq_sqlalchemy"))

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

source_suffix = {
    ".rst": "restructuredtext",
}

master_doc = "index"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "conf.py"]
