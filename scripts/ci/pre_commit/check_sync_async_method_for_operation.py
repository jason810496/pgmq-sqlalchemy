#!/usr/bin/env python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
#   "libcst>=1.0.0",
# ]
# ///
"""
Script to check for missing async methods in PGMQOperation for per-commit.

For each public sync method (not starting with _), checks if there's a corresponding
async method with the same name plus '_async' suffix.
"""

import libcst as cst
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__name__).parent.parent.joinpath("scripts").resolve()))

from scripts_utils.config import OPERATION_FILE  # noqa: E402
from scripts_utils.console import console  # noqa: E402
from scripts_utils.common_ast import (  # noqa: E402
    parse_methods_info_from_target_class,
    check_missing_async_methods,
)


def main():
    """Main function."""

    module_tree = cst.parse_module(OPERATION_FILE.read_text())
    _, missing_async = parse_methods_info_from_target_class(
        module_tree, target_class="PGMQOperation"
    )
    check_missing_async_methods(console, "PGMQOperation", missing_async)


if __name__ == "__main__":
    main()
