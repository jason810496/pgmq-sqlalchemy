#!/usr/bin/env python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
#   "libcst>=1.0.0",
#   "ruff>=0.14.10",
# ]
# ///
"""
Script to check for missing async test functions in test_queue.py and generate them.

For each test function (starting with 'test_'), checks if there's a corresponding
async test with the same name plus '_async' suffix. If missing, generates it.
"""

import libcst as cst
import sys
from pathlib import Path
import contextlib
import shutil

import tempfile


from scripts_utils.config import TEST_QUEUE_FILE, TEST_QUEUE_BACKUP_FILE
from scripts_utils.console import console, user_input
from scripts_utils.queue_test_ast import (
    parse_test_functions_from_module,
    get_async_tests_to_add,
    fill_missing_tests_to_module,
)
from scripts_utils.formatting import format_file, compare_file


def main():
    """Main function."""

    module_tree = cst.parse_module(TEST_QUEUE_FILE.read_text())
    sync_tests, missing_async = parse_test_functions_from_module(module_tree)

    if not missing_async:
        console.print(
            "[bold green]SUCCESS:[/bold green] All test functions have corresponding async versions!"
        )
        sys.exit(0)

    # log all the missing async tests
    console.print()
    console.print(
        f"[bold yellow]WARNING:[/bold yellow] Found {len(missing_async)} missing async tests:",
        style="bold",
    )
    for test in missing_async:
        console.print(f"  [yellow]-[/yellow] {test}_async")
    console.print()

    # create missing async tests
    async_tests_to_add = get_async_tests_to_add(sync_tests, missing_async)
    # insert back to module
    module_tree = fill_missing_tests_to_module(module_tree, async_tests_to_add)

    # write back to tmp file for comparison
    with tempfile.NamedTemporaryFile(mode="w+t", delete=False, suffix=".py") as f:
        f.write(module_tree.code)
        f.flush()
        tmp_file = f.name
        console.log(f"Complete missing async tests at {tmp_file}")

    if tmp_file:
        max_formatting = 3
        for _ in range(max_formatting):
            if format_file(tmp_file):
                break

    _, missing_async_for_tmp = parse_test_functions_from_module(
        cst.parse_module(Path(tmp_file).read_text())
    )

    if missing_async_for_tmp:
        console.log(
            f"[error]Still have async tests missing after generating in {tmp_file}: {missing_async_for_tmp}[/]"
        )
    else:
        console.log("[success]All missing async tests are generated[/]")

    # compare existed test_queue.py and tmp.py
    with contextlib.suppress(Exception):
        compare_file(TEST_QUEUE_FILE, tmp_file)

    # ask whether to apply the change
    if user_input(f"Do you want to apply change to {TEST_QUEUE_FILE}"):
        console.log(f"Backup existed {TEST_QUEUE_FILE} at {TEST_QUEUE_BACKUP_FILE}")
        shutil.copy(TEST_QUEUE_FILE, TEST_QUEUE_BACKUP_FILE)
        shutil.copy(tmp_file, TEST_QUEUE_FILE)
        console.log("Add missing async tests successfully")

    sys.exit(0)


if __name__ == "__main__":
    main()
