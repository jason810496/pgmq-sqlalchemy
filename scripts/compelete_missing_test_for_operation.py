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
Script to check for missing async tests in test_operation.py and generate them.

For each public sync test (test_*_sync), checks if there's a corresponding
async test with _async suffix. If missing, generates it using CST transformations.
"""

import libcst as cst
import sys
from pathlib import Path
import contextlib
import shutil
import tempfile


from scripts_utils.console import console, user_input
from scripts_utils.formatting import format_file, compare_file
from scripts_utils.operation_test_ast import (
    parse_test_functions_from_module,
    get_async_tests_to_add,
    fill_missing_tests_to_module,
)


def main():
    """Main function."""

    # Define test file path
    PROJECT_ROOT = Path(__file__).parent.parent
    TEST_FILE = PROJECT_ROOT / "tests" / "test_operation.py"
    TEST_BACKUP_FILE = PROJECT_ROOT / "tests" / "test_operation_backup.py"

    if not TEST_FILE.exists():
        console.print(f"[bold red]ERROR:[/bold red] Test file not found: {TEST_FILE}")
        sys.exit(1)

    module_tree = cst.parse_module(TEST_FILE.read_text())
    all_tests, missing_async = parse_test_functions_from_module(module_tree)

    if not missing_async:
        console.print(
            "[bold green]SUCCESS:[/bold green] All sync tests have corresponding async versions!"
        )
        sys.exit(0)

    # Log all the missing async tests
    console.print()
    console.print(
        f"[bold yellow]WARNING:[/bold yellow] Found {len(missing_async)} missing async tests:",
        style="bold",
    )
    for test_name in sorted(missing_async):
        async_name = test_name.replace("_sync", "_async")
        console.print(f"  [yellow]-[/yellow] {async_name}")
    console.print()

    # Create missing async tests
    async_tests_to_add = get_async_tests_to_add(all_tests, missing_async)

    # Insert back to module
    module_tree = fill_missing_tests_to_module(module_tree, async_tests_to_add)

    # Write back to tmp file for comparison
    tmp_file = ""
    with tempfile.NamedTemporaryFile(mode="w+t", delete=False, suffix=".py") as f:
        f.write(module_tree.code)
        f.flush()
        tmp_file = f.name
        console.log(f"Generated missing async tests at {tmp_file}")

    if tmp_file:
        max_formatting = 3
        for _ in range(max_formatting):
            if format_file(tmp_file):
                break

    # Verify that all async tests are now present
    _, missing_async_for_tmp = parse_test_functions_from_module(
        cst.parse_module(Path(tmp_file).read_text())
    )

    if missing_async_for_tmp:
        console.log(
            f"[error]Still have missing async tests after generation in {tmp_file}: {missing_async_for_tmp}[/]"
        )
    else:
        console.log("[success]All missing async tests are generated[/]")

    # Compare existing test file and tmp file
    with contextlib.suppress(Exception):
        compare_file(TEST_FILE, tmp_file)

    # Ask whether to apply the change
    if user_input(f"Do you want to apply change to {TEST_FILE}"):
        console.log(f"Backup existing {TEST_FILE} at {TEST_BACKUP_FILE}")
        shutil.copy(TEST_FILE, TEST_BACKUP_FILE)
        shutil.copy(tmp_file, TEST_FILE)
        console.log("Added missing async tests successfully")

    sys.exit(0)


if __name__ == "__main__":
    main()
