#!/usr/bin/env python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Script to check for missing async methods in PGMQueue class and generate them.

For each public sync method (not starting with _), checks if there's a corresponding
async method with the same name plus '_async' suffix. If missing, generates it.
"""

import ast
import sys
from pathlib import Path
import contextlib

import tempfile


from scripts_utils.config import QUEUE_FILE
from scripts_utils.console import console
from scripts_utils.common_ast import (
    parse_methods_info_from_target_class,
    fill_missing_methods_to_class,
)
from scripts_utils.formatting import format_file, compare_file
from scripts_utils.queue_ast import get_async_methods_to_add


def main():
    """Main function."""

    module_tree = ast.parse(source=QUEUE_FILE.read_text(), filename=QUEUE_FILE)
    sync_methods, missing_async = parse_methods_info_from_target_class(
        module_tree, target_class="PGMQueue"
    )

    if not missing_async:
        console.print(
            "[bold green]SUCCESS:[/bold green] All public methods have corresponding async versions!"
        )
        sys.exit(0)

    # log all the missing async methods
    console.print()
    console.print(
        f"[bold yellow]WARNING:[/bold yellow] Found {len(missing_async)} missing async methods:",
        style="bold",
    )
    for method in missing_async:
        console.print(f"  [yellow]-[/yellow] {method}_async")
    console.print()

    # create missing async method from
    async_methods_to_add = get_async_methods_to_add(sync_methods, missing_async)
    # insert back to class
    fill_missing_methods_to_class(module_tree, "PGMQueue", async_methods_to_add)
    module_tree = ast.fix_missing_locations(module_tree)

    # write back to tmp file for comparison
    tmp_file = ""
    with tempfile.NamedTemporaryFile(mode="w+t", delete=False, suffix=".py") as f:
        f.write(ast.unparse(module_tree))
        f.flush()
        tmp_file = f.name
        console.log(f"Complete missing async methods at {tmp_file}")

    if tmp_file:
        max_formatting = 3
        for _ in range(max_formatting):
            if format_file(tmp_file):
                break

    _, missing_async_for_tmp = parse_methods_info_from_target_class(
        ast.parse(Path(tmp_file).read_text()), "PGMQueue"
    )

    if missing_async_for_tmp:
        console.log(
            f"[error]Still get async methods to add after generating missing async methods in {tmp_file}: {missing_async_for_tmp}[/]"
        )
    else:
        console.log("[success]All missing async methods are generated[/]")

    # compare existed queue.py and tmp.py
    with contextlib.suppress(Exception):
        compare_file(QUEUE_FILE, tmp_file)

    sys.exit(0)


if __name__ == "__main__":
    main()
