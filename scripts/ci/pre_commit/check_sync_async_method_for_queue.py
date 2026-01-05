#!/usr/bin/env python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
#   "libcst>=1.0.0",
# ]
# ///
"""
Script to check for missing async methods in PGMQueue for per-commit.

For each public sync method (not starting with _), checks if there's a corresponding
async method with the same name plus '_async' suffix.
"""

import libcst as cst
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__name__).parent.parent.joinpath("scripts").resolve()))

from scripts_utils.config import QUEUE_FILE  # noqa: E402
from scripts_utils.console import console  # noqa: E402
from scripts_utils.common_ast import parse_methods_info_from_target_class  # noqa: E402


def main():
    """Main function."""

    module_tree = cst.parse_module(QUEUE_FILE.read_text())
    _, missing_async = parse_methods_info_from_target_class(
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

    sys.exit(1)


if __name__ == "__main__":
    main()
