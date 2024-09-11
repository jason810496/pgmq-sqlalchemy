"""
Internal script to update docstring.
Update common docstring define in pgmq_sqlalchemy/_docstring.py to respecitve module docstring.
"""

import sys
import importlib
import ast
import difflib
from typing import Union, Dict, List, Callable

sys.path.append("..")

from pgmq_sqlalchemy import (
    PGMQueue,
    func,
    async_func,
)
import pgmq_sqlalchemy._docstring as doc

"""
{
    "group_name":{
        "functions":[
            module_name.function_name,
            module_name.class_name.method_name,
        ],
        "docstring_before": DOCSTRING_BEFORE,
        "docstring_after": DOCSTRING_AFTER,
    }
}
"""
DEFINITION_TYPE = Dict[str, Dict[str, Union[str, List[Callable]]]]

docstring_definition: DEFINITION_TYPE = {
    "create_queue": {
        "functions": [
            PGMQueue.create_queue,
            func.create_queue,
            async_func.create_queue,
        ],
        "docstring_before": doc.CREATE_QUEUE_DOCSTRING_BEFORE,
        "docstring_after": doc.CREATE_QUEUE_DOCSTRING_AFTER,
    }
}


def update_docstring(definition: DEFINITION_TYPE):
    """Update docstring in the module."""
    for group_name, group_info in definition.items():
        functions = group_info["functions"]
        docstring_before = group_info["docstring_before"]
        docstring_after = group_info["docstring_after"]

        for function in functions:
            module_name = function.__module__
            function_name = function.__name__

            # Get the module object
            module = importlib.import_module(module_name)
            module_path = module.__file__

            # Read the content of the module file
            with open(module_path, "r") as file:
                module_content = file.read()

            # Find the function definition and update its docstring
            updated_module_content = update_function_docstring(
                module_content, function_name, docstring_before, docstring_after
            )

            # Preview changes
            has_changes = preview_changes(module_content, updated_module_content)

            if has_changes:
                user_input = input(
                    f"\nDo you want to apply these changes to {function_name} in {module_name}? (y/n): "
                ).lower()
                if user_input == "y":
                    # Write the updated content back to the file
                    with open(module_path, "w") as file:
                        file.write(updated_module_content)
                    print(f"Updated docstring for {function_name} in {module_name}")
                else:
                    print(
                        f"Changes for {function_name} in {module_name} were not applied."
                    )
            else:
                print(f"No changes needed for {function_name} in {module_name}")


def update_function_docstring(
    content: str, function_name: str, before: str, after: str
) -> str:
    """Update the docstring of a specific function in the given content."""
    tree = ast.parse(content)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Str)
            ):
                # Function has a docstring
                original_docstring = ast.get_docstring(node)
                new_docstring = f"{before}\n{original_docstring}\n{after}"
                node.body[0].value.s = new_docstring
            else:
                # Function doesn't have a docstring, create one
                new_docstring = f"{before}\n{after}"
                node.body.insert(0, ast.Expr(value=ast.Str(s=new_docstring)))

    return ast.unparse(tree)


def find_function_definition(module_content: str, function_name: str) -> str:
    """Find the function definition in the module content."""
    tree = ast.parse(module_content)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return ast.unparse(node)
    return None


def preview_changes(original: str, modified: str):
    """Generate and display a unified diff of the changes with colored output."""
    original_lines = original.splitlines(keepends=True)
    modified_lines = modified.splitlines(keepends=True)
    # color
    GREEN = "\033[92m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    ENDC = "\033[0m"

    # Generate the unified diff
    diff = difflib.unified_diff(
        original_lines, modified_lines, fromfile="original", tofile="modified"
    )

    has_changes = False
    print("Preview of changes:\n")
    for line in diff:
        if line.startswith("+") and not line.startswith("+++"):
            print(GREEN + line, end=ENDC)  # Green for added lines
            has_changes = True
        elif line.startswith("-") and not line.startswith("---"):
            print(RED + line, end=ENDC)  # Red for removed lines
            has_changes = True
        elif line.startswith("@"):
            print(CYAN + line, end=ENDC)  # Cyan for diff context lines
            has_changes = True
        else:
            print(line, end="")

    return has_changes


if __name__ == "__main__":
    update_docstring(docstring_definition)
