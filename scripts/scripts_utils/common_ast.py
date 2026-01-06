from typing import List, Dict, Tuple, Literal, Set, TYPE_CHECKING
import sys

import libcst as cst

if TYPE_CHECKING:
    from rich.console import Console


class MethodInfo:
    """Information about a method."""

    def __init__(self, name: str, node: cst.FunctionDef):
        self.name = name
        self.node = node
        self.is_target = not name.startswith(
            "_"
        )  # all the public method is our target method for further processing
        self.is_async = name.endswith("_async")
        self.base_name = name[:-6] if self.is_async else name


class ParseTargetClassFunctionsVisitor(cst.CSTVisitor):
    """CST visitor to parse functions out of target class name for given module tree"""

    def __init__(self, class_name: str):
        self.class_name = class_name
        self.methods: List[MethodInfo] = []
        self.is_cur_node_in_target_class = False

    def visit_ClassDef(self, node: cst.ClassDef):
        if node.name.value == self.class_name:
            self.is_cur_node_in_target_class = True
            # Visit children
            for stmt in node.body.body:
                if isinstance(stmt, cst.FunctionDef):
                    self.methods.append(MethodInfo(stmt.name.value, stmt))
            self.is_cur_node_in_target_class = False


class FillMissingMethodsToClass(cst.CSTTransformer):
    """CST Transformer to fill missing async_methods back to target class"""

    def __init__(self, class_name: str, to_add_async_methods: Dict[str, MethodInfo]):
        self.class_name = class_name
        self.to_add_async_methods = to_add_async_methods

    def leave_ClassDef(
        self, original_node: cst.ClassDef, updated_node: cst.ClassDef
    ) -> cst.ClassDef:
        if updated_node.name.value == self.class_name:
            # Get current body statements
            body_statements = list(updated_node.body.body)
            new_body = []

            for stmt in body_statements:
                new_body.append(stmt)
                # If this is a sync function, check if we need to add async version after it
                if isinstance(stmt, cst.FunctionDef):
                    func_name = stmt.name.value
                    if func_name in self.to_add_async_methods:
                        # Add the async version right after the sync version
                        new_body.append(self.to_add_async_methods[func_name].node)

            # Update the class body with new statements
            return updated_node.with_changes(
                body=updated_node.body.with_changes(body=new_body)
            )

        return updated_node


def parse_methods_info_from_target_class(
    module_tree: cst.Module, target_class: Literal["PGMQueue", "PGMQOperation"]
) -> Tuple[List[MethodInfo], set[str]]:
    """
    Parse methods of target class from give module CST Tree

    Args:
        module_tree: cst.Module
        target_class: either "PGMQueue" or "PGMQOperation" str

    Returns:
        Tuple of sync_methods, missing_async_set
    """

    analyzer = ParseTargetClassFunctionsVisitor(target_class)
    module_tree.visit(analyzer)

    # Categorize methods
    # We use sync methods as source of truth
    async_methods_set = set()
    missing_async_set = set()

    for method_info in analyzer.methods:
        # skip non target methods
        if not method_info.is_target:
            continue

        if method_info.is_async:
            async_methods_set.add(method_info.base_name)

    # Find missing async methods and generate class with interleaved methods
    for method_info in analyzer.methods:
        # skip non target methods
        if not method_info.is_target:
            continue

        if method_info.base_name not in async_methods_set:
            missing_async_set.add(method_info.base_name)

    return analyzer.methods, missing_async_set


def fill_missing_methods_to_class(
    module_tree: cst.Module,
    target_class: Literal["PGMQueue", "PGMQOperation"],
    to_add_async_methods: Dict[str, MethodInfo],
) -> cst.Module:
    transformer = FillMissingMethodsToClass(
        class_name=target_class, to_add_async_methods=to_add_async_methods
    )
    return module_tree.visit(transformer)


def check_missing_async_methods(
    console: "Console",
    target_class: Literal["PGMQueue", "PGMQOperation"],
    missing_async: Set[str],
) -> None:
    if not missing_async:
        console.print(
            f"[bold green]SUCCESS:[/bold green] All public methods have corresponding async versions for {target_class}!"
        )
        sys.exit(0)

    # log all the missing async methods
    console.print()
    console.print(
        f"[bold yellow]WARNING:[/bold yellow] Found {len(missing_async)} missing async methods for {target_class}:",
        style="bold",
    )
    for method in missing_async:
        console.print(f"  [yellow]-[/yellow] {method}_async")
    console.print()

    sys.exit(1)
