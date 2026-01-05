from typing import List, Dict, Tuple, Literal

import ast


class MethodInfo:
    """Information about a method."""

    def __init__(self, name: str, node: ast.FunctionDef):
        self.name = name
        self.node = node
        self.is_target = not name.startswith(
            "_"
        )  # all the public method is our target method for further processing
        self.is_async = name.endswith("_async")
        self.base_name = name[:-6] if self.is_async else name


class ParseTargetClassFunctionsVisitor(ast.NodeVisitor):
    """AST visitor to parse functions out of target class name for given module tree"""

    def __init__(self, class_name: str):
        self.class_name = class_name
        self.methods: List[MethodInfo] = []
        self.is_cur_node_in_target_class = False

    def visit_ClassDef(self, node: ast.ClassDef):
        if node.name == self.class_name:
            self.is_cur_node_in_target_class = True
            self.generic_visit(node)
            self.is_cur_node_in_target_class = False
        else:
            self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if self.is_cur_node_in_target_class:
            # add all the method to the methods
            self.methods.append(MethodInfo(node.name, node))
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        if self.is_cur_node_in_target_class:
            # add all the method to the methods
            self.methods.append(MethodInfo(node.name, node))
        self.generic_visit(node)


class FillMissingMethodsToClass(ast.NodeTransformer):
    """AST Transformer to fill missing async_methods back to target class"""

    def __init__(self, class_name: str, to_add_async_methods: Dict[str, MethodInfo]):
        self.class_name = class_name
        self.to_add_async_methods = to_add_async_methods

    def visit_ClassDef(self, node: ast.ClassDef):
        if node.name == self.class_name:
            for sync_func_name, async_func_node in self.to_add_async_methods.items():
                idx = next(
                    (
                        i
                        for i, stmt in enumerate(node.body)
                        if isinstance(stmt, ast.FunctionDef)
                        and stmt.name == sync_func_name
                    ),
                    -1,
                )

                if idx != -1:
                    node.body.insert(idx + 1, async_func_node.node)

        return self.generic_visit(node)


def parse_methods_info_from_target_class(
    module_tree: ast.Module, target_class: Literal["PGMQueue", "PGMQOperation"]
) -> Tuple[List[MethodInfo], set[str]]:
    """
    Parse methods of target class from give module AST Tree

    Args:
        module_tree: ast.Module
        target_class: either "PGMQueue" or "PGMQOperation" str

    Returns:
        Tuple of sync_methods, missing_async_set
    """

    analyzer = ParseTargetClassFunctionsVisitor(target_class)
    analyzer.visit(module_tree)

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
    module_tree: ast.Module,
    target_class: Literal["PGMQueue", "PGMQOperation"],
    to_add_async_methods: Dict[str, MethodInfo],
):
    transformer = FillMissingMethodsToClass(
        class_name=target_class, to_add_async_methods=to_add_async_methods
    )
    transformer.visit(module_tree)
