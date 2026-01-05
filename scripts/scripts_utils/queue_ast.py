import ast
import re
import sys
from pathlib import Path
from typing import List, Set, Dict
import copy


sys.path.insert(0, str(Path(__name__).parent.parent.joinpath("scripts").resolve()))

from scripts_utils.common_ast import MethodInfo  # noqa: E402


class AsyncFuncTransformer(ast.NodeTransformer):
    to_replace_execute_func_attr: str = "_execute_operation"
    target_execute_func_attr: str = "_execute_async_operation"

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute):
            # Handle PGMQOperation.method calls
            for arg in node.args:
                if isinstance(arg, ast.Attribute) and (
                    isinstance(arg.value, ast.Name) and arg.value.id == "PGMQOperation"
                ):
                    # Add _async suffix to method name
                    arg.attr = f"{arg.attr}_async"

            # Replace `self._execute_operation` to `self._execute_async_operation`
            if (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "self"
                and node.func.attr == self.to_replace_execute_func_attr
            ):
                node.func.attr = self.target_execute_func_attr

        return self.generic_visit(node)

    def visit_FunctionDef(self, node):
        # Transform function to async
        new_node = ast.AsyncFunctionDef(
            name=f"{node.name}_async",
            args=node.args,
            body=node.body,
            decorator_list=node.decorator_list,
            returns=node.returns,
            lineno=node.lineno,
            col_offset=node.col_offset,
        )

        # Transform docstring if exists
        if orig_doc_string := ast.get_docstring(node):
            transformed_docstring = self.transform_docstring(orig_doc_string)

            # Create proper AST node for docstring
            docstring_node = ast.Expr(value=ast.Constant(value=transformed_docstring))
            new_node.body[0] = docstring_node

        # Transform return statements
        for i, stmt in enumerate(new_node.body):
            if isinstance(stmt, ast.Return) and stmt.value:
                # Wrap return value in await
                new_node.body[i] = ast.Return(value=ast.Await(value=stmt.value))

        return self.generic_visit(new_node)

    def transform_docstring(self, docstring: str) -> str:
        """Transform docstring for async version."""
        # replace ` = pgmq_client.<method_name>(` with ` = await pgmq_client.<method_name>_async(`
        # replace `time.sleep` with `await asyncio.sleep`
        modified = re.sub(r"(pgmq_client\.)(\w+)", r"await \1\2_async", docstring)
        modified = re.sub(r"time\.sleep\(", r"await asyncio.sleep(", docstring)
        return modified


def transform_to_async(
    transformer: AsyncFuncTransformer, method_info: MethodInfo
) -> MethodInfo:
    orig_sync_func_node = method_info.node
    async_node = copy.deepcopy(orig_sync_func_node)

    async_node = transformer.visit(async_node)
    async_node = ast.fix_missing_locations(async_node)

    return MethodInfo(f"{method_info.base_name}_async", async_node)


def get_async_methods_to_add(
    sync_methods: List[MethodInfo], missing_async: Set[str]
) -> Dict[str, MethodInfo]:
    transformer = AsyncFuncTransformer()
    async_methods: Dict[str, MethodInfo] = {}
    for method_info in sync_methods:
        if method_info.base_name in missing_async:
            async_methods[method_info.base_name] = transform_to_async(
                transformer, method_info
            )

    return async_methods
