import libcst as cst
import re
import sys
from pathlib import Path
from typing import List, Set, Dict


sys.path.insert(0, str(Path(__name__).parent.parent.joinpath("scripts").resolve()))

from scripts_utils.common_ast import MethodInfo  # noqa: E402


class AsyncFuncTransformer(cst.CSTTransformer):
    to_replace_execute_func_attr: str = "_execute_operation"
    target_execute_func_attr: str = "_execute_async_operation"

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        # Handle PGMQOperation.method calls
        if isinstance(updated_node.func, cst.Attribute):
            # Check if any argument is PGMQOperation.method
            new_args = []
            for arg in updated_node.args:
                if isinstance(arg.value, cst.Attribute) and isinstance(
                    arg.value.value, cst.Name
                ):
                    if arg.value.value.value == "PGMQOperation":
                        # Add _async suffix to method name
                        new_attr = arg.value.with_changes(
                            attr=cst.Name(f"{arg.value.attr.value}_async")
                        )
                        new_args.append(arg.with_changes(value=new_attr))
                        continue
                new_args.append(arg)

            # Replace `self._execute_operation` to `self._execute_async_operation`
            if isinstance(updated_node.func.value, cst.Name):
                if (
                    updated_node.func.value.value == "self"
                    and updated_node.func.attr.value
                    == self.to_replace_execute_func_attr
                ):
                    updated_node = updated_node.with_changes(
                        func=updated_node.func.with_changes(
                            attr=cst.Name(self.target_execute_func_attr)
                        )
                    )

            if new_args:
                updated_node = updated_node.with_changes(args=new_args)

        return updated_node

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        # Transform function to async
        new_node = updated_node.with_changes(
            asynchronous=cst.Asynchronous(),
            name=cst.Name(f"{updated_node.name.value}_async"),
        )

        # Transform docstring if exists
        if updated_node.body.body and isinstance(
            updated_node.body.body[0], cst.SimpleStatementLine
        ):
            first_stmt = updated_node.body.body[0]
            if first_stmt.body and isinstance(first_stmt.body[0], cst.Expr):
                expr = first_stmt.body[0]
                if isinstance(expr.value, (cst.SimpleString, cst.ConcatenatedString)):
                    # Extract docstring value
                    if isinstance(expr.value, cst.SimpleString):
                        docstring = expr.value.value
                    else:
                        # For concatenated strings, we'll skip transformation for now
                        docstring = None
                    if docstring:
                        # Remove quotes to get actual string content
                        if docstring.startswith('"""') or docstring.startswith("'''"):
                            quote = docstring[:3]
                            content = docstring[3:-3]
                        elif docstring.startswith('"') or docstring.startswith("'"):
                            quote = docstring[0]
                            content = docstring[1:-1]
                        else:
                            content = docstring
                            quote = '"""'

                        transformed_content = self.transform_docstring(content)
                        new_docstring = f"{quote}{transformed_content}{quote}"

                        # Create new docstring node
                        new_expr = expr.with_changes(
                            value=cst.SimpleString(new_docstring)
                        )
                        new_first_stmt = first_stmt.with_changes(body=[new_expr])

                        # Update body with new docstring
                        new_body = [new_first_stmt] + list(updated_node.body.body[1:])
                        new_node = new_node.with_changes(
                            body=new_node.body.with_changes(body=new_body)
                        )

        return new_node

    def leave_Return(
        self, original_node: cst.Return, updated_node: cst.Return
    ) -> cst.Return:
        # Only wrap return value in await if it's a call expression
        # (which is likely to be an operation that needs awaiting)
        if updated_node.value and isinstance(updated_node.value, cst.Call):
            return updated_node.with_changes(
                value=cst.Await(expression=updated_node.value)
            )
        return updated_node

    def transform_docstring(self, docstring: str) -> str:
        """Transform docstring for async version."""
        # replace ` = pgmq_client.<method_name>(` with ` = await pgmq_client.<method_name>_async(`
        # replace `time.sleep` with `await asyncio.sleep`
        modified = re.sub(r"(pgmq_client\.)(\w+)", r"await \1\2_async", docstring)
        modified = re.sub(r"time\.sleep\(", r"await asyncio.sleep(", modified)
        return modified


def transform_to_async(
    transformer: AsyncFuncTransformer, method_info: MethodInfo
) -> MethodInfo:
    orig_sync_func_node = method_info.node
    # Deep copy is handled by libcst internally during transformation
    async_node = orig_sync_func_node.visit(transformer)

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
