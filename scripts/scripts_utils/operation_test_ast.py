import libcst as cst
from typing import Dict, Set, List, Tuple
from scripts_utils.common_ast import MethodInfo


class AsyncTestTransformer(cst.CSTTransformer):
    """Transform sync test functions to async test functions."""

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        """Transform function to async test."""
        # Change function name from _sync to _async
        new_name = updated_node.name.value.replace("_sync", "_async")

        # Add async keyword
        new_node = updated_node.with_changes(
            asynchronous=cst.Asynchronous(), name=cst.Name(new_name)
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
                        # For concatenated strings, skip transformation
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

    def leave_Param(
        self, original_node: cst.Param, updated_node: cst.Param
    ) -> cst.Param:
        """Transform function parameters to use async fixtures."""
        param_name = updated_node.name.value

        if param_name == "get_session_maker":
            return updated_node.with_changes(name=cst.Name("get_async_session_maker"))
        if param_name == "db_session":
            return updated_node.with_changes(name=cst.Name("async_db_session"))
        elif param_name == "pgmq_setup_teardown":
            return updated_node.with_changes(name=cst.Name("async_pgmq_setup_teardown"))
        elif param_name == "pgmq_partitioned_setup_teardown":
            return updated_node.with_changes(
                name=cst.Name("async_pgmq_partitioned_setup_teardown")
            )

        return updated_node

    def leave_With(self, original_node: cst.With, updated_node: cst.With) -> cst.With:
        """Transform 'with' statements to 'async with'."""
        # Check if this is a session context manager
        for item in updated_node.items:
            if isinstance(item.item, cst.Call):
                if isinstance(item.item.func, cst.Name):
                    if "session_maker" in item.item.func.value:
                        # Transform to async with
                        return updated_node.with_changes(
                            asynchronous=cst.Asynchronous()
                        )

        return updated_node

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Transform method calls to add _async suffix and await."""
        # Check if this is a PGMQOperation method call
        if isinstance(updated_node.func, cst.Attribute):
            if isinstance(updated_node.func.value, cst.Name):
                if updated_node.func.value.value == "PGMQOperation":
                    # Add _async suffix to method name
                    new_func = updated_node.func.with_changes(
                        attr=cst.Name(f"{updated_node.func.attr.value}_async")
                    )
                    return updated_node.with_changes(func=new_func)

        # Check if this is a function call
        if isinstance(updated_node.func, cst.Name):
            if updated_node.func.value == "get_session_maker":
                # Replace with get_async_session_maker
                return updated_node.with_changes(
                    func=cst.Name("get_async_session_maker")
                )
            elif updated_node.func.value == "check_queue_exists":
                # Replace with check_queue_exists_async and update db_session arg
                new_call = updated_node.with_changes(
                    func=cst.Name("check_queue_exists_async")
                )
                # Also need to update the db_session argument to async_db_session
                if updated_node.args:
                    new_args = []
                    for arg in updated_node.args:
                        if (
                            isinstance(arg.value, cst.Name)
                            and arg.value.value == "db_session"
                        ):
                            new_args.append(
                                arg.with_changes(value=cst.Name("async_db_session"))
                            )
                        else:
                            new_args.append(arg)
                    new_call = new_call.with_changes(args=new_args)
                # Wrap in await
                return cst.Await(expression=new_call)

        return updated_node

    def leave_Assign(
        self, original_node: cst.Assign, updated_node: cst.Assign
    ) -> cst.Assign:
        """Add await to assignments that call async methods."""
        # Check if the value is a PGMQOperation call
        if isinstance(updated_node.value, cst.Call):
            if isinstance(updated_node.value.func, cst.Attribute):
                if isinstance(updated_node.value.func.value, cst.Name):
                    if updated_node.value.func.value.value == "PGMQOperation":
                        # Wrap in await
                        return updated_node.with_changes(
                            value=cst.Await(expression=updated_node.value)
                        )
                    # Check if this is a session method call (session.commit, session.rollback, etc.)
                    elif updated_node.value.func.value.value == "session":
                        # Wrap in await
                        return updated_node.with_changes(
                            value=cst.Await(expression=updated_node.value)
                        )

        return updated_node

    def leave_Expr(self, original_node: cst.Expr, updated_node: cst.Expr) -> cst.Expr:
        """Add await to expression statements that call async methods."""
        # Check if this is a PGMQOperation call (not in assignment)
        if isinstance(updated_node.value, cst.Call):
            if isinstance(updated_node.value.func, cst.Attribute):
                if isinstance(updated_node.value.func.value, cst.Name):
                    if updated_node.value.func.value.value == "PGMQOperation":
                        # Wrap in await
                        return updated_node.with_changes(
                            value=cst.Await(expression=updated_node.value)
                        )
                    # Check if this is a session method call (session.commit, session.rollback, etc.)
                    elif updated_node.value.func.value.value == "session":
                        # Wrap in await
                        return updated_node.with_changes(
                            value=cst.Await(expression=updated_node.value)
                        )

        return updated_node

    def transform_docstring(self, docstring: str) -> str:
        """Transform docstring for async version."""
        # Replace 'synchronously' with 'asynchronously'
        modified = docstring.replace(
            "using PGMQOperation.", "using PGMQOperation asynchronously."
        )

        # Add 'asynchronously' before the period if not already present
        if "asynchronously" not in modified and not modified.endswith(
            "asynchronously."
        ):
            modified = modified.rstrip(".")
            if modified and not modified.endswith("asynchronously"):
                modified += " asynchronously."

        return modified


class TestFunctionVisitor(cst.CSTVisitor):
    """Visitor to collect test functions from a module."""

    def __init__(self):
        self.test_functions: List[MethodInfo] = []

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        """Visit function definitions and collect test functions."""
        func_name = node.name.value
        if func_name.startswith("test_"):
            # Determine if it's async or sync
            is_async = func_name.endswith("_async")
            base_name = func_name[:-6] if is_async else func_name

            method_info = MethodInfo(func_name, node)
            method_info.is_target = True
            method_info.is_async = is_async
            method_info.base_name = base_name

            self.test_functions.append(method_info)


class FillMissingTestsTransformer(cst.CSTTransformer):
    """Transformer to add missing async tests after their sync counterparts."""

    def __init__(self, to_add_async_tests: Dict[str, MethodInfo]):
        self.to_add_async_tests = to_add_async_tests
        self.added_decorators = False

    def leave_Module(
        self, original_node: cst.Module, updated_node: cst.Module
    ) -> cst.Module:
        """Transform the module to add missing async tests."""
        new_body = []

        for stmt in updated_node.body:
            new_body.append(stmt)

            # If this is a sync test function, check if we need to add async version
            if isinstance(stmt, cst.FunctionDef):
                func_name = stmt.name.value
                if func_name in self.to_add_async_tests:
                    # Add decorator before async test
                    decorator = cst.Decorator(
                        decorator=cst.Attribute(
                            value=cst.Attribute(
                                value=cst.Name("pytest"), attr=cst.Name("mark")
                            ),
                            attr=cst.Name("asyncio"),
                        )
                    )

                    async_test = self.to_add_async_tests[func_name].node

                    # Add decorator to async test
                    if async_test.decorators:
                        decorated_async = async_test.with_changes(
                            decorators=[decorator] + list(async_test.decorators)
                        )
                    else:
                        decorated_async = async_test.with_changes(
                            decorators=[decorator]
                        )

                    # Add empty line before async test for readability
                    new_body.append(
                        cst.EmptyLine(indent=False, whitespace=cst.SimpleWhitespace(""))
                    )
                    new_body.append(
                        cst.EmptyLine(indent=False, whitespace=cst.SimpleWhitespace(""))
                    )
                    new_body.append(decorated_async)

        return updated_node.with_changes(body=new_body)


def parse_test_functions_from_module(
    module_tree: cst.Module,
) -> Tuple[List[MethodInfo], Set[str]]:
    """
    Parse test functions from module.

    Returns:
        Tuple of (all_test_functions, missing_async_test_names)
    """
    visitor = TestFunctionVisitor()
    module_tree.visit(visitor)

    # Categorize tests
    async_tests_set = set()
    missing_async_set = set()

    for test_info in visitor.test_functions:
        if not test_info.is_target:
            continue

        if test_info.is_async:
            # Extract base name without _async suffix
            base_name = test_info.name.replace("_async", "")
            async_tests_set.add(base_name)

    # Find missing async tests
    for test_info in visitor.test_functions:
        if not test_info.is_target:
            continue

        # Check if this is a sync test
        if test_info.name.endswith("_sync"):
            # Get base name without _sync suffix
            base_name_without_sync = test_info.name.replace("_sync", "")
            # Check if async version exists
            if base_name_without_sync not in async_tests_set:
                missing_async_set.add(test_info.name)  # Store full sync name

    return visitor.test_functions, missing_async_set


def transform_test_to_async(test_info: MethodInfo) -> MethodInfo:
    """Transform a sync test function to async."""
    transformer = AsyncTestTransformer()
    async_node = test_info.node.visit(transformer)

    new_name = test_info.name.replace("_sync", "_async")
    return MethodInfo(new_name, async_node)


def get_async_tests_to_add(
    all_tests: List[MethodInfo], missing_async: Set[str]
) -> Dict[str, MethodInfo]:
    """
    Generate async tests for missing ones.

    Args:
        all_tests: All test functions found
        missing_async: Set of sync test names that need async versions

    Returns:
        Dictionary mapping sync test name to async MethodInfo
    """
    async_tests: Dict[str, MethodInfo] = {}

    for test_info in all_tests:
        if test_info.name in missing_async:
            async_tests[test_info.name] = transform_test_to_async(test_info)

    return async_tests


def fill_missing_tests_to_module(
    module_tree: cst.Module, to_add_async_tests: Dict[str, MethodInfo]
) -> cst.Module:
    """Fill missing async tests into the module."""
    transformer = FillMissingTestsTransformer(to_add_async_tests)
    return module_tree.visit(transformer)
