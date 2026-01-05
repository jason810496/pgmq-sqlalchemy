import libcst as cst
import re
import sys
from pathlib import Path
from typing import List, Set, Dict

sys.path.insert(0, str(Path(__name__).parent.parent.joinpath("scripts").resolve()))


class TestInfo:
    """Information about a test function."""

    def __init__(self, name: str, node: cst.FunctionDef):
        self.name = name
        self.node = node
        self.is_test = name.startswith("test_")
        self.is_async = name.endswith("_async")
        self.base_name = name[:-6] if self.is_async else name


class ParseTestFunctionsVisitor(cst.CSTVisitor):
    """CST visitor to parse test functions from test module"""

    def __init__(self):
        self.tests: List[TestInfo] = []

    def visit_FunctionDef(self, node: cst.FunctionDef):
        func_name = node.name.value
        if func_name.startswith("test_"):
            self.tests.append(TestInfo(func_name, node))


class AsyncTestTransformer(cst.CSTTransformer):
    """Transform sync test to async test"""

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Transform method calls to async versions with await"""
        if isinstance(updated_node.func, cst.Attribute):
            # Check if calling method on pgmq object
            if isinstance(updated_node.func.value, cst.Name):
                var_name = updated_node.func.value.value
                # Transform pgmq.method() to await pgmq.method_async()
                if var_name == "pgmq":
                    method_name = updated_node.func.attr.value
                    # Add _async suffix to method name
                    new_attr = cst.Name(f"{method_name}_async")
                    new_func = updated_node.func.with_changes(attr=new_attr)
                    new_call = updated_node.with_changes(func=new_func)
                    # Wrap in await
                    return cst.Await(expression=new_call)
                # Transform time.sleep() to await asyncio.sleep()
                elif var_name == "time":
                    attr_name = updated_node.func.attr.value
                    if attr_name == "sleep":
                        # Change time.sleep to asyncio.sleep
                        new_func = updated_node.func.with_changes(
                            value=cst.Name("asyncio")
                        )
                        new_call = updated_node.with_changes(func=new_func)
                        # Wrap in await
                        return cst.Await(expression=new_call)

        return updated_node

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef:
        """Transform function to async and update name"""
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

    def transform_docstring(self, docstring: str) -> str:
        """Transform docstring for async test version."""
        # Replace pgmq.method( with await pgmq.method_async(
        modified = re.sub(r"(pgmq\.)(\w+)\(", r"await \1\2_async(", docstring)
        # Replace time.sleep with await asyncio.sleep
        modified = re.sub(r"time\.sleep\(", r"await asyncio.sleep(", modified)
        return modified


class FillMissingTestsToModule(cst.CSTTransformer):
    """CST Transformer to fill missing async tests to test module"""

    def __init__(self, to_add_async_tests: Dict[str, TestInfo]):
        self.to_add_async_tests = to_add_async_tests
        self.body_statements = []
        self.has_asyncio_import = False

    def visit_Module(self, node: cst.Module) -> bool:
        """Collect all statements and check for asyncio import"""
        self.body_statements = list(node.body)
        # Check if asyncio is already imported
        for stmt in node.body:
            if isinstance(stmt, cst.SimpleStatementLine):
                for item in stmt.body:
                    if isinstance(item, cst.Import):
                        for name in item.names:
                            if isinstance(name, cst.ImportAlias):
                                if (
                                    isinstance(name.name, cst.Name)
                                    and name.name.value == "asyncio"
                                ):
                                    self.has_asyncio_import = True
                                    break
        return True

    def leave_Module(
        self, original_node: cst.Module, updated_node: cst.Module
    ) -> cst.Module:
        """Add async tests after their sync counterparts and add asyncio import if needed"""
        new_body = []
        inserted_asyncio = False

        for stmt in updated_node.body:
            # Insert asyncio import after other imports if needed
            if not inserted_asyncio and not self.has_asyncio_import:
                if isinstance(stmt, cst.SimpleStatementLine):
                    # Check if this is the last import statement
                    is_import = any(
                        isinstance(item, (cst.Import, cst.ImportFrom))
                        for item in stmt.body
                    )
                    if is_import:
                        # Look ahead to see if next statement is not an import
                        current_idx = list(updated_node.body).index(stmt)
                        if current_idx + 1 < len(updated_node.body):
                            next_stmt = list(updated_node.body)[current_idx + 1]
                            if isinstance(next_stmt, cst.SimpleStatementLine):
                                next_is_import = any(
                                    isinstance(item, (cst.Import, cst.ImportFrom))
                                    for item in next_stmt.body
                                )
                                if not next_is_import:
                                    # This is the last import, add asyncio after it
                                    new_body.append(stmt)
                                    new_body.append(
                                        cst.SimpleStatementLine(
                                            body=[
                                                cst.Import(
                                                    names=[
                                                        cst.ImportAlias(
                                                            name=cst.Name("asyncio")
                                                        )
                                                    ]
                                                )
                                            ]
                                        )
                                    )
                                    inserted_asyncio = True
                                    continue
                        else:
                            # This is the last statement, add asyncio after it
                            new_body.append(stmt)
                            new_body.append(
                                cst.SimpleStatementLine(
                                    body=[
                                        cst.Import(
                                            names=[
                                                cst.ImportAlias(
                                                    name=cst.Name("asyncio")
                                                )
                                            ]
                                        )
                                    ]
                                )
                            )
                            inserted_asyncio = True
                            continue

            new_body.append(stmt)
            # If this is a sync test function, check if we need to add async version after it
            if isinstance(stmt, cst.FunctionDef):
                func_name = stmt.name.value
                if func_name in self.to_add_async_tests:
                    # Add blank line before async test
                    new_body.append(cst.EmptyLine(whitespace=cst.SimpleWhitespace("")))
                    new_body.append(cst.EmptyLine(whitespace=cst.SimpleWhitespace("")))
                    # Add the async version right after the sync version
                    new_body.append(self.to_add_async_tests[func_name].node)

        return updated_node.with_changes(body=new_body)


def parse_test_functions_from_module(
    module_tree: cst.Module,
) -> tuple[List[TestInfo], Set[str]]:
    """
    Parse test functions from test module CST Tree

    Args:
        module_tree: cst.Module

    Returns:
        Tuple of all_tests, missing_async_set
    """

    analyzer = ParseTestFunctionsVisitor()
    module_tree.visit(analyzer)

    # Categorize test functions
    async_tests_set = set()
    missing_async_set = set()

    for test_info in analyzer.tests:
        if test_info.is_async:
            async_tests_set.add(test_info.base_name)

    # Find missing async tests
    for test_info in analyzer.tests:
        if not test_info.is_async and test_info.base_name not in async_tests_set:
            missing_async_set.add(test_info.base_name)

    return analyzer.tests, missing_async_set


def transform_to_async_test(
    transformer: AsyncTestTransformer, test_info: TestInfo
) -> TestInfo:
    """Transform a sync test to async test"""
    orig_sync_func_node = test_info.node
    async_node = orig_sync_func_node.visit(transformer)

    return TestInfo(f"{test_info.base_name}_async", async_node)


def get_async_tests_to_add(
    sync_tests: List[TestInfo], missing_async: Set[str]
) -> Dict[str, TestInfo]:
    """Generate async test functions for missing tests"""
    transformer = AsyncTestTransformer()
    async_tests: Dict[str, TestInfo] = {}

    for test_info in sync_tests:
        if test_info.name in missing_async and not test_info.is_async:
            async_tests[test_info.name] = transform_to_async_test(
                transformer, test_info
            )

    return async_tests


def fill_missing_tests_to_module(
    module_tree: cst.Module, to_add_async_tests: Dict[str, TestInfo]
) -> cst.Module:
    """Fill missing async tests to module"""
    transformer = FillMissingTestsToModule(to_add_async_tests=to_add_async_tests)
    return module_tree.visit(transformer)
