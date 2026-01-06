import libcst as cst
import re
import sys
from pathlib import Path
from typing import List, Set, Dict, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent.joinpath("scripts").resolve()))


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

    def leave_With(self, original_node: cst.With, updated_node: cst.With) -> cst.With:
        """Transform 'with get_session_maker' to 'async with get_async_session_maker'"""
        # Check if any with item uses get_session_maker or get_async_session_maker
        # (get_async_session_maker might already be transformed by leave_Name)
        new_items = []
        has_session_maker = False

        for item in updated_node.items:
            if isinstance(item.item, cst.Call):
                if isinstance(item.item.func, cst.Name):
                    func_name = item.item.func.value
                    if func_name in ("get_session_maker", "get_async_session_maker"):
                        has_session_maker = True
                        # Ensure it's get_async_session_maker
                        if func_name == "get_session_maker":
                            new_call = item.item.with_changes(
                                func=cst.Name("get_async_session_maker")
                            )
                            new_items.append(item.with_changes(item=new_call))
                        else:
                            new_items.append(item)
                        continue
            new_items.append(item)

        # Only make it async if it uses session maker
        if has_session_maker:
            return updated_node.with_changes(
                asynchronous=cst.Asynchronous(), items=new_items
            )
        else:
            # Keep as regular with statement for other cases (like pytest.raises)
            return updated_node.with_changes(items=new_items)

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

        # Transform check_queue_exists to await check_queue_exists_async
        if isinstance(updated_node.func, cst.Name):
            if updated_node.func.value == "check_queue_exists":
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

    def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
        """Transform variable name references to use async fixture names."""
        name = updated_node.value

        # Transform fixture references in function body
        if name == "pgmq_setup_teardown":
            return updated_node.with_changes(value="async_pgmq_setup_teardown")
        elif name == "pgmq_partitioned_setup_teardown":
            return updated_node.with_changes(
                value="async_pgmq_partitioned_setup_teardown"
            )
        elif name == "get_session_maker":
            return updated_node.with_changes(value="get_async_session_maker")
        elif name == "db_session":
            return updated_node.with_changes(value="async_db_session")

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

    def __init__(self, to_add_async_tests: Dict[str, Tuple[TestInfo, TestInfo]]):
        self.to_add_async_tests = to_add_async_tests
        self.body_statements = []
        self.has_asyncio_import = False
        self.has_check_queue_exists_async_import = False

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
                    # Check for check_queue_exists_async import
                    if isinstance(item, cst.ImportFrom):
                        if (
                            isinstance(item.module, cst.Attribute)
                            and isinstance(item.module.value, cst.Name)
                            and item.module.value.value == "tests"
                            and isinstance(item.module.attr, cst.Name)
                            and item.module.attr.value == "_utils"
                        ):
                            if isinstance(item.names, cst.ImportStar):
                                self.has_check_queue_exists_async_import = True
                            else:
                                for name in item.names:
                                    if isinstance(name, cst.ImportAlias):
                                        if (
                                            isinstance(name.name, cst.Name)
                                            and name.name.value
                                            == "check_queue_exists_async"
                                        ):
                                            self.has_check_queue_exists_async_import = (
                                                True
                                            )
                                            break
        return True

    def leave_Module(
        self, original_node: cst.Module, updated_node: cst.Module
    ) -> cst.Module:
        """Add async tests after their sync counterparts and add asyncio import if needed"""
        new_body = []
        inserted_asyncio = False
        updated_check_queue_exists_import = False
        updated_fixture_imports = False
        updated_use_fixtures = False
        seen_imports_from_utils = False

        for stmt in updated_node.body:
            # This section is removed - we'll add async tests after sync tests in the main loop below

            # Update fixture imports from tests.fixture_deps
            if not updated_fixture_imports and isinstance(
                stmt, cst.SimpleStatementLine
            ):
                for item in stmt.body:
                    if isinstance(item, cst.ImportFrom):
                        if (
                            isinstance(item.module, cst.Attribute)
                            and isinstance(item.module.value, cst.Name)
                            and item.module.value.value == "tests"
                            and isinstance(item.module.attr, cst.Name)
                            and item.module.attr.value == "fixture_deps"
                        ):
                            # Add async fixture imports if needed
                            if not isinstance(item.names, cst.ImportStar):
                                has_pgmq_setup_teardown = False
                                has_async_pgmq_setup_teardown = False
                                has_pgmq_partitioned = False
                                has_async_pgmq_partitioned = False

                                for name in item.names:
                                    if isinstance(name, cst.ImportAlias) and isinstance(
                                        name.name, cst.Name
                                    ):
                                        if name.name.value == "pgmq_setup_teardown":
                                            has_pgmq_setup_teardown = True
                                        elif (
                                            name.name.value
                                            == "async_pgmq_setup_teardown"
                                        ):
                                            has_async_pgmq_setup_teardown = True
                                        elif (
                                            name.name.value
                                            == "pgmq_partitioned_setup_teardown"
                                        ):
                                            has_pgmq_partitioned = True
                                        elif (
                                            name.name.value
                                            == "async_pgmq_partitioned_setup_teardown"
                                        ):
                                            has_async_pgmq_partitioned = True

                                # Add async versions if sync versions exist but async don't
                                new_names = list(item.names)
                                if (
                                    has_pgmq_setup_teardown
                                    and not has_async_pgmq_setup_teardown
                                ):
                                    new_names.append(
                                        cst.ImportAlias(
                                            name=cst.Name("async_pgmq_setup_teardown")
                                        )
                                    )
                                if (
                                    has_pgmq_partitioned
                                    and not has_async_pgmq_partitioned
                                ):
                                    new_names.append(
                                        cst.ImportAlias(
                                            name=cst.Name(
                                                "async_pgmq_partitioned_setup_teardown"
                                            )
                                        )
                                    )

                                if len(new_names) > len(item.names):
                                    new_import = item.with_changes(names=new_names)
                                    new_stmt = stmt.with_changes(body=[new_import])
                                    new_body.append(new_stmt)
                                    updated_fixture_imports = True
                                    continue

            # Update check_queue_exists import to include check_queue_exists_async
            if isinstance(stmt, cst.SimpleStatementLine):
                for item in stmt.body:
                    if isinstance(item, cst.ImportFrom):
                        if (
                            isinstance(item.module, cst.Attribute)
                            and isinstance(item.module.value, cst.Name)
                            and item.module.value.value == "tests"
                            and isinstance(item.module.attr, cst.Name)
                            and item.module.attr.value == "_utils"
                        ):
                            # Skip duplicate imports from tests._utils
                            if seen_imports_from_utils:
                                continue

                            # Check if check_queue_exists is imported
                            if not isinstance(item.names, cst.ImportStar):
                                has_check_queue_exists = False
                                has_check_queue_exists_async = False
                                for name in item.names:
                                    if isinstance(name, cst.ImportAlias):
                                        if isinstance(name.name, cst.Name):
                                            if name.name.value == "check_queue_exists":
                                                has_check_queue_exists = True
                                            elif (
                                                name.name.value
                                                == "check_queue_exists_async"
                                            ):
                                                has_check_queue_exists_async = True

                                # If check_queue_exists is imported but not check_queue_exists_async, add it
                                if not updated_check_queue_exists_import:
                                    if (
                                        has_check_queue_exists
                                        and not has_check_queue_exists_async
                                    ):
                                        new_names = list(item.names)
                                        new_names.append(
                                            cst.ImportAlias(
                                                name=cst.Name(
                                                    "check_queue_exists_async"
                                                )
                                            )
                                        )
                                        new_import = item.with_changes(names=new_names)
                                        new_stmt = stmt.with_changes(body=[new_import])
                                        new_body.append(new_stmt)
                                        updated_check_queue_exists_import = True
                                        seen_imports_from_utils = True
                                        continue
                                    else:
                                        # Import already has both or is fine as-is
                                        new_body.append(stmt)
                                        seen_imports_from_utils = True
                                        continue

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

            # Update use_fixtures list to include async versions
            if not updated_use_fixtures and isinstance(stmt, cst.SimpleStatementLine):
                for item in stmt.body:
                    if isinstance(item, cst.Assign):
                        for target in item.targets:
                            if (
                                isinstance(target.target, cst.Name)
                                and target.target.value == "use_fixtures"
                            ):
                                # Check if value is a list
                                if isinstance(item.value, cst.List):
                                    has_pgmq_setup_teardown = False
                                    has_async_pgmq_setup_teardown = False
                                    has_pgmq_partitioned = False
                                    has_async_pgmq_partitioned = False

                                    for elem in item.value.elements:
                                        if isinstance(elem.value, cst.Name):
                                            if (
                                                elem.value.value
                                                == "pgmq_setup_teardown"
                                            ):
                                                has_pgmq_setup_teardown = True
                                            elif (
                                                elem.value.value
                                                == "async_pgmq_setup_teardown"
                                            ):
                                                has_async_pgmq_setup_teardown = True
                                            elif (
                                                elem.value.value
                                                == "pgmq_partitioned_setup_teardown"
                                            ):
                                                has_pgmq_partitioned = True
                                            elif (
                                                elem.value.value
                                                == "async_pgmq_partitioned_setup_teardown"
                                            ):
                                                has_async_pgmq_partitioned = True

                                    # Add async versions to the list
                                    new_elements = list(item.value.elements)
                                    if (
                                        has_pgmq_setup_teardown
                                        and not has_async_pgmq_setup_teardown
                                    ):
                                        new_elements.append(
                                            cst.Element(
                                                value=cst.Name(
                                                    "async_pgmq_setup_teardown"
                                                )
                                            )
                                        )
                                    if (
                                        has_pgmq_partitioned
                                        and not has_async_pgmq_partitioned
                                    ):
                                        new_elements.append(
                                            cst.Element(
                                                value=cst.Name(
                                                    "async_pgmq_partitioned_setup_teardown"
                                                )
                                            )
                                        )

                                    if len(new_elements) > len(item.value.elements):
                                        new_list = item.value.with_changes(
                                            elements=new_elements
                                        )
                                        new_assign = item.with_changes(value=new_list)
                                        new_stmt = stmt.with_changes(body=[new_assign])
                                        new_body.append(new_stmt)
                                        updated_use_fixtures = True
                                        continue

            new_body.append(stmt)
            # If this is a sync test function, check if we need to add async versions after it
            if isinstance(stmt, cst.FunctionDef):
                func_name = stmt.name.value
                if func_name in self.to_add_async_tests:
                    (
                        async_test_with_decorator,
                        async_test_without_decorator,
                    ) = self.to_add_async_tests[func_name]

                    # Add decorator for first version
                    decorator = cst.Decorator(
                        decorator=cst.Attribute(
                            value=cst.Attribute(
                                value=cst.Name("pytest"), attr=cst.Name("mark")
                            ),
                            attr=cst.Name("asyncio"),
                        )
                    )

                    # First async test WITH decorator
                    async_test_node = async_test_with_decorator.node
                    if async_test_node.decorators:
                        decorated_async = async_test_node.with_changes(
                            decorators=[decorator] + list(async_test_node.decorators)
                        )
                    else:
                        decorated_async = async_test_node.with_changes(
                            decorators=[decorator]
                        )

                    # Add blank lines and first async test (with decorator)
                    new_body.append(cst.EmptyLine(whitespace=cst.SimpleWhitespace("")))
                    new_body.append(cst.EmptyLine(whitespace=cst.SimpleWhitespace("")))
                    new_body.append(decorated_async)

        return updated_node.with_changes(body=new_body)


def parse_test_functions_from_module(
    module_tree: cst.Module,
) -> Tuple[List[TestInfo], Set[str]]:
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
    """Transform a sync test to async test.

    Args:
        transformer: AsyncTestTransformer instance for CST transformations
        test_info: TestInfo object containing sync test metadata

    Returns:
        TestInfo object with transformed async test function
    """
    orig_sync_func_node = test_info.node
    async_node = orig_sync_func_node.visit(transformer)

    return TestInfo(f"{test_info.base_name}_async", async_node)


def get_async_tests_to_add(
    sync_tests: List[TestInfo], missing_async: Set[str]
) -> Dict[str, Tuple[TestInfo, TestInfo]]:
    """Generate async test functions for missing tests.

    Returns a dict mapping sync test name to a tuple of (async_test_with_decorator, async_test_without_decorator)
    """
    transformer = AsyncTestTransformer()
    async_tests: Dict[str, Tuple[TestInfo, TestInfo]] = {}

    for test_info in sync_tests:
        if test_info.name in missing_async and not test_info.is_async:
            # Generate the async test
            async_test = transform_to_async_test(transformer, test_info)
            # Store both versions (we'll add decorators later in the transformer)
            async_tests[test_info.name] = (async_test, async_test)

    return async_tests


def fill_missing_tests_to_module(
    module_tree: cst.Module, to_add_async_tests: Dict[str, Tuple[TestInfo, TestInfo]]
) -> cst.Module:
    """Fill missing async tests to module"""
    transformer = FillMissingTestsToModule(to_add_async_tests=to_add_async_tests)
    return module_tree.visit(transformer)
