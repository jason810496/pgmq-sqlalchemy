import libcst as cst
import re
import sys
from pathlib import Path
from typing import List, Set, Dict

sys.path.insert(0, str(Path(__file__).parent.parent.joinpath("scripts").resolve()))

from scripts_utils.common_ast import MethodInfo  # noqa: E402


class AsyncOperationTransformer(cst.CSTTransformer):
    """Transform sync PGMQOperation methods to async versions."""

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        """Transform session.execute() and session.commit() calls to await."""
        # Check if this is a session.execute() or session.commit() call
        if isinstance(updated_node.func, cst.Attribute):
            if isinstance(updated_node.func.value, cst.Name):
                if updated_node.func.value.value == "session":
                    if updated_node.func.attr.value in ["execute", "commit"]:
                        # Wrap in await
                        return cst.Await(expression=updated_node)
        
        return updated_node

    def leave_FunctionDef(self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef) -> cst.FunctionDef:
        """Transform function to async version."""
        # Transform function to async
        new_node = updated_node.with_changes(
            asynchronous=cst.Asynchronous(),
            name=cst.Name(f"{updated_node.name.value}_async")
        )

        # Update parameters - change Session to AsyncSession
        if updated_node.params:
            new_params = self._transform_params(updated_node.params)
            new_node = new_node.with_changes(params=new_params)

        # Transform docstring if exists
        if updated_node.body.body and isinstance(updated_node.body.body[0], cst.SimpleStatementLine):
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
                    
                    if docstring and len(docstring) >= 2:
                        # Remove quotes to get actual string content
                        if len(docstring) >= 6 and (docstring.startswith('"""') or docstring.startswith("'''")):
                            quote = docstring[:3]
                            content = docstring[3:-3]
                        elif len(docstring) >= 2 and (docstring.startswith('"') or docstring.startswith("'")):
                            quote = docstring[0]
                            content = docstring[1:-1]
                        else:
                            content = docstring
                            quote = '"""'
                        
                        transformed_content = self.transform_docstring(content)
                        new_docstring = f'{quote}{transformed_content}{quote}'
                        
                        # Create new docstring node
                        new_expr = expr.with_changes(value=cst.SimpleString(new_docstring))
                        new_first_stmt = first_stmt.with_changes(body=[new_expr])
                        
                        # Update body with new docstring
                        new_body = [new_first_stmt] + list(updated_node.body.body[1:])
                        new_node = new_node.with_changes(
                            body=new_node.body.with_changes(body=new_body)
                        )

        return new_node

    def _transform_params(self, params: cst.Parameters) -> cst.Parameters:
        """Transform parameters - change Session to AsyncSession."""
        new_kwonly_params = []
        
        if params.kwonly_params:
            for param in params.kwonly_params:
                if param.annotation:
                    # Check if annotation is Session
                    if isinstance(param.annotation.annotation, cst.Name):
                        if param.annotation.annotation.value == "Session":
                            # Replace Session with AsyncSession
                            new_annotation = param.annotation.with_changes(
                                annotation=cst.Name("AsyncSession")
                            )
                            new_param = param.with_changes(annotation=new_annotation)
                            new_kwonly_params.append(new_param)
                            continue
                new_kwonly_params.append(param)
        
        return params.with_changes(kwonly_params=new_kwonly_params)

    def transform_docstring(self, docstring: str) -> str:
        """Transform docstring for async version."""
        # Replace references to sync version with async version
        modified = docstring
        
        # Change method description to indicate it's async
        if "asynchronously" not in modified.lower() and "(async)" not in modified.lower():
            # Add async indication at the end of first sentence if not present
            modified = re.sub(
                r'(^[^.]+\.)',
                r'\1 (async)',
                modified,
                count=1
            )
            # Or if that didn't work, try to add it more explicitly
            if "(async)" not in modified:
                # Replace session reference
                modified = re.sub(
                    r'SQLAlchemy session\.',
                    r'Async SQLAlchemy session.',
                    modified
                )
                modified = re.sub(
                    r'session: SQLAlchemy session',
                    r'session: Async SQLAlchemy session',
                    modified
                )
        
        return modified


def transform_to_async_operation(
    transformer: AsyncOperationTransformer, method_info: MethodInfo
) -> MethodInfo:
    """Transform a sync method to async for PGMQOperation."""
    orig_sync_func_node = method_info.node
    async_node = orig_sync_func_node.visit(transformer)
    
    return MethodInfo(f"{method_info.base_name}_async", async_node)


def get_async_methods_to_add(
    sync_methods: List[MethodInfo], missing_async: Set[str]
) -> Dict[str, MethodInfo]:
    """Get async methods to add for missing sync methods."""
    transformer = AsyncOperationTransformer()
    async_methods: Dict[str, MethodInfo] = {}
    for method_info in sync_methods:
        if method_info.base_name in missing_async:
            async_methods[method_info.base_name] = transform_to_async_operation(
                transformer, method_info
            )
    
    return async_methods
