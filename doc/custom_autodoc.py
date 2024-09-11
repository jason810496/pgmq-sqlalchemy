from sphinx.application import Sphinx
from sphinx.ext.autodoc import Options
from typing import List, Any, Set
import re

from doc.link_docstring import docstring_definition

_existed_substitution: Set[str] = set()
_existed_link: Set[str] = set()


def override_docstring(
    app: Sphinx, what: str, name: str, obj: Any, options: Options, lines: List[str]
) -> None:
    """
    Custom function to override docstrings dynamically.

    :param app: The Sphinx application object.
    :param what: The type of the object (e.g., 'module', 'class', 'function').
    :param name: The fully qualified name of the object.
    :param obj: The object itself (e.g., function or class).
    :param options: The options given to the directive.
    :param lines: The lines of the docstring as a list of strings.
    """
    # Dynamically replace the docstring for a specific function
    group_name = name.split(".")[-1]
    if group_name in docstring_definition:
        docstring_before = docstring_definition[group_name]["docstring_before"]
        docstring_after = docstring_definition[group_name]["docstring_after"]
        new_docstring = (
            docstring_before + "\n" + "\n".join(lines) + "\n" + docstring_after
        )
        lines.clear()
        lines.extend(new_docstring.strip().splitlines())
    # Check substitution existed before, if existed: skip the line
    substitution_pattern = r"\.\. \|([^|]+)\|"
    lines[:] = [
        line
        for line in lines
        if not re.match(substitution_pattern, line)
        or re.match(substitution_pattern, line).group(1) not in _existed_substitution
    ]
    _existed_substitution.update(re.findall(substitution_pattern, "\n".join(lines)))

    # Check link existed before, if existed: skip the line
    link_pattern = r"^\.\. _([^:]+):"
    lines[:] = [
        line
        for line in lines
        if not re.match(link_pattern, line)
        or re.match(link_pattern, line).group(1) not in _existed_link
    ]
    _existed_link.update(re.findall(link_pattern, "\n".join(lines)))


def setup(app: Sphinx) -> None:
    app.connect("autodoc-process-docstring", override_docstring)
