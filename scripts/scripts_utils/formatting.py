import subprocess
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__name__).parent.parent.joinpath("scripts").resolve()))


def format_file(file_path: str) -> bool:
    try:
        ruff_stdout = subprocess.check_output(["ruff", "format", file_path]).decode()
    except Exception as e:
        raise e

    return "unchanged" in ruff_stdout


def compare_file(existed_file: str, new_file: str):
    try:
        subprocess.check_call(
            ["git", "difftool", "--tool=vimdiff", "--no-index", existed_file, new_file]
        )
    except Exception as e:
        raise e
