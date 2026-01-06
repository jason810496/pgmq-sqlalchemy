from rich.console import Console
from rich.theme import Theme


console = Console(
    force_terminal=True,
    color_system="standard",
    theme=Theme(
        {
            "success": "green",
            "info": "bright_blue",
            "warning": "bright_yellow",
            "error": "red",
            "special": "magenta",
        }
    ),
    width=202,
)


def user_input(msg: str) -> bool:
    console.log(f"{msg} (Y/n)")
    s = input()
    if len(s) != 1:
        return False

    return s.lower() == "y"
