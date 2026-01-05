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
