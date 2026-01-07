from typing import Callable, ParamSpec

import time
import subprocess
import sys
import os
import fcntl
from collections import deque
from dataclasses import dataclass

from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

PS = ParamSpec("PS")


@dataclass
class CmdArg:
    cmd: list[str]
    panel_title: str

    @property
    def layout_name(self):
        return "_".join(self.cmd) + "_" + self.panel_title


class ProcessInstance:
    def __init__(self, cmd_arg: CmdArg, console: Console):
        console.print(f"[yellow]Starting {cmd_arg.panel_title}...[/yellow]")

        self.process = ProcessInstance.create_process(cmd_arg)
        self.panel_title = cmd_arg.panel_title
        self.layout_name = cmd_arg.layout_name
        self.panel_output_buffer = deque()
        self.panel_output_buffer.append(f"Starting {cmd_arg.panel_title}...")

        console.print(
            f"[green]{cmd_arg.panel_title} started with PID: {self.process.pid}[/green]"
        )

    @property
    def alive(self) -> bool:
        # if p.poll() is None, which means the process is not stop
        return self.process.poll() is None

    @property
    def pid(self) -> int:
        return self.process.pid

    def kill(self) -> None:
        return self.process.kill()

    def terminate(self) -> None:
        return self.process.terminate()

    def wait(self, timeout: float | None = None):
        return self.process.wait(timeout=timeout)

    @staticmethod
    def set_non_blocking(fd) -> None:
        """Set file descriptor to non-blocking mode"""
        flags = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    @staticmethod
    def create_process(cmd_arg: CmdArg) -> subprocess.Popen:
        process = subprocess.Popen(
            cmd_arg.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        # Set file descriptors to non-blocking mode
        ProcessInstance.set_non_blocking(process.stdout)
        ProcessInstance.set_non_blocking(process.stderr)

        return process

    @staticmethod
    def _get_output(process: subprocess.Popen) -> tuple[str | None, str | None]:
        """Get stdout and stderr output from process (non-blocking)"""
        stdout_line = None
        stderr_line = None

        # Try to read from stdout (non-blocking)
        if process.stdout:
            try:
                line = process.stdout.readline()
                if line:
                    stdout_line = line.rstrip()
            except (IOError, OSError):
                pass  # No data available

        # Try to read from stderr (non-blocking)
        if process.stderr:
            try:
                line = process.stderr.readline()
                if line:
                    stderr_line = line.rstrip()
            except (IOError, OSError):
                pass  # No data available

        return stdout_line, stderr_line

    def get_output(self) -> tuple[str | None, str | None]:
        return self._get_output(self.process)

    def get_panel_output(self, max_lines: int) -> Panel | None:
        stdout_line, stderr_line = self.get_output()
        if stdout_line is None and stderr_line is None:
            return None

        # Append new output to buffer
        if stdout_line:
            self.panel_output_buffer.append(stdout_line)
        if stderr_line:
            self.panel_output_buffer.append(f"[red]{stderr_line}[/red]")

        # Remove old lines if buffer exceeds max_lines
        while len(self.panel_output_buffer) > max_lines:
            self.panel_output_buffer.popleft()

        # Build content from buffer
        content = "\n".join(self.panel_output_buffer)
        return Panel(content, title=self.panel_title)


class MultiSubprocessesRenderer:
    def __init__(
        self,
        cmds: list[CmdArg],
        stop_condition_callable: Callable[PS, bool] | None = None,
        timeout: int | float | None = None,
        wait_process_init_time: int | None = None,
        render_interval: float = 0.05,
        max_lines: int = 50,
        show_pid_in_panel_title: bool = True,
    ) -> None:
        self.cmds = cmds
        self.timeout = timeout
        self.wait_process_init_time = wait_process_init_time
        self.render_interval = render_interval
        self.max_lines = max_lines
        self.show_pid_in_panel_title = show_pid_in_panel_title
        if stop_condition_callable is None:
            self.stop_condition_callable = lambda: False
        else:
            self.stop_condition_callable = stop_condition_callable

        # rich attributes
        self.console = Console()

    def _init_layouts(self) -> None:
        # Set up rich layout
        self.layout = Layout()
        self.layout.split_row(*[Layout(name=cmd.layout_name) for cmd in self.cmds])
        # Initial content for panels
        for cmd in self.cmds:
            self.layout[cmd.layout_name].update(
                Panel(
                    Text(f"Starting {cmd.panel_title}...", justify="center"),
                    title=cmd.panel_title,
                )
            )

    def _init_processes(self) -> None:
        self.console.print("")
        self.process_instances = [
            ProcessInstance(cmd_arg, self.console) for cmd_arg in self.cmds
        ]
        if self.show_pid_in_panel_title:
            for p in self.process_instances:
                p.panel_title = f"{p.panel_title}: {p.pid}"

    def _update_processes_panel_output_to_layout(self):
        for p in self.process_instances:
            if new_panel_content := p.get_panel_output(self.max_lines):
                self.layout[p.layout_name].update(new_panel_content)

    @property
    def _any_process_is_not_stop(self) -> bool:
        for p in self.process_instances:
            if p.alive:
                return True
        return False

    @property
    def _is_timeout(self) -> bool:
        return (self.timeout is not None) and (
            time.time() - self.start_time
        ) > self.timeout

    def _graceful_cleanup_processes(self) -> None:
        for p in self.process_instances:
            # if the process is still running, kill it
            if p.alive:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()

    def start_render(self) -> None:
        self.start_time = time.time()
        self.stop_condition_match = False
        with Live(
            self.layout, screen=True, redirect_stdout=False, redirect_stderr=False
        ) as live:
            # Loop while processes are alive
            while self._any_process_is_not_stop:
                # Small sleep to prevent busy waiting
                time.sleep(self.render_interval)
                self._update_processes_panel_output_to_layout()
                # Manually refresh the live display
                live.update(self.layout)
                # If we reach the condition, we will exit the loop
                if self.stop_condition_callable():
                    self.stop_condition_match = True
                    break
                # If we reach timeout
                if self._is_timeout:
                    break

        if self._is_timeout and self.start_time is not None:
            self.console.print(
                f"[red]Timeout after {(time.time() - self.start_time)} seconds. Shutdown.[/red]"
            )
        if self.stop_condition_match:
            self.console.print(
                "[yellow]Reach stop_condition_callable, stop rendering.[/yellow]"
            )
        if self.start_time is not None:
            self.console.print(
                f"Took {(time.time() - self.start_time)} seconds to finish."
            )

    def __enter__(self):
        self._init_layouts()
        self._init_processes()
        if self.wait_process_init_time:
            time.sleep(self.wait_process_init_time)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is KeyboardInterrupt:
            self.console.print(
                "\n[yellow]Keyboard Interrupt detected.\nCleaning up all processes before continuing...[/yellow]"
            )
            self._graceful_cleanup_processes()
            return True

        self._graceful_cleanup_processes()
        return False


# 2. Main script to manage the display and processes
if __name__ == "__main__":
    start_time = time.time()

    def stop_condition_callable():
        if (time.time() - start_time) > 1:
            return True
        return False

    with MultiSubprocessesRenderer(
        cmds=[
            CmdArg(
                [sys.executable, "-u", "examples/fastapi_pub_sub/api.py"],
                panel_title="API process",
            ),
            CmdArg(
                [sys.executable, "-u", "examples/fastapi_pub_sub/consumer.py"],
                panel_title="Consumer process",
            ),
        ],
        stop_condition_callable=stop_condition_callable,
    ) as renderer:
        renderer.start_render()
