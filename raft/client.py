import Pyro5.api
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from raft.config import LEADER_NAME, NAMESERVER_HOST, NAMESERVER_PORT


class RaftClient:
    """Interactive Raft cluster client.

    Looks up the current leader in the Pyro5 name server before every
    command, so the URI is always fresh.
    """

    def __init__(self) -> None:
        self._console = Console()

    # ------------------------------------------------------------------
    # Leader discovery
    # ------------------------------------------------------------------

    def _find_leader(self) -> str | None:
        """Return the leader URI from the name server, or None on failure."""
        try:
            ns = Pyro5.api.locate_ns(host=NAMESERVER_HOST, port=NAMESERVER_PORT)
            uri = ns.lookup(LEADER_NAME)
            return str(uri)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Command submission
    # ------------------------------------------------------------------

    def send_command(self, command: str) -> bool:
        """Look up the leader and submit *command*.

        Returns True if the command was committed.
        """
        uri = self._find_leader()
        if uri is None:
            return False
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                return bool(proxy.submit_command(command))
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Interactive loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the interactive command REPL."""
        self._console.print(
            Panel.fit(
                "[bold green]Raft Client[/bold green]\n"
                "Enter any command to replicate it across the cluster.\n"
                "Type [cyan]help[/cyan] for available commands.",
                title="sd_pyro",
            )
        )

        while True:
            try:
                raw = Prompt.ask("[bold cyan]>[/bold cyan]")
            except (KeyboardInterrupt, EOFError):
                self._console.print("\n[yellow]Interrupted.[/yellow]")
                break

            command = raw.strip()
            if not command:
                continue

            if command in ("quit", "exit"):
                self._console.print("[yellow]Goodbye.[/yellow]")
                break

            if command == "help":
                self._show_help()
                continue

            if command == "status":
                self._show_status()
                continue

            ok = self.send_command(command)
            if ok:
                self._console.print(
                    f"[green]✓[/green] Committed: [italic]{command}[/italic]"
                )
            else:
                self._console.print(
                    "[red]✗[/red] Failed — no leader or command not committed"
                )

    def _show_help(self) -> None:
        from rich.table import Table

        table = Table(title="Available Commands", show_header=True)
        table.add_column("Command", style="cyan", no_wrap=True)
        table.add_column("Description")
        table.add_row("help", "Show this help message")
        table.add_row("status", "Show the current leader URI")
        table.add_row("quit / exit", "Exit the client")
        table.add_row(
            "<any text>",
            "Submit a command to be replicated across the cluster",
        )
        self._console.print(table)

    def _show_status(self) -> None:
        uri = self._find_leader()
        if uri:
            self._console.print(
                Panel(f"Leader: [green]{uri}[/green]", title="Cluster Status")
            )
        else:
            self._console.print(
                Panel("[red]No leader found[/red]", title="Cluster Status")
            )
