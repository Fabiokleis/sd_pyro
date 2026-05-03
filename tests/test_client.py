from unittest.mock import MagicMock, patch

from raft.client import RaftClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_proxy(result: bool = True) -> MagicMock:
    proxy = MagicMock()
    proxy.__enter__ = MagicMock(return_value=proxy)
    proxy.__exit__ = MagicMock(return_value=False)
    proxy.submit_command = MagicMock(return_value=result)
    return proxy


def _make_ns(uri: str = "PYRO:node1@localhost:9001") -> MagicMock:
    ns = MagicMock()
    ns.lookup.return_value = uri
    return ns


# ---------------------------------------------------------------------------
# TestFindLeader
# ---------------------------------------------------------------------------


class TestFindLeader:
    def test_returns_uri_from_nameserver(self) -> None:
        ns = _make_ns("PYRO:node1@localhost:9001")
        with patch("Pyro5.api.locate_ns", return_value=ns):
            assert RaftClient()._find_leader() == "PYRO:node1@localhost:9001"

    def test_returns_none_on_nameserver_unavailable(self) -> None:
        with patch("Pyro5.api.locate_ns", side_effect=Exception("no ns")):
            assert RaftClient()._find_leader() is None

    def test_returns_none_when_leader_not_registered(self) -> None:
        ns = MagicMock()
        ns.lookup.side_effect = Exception("name not found")
        with patch("Pyro5.api.locate_ns", return_value=ns):
            assert RaftClient()._find_leader() is None

    def test_converts_lookup_result_to_str(self) -> None:
        """lookup() may return a URI object; _find_leader must return str."""
        ns = MagicMock()
        ns.lookup.return_value = object()  # not a str
        with patch("Pyro5.api.locate_ns", return_value=ns):
            result = RaftClient()._find_leader()
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestSendCommand
# ---------------------------------------------------------------------------


class TestSendCommand:
    def test_returns_true_on_committed(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        with patch("Pyro5.api.Proxy", return_value=_make_proxy(True)):
            assert client.send_command("set x 1") is True

    def test_returns_false_when_commit_fails(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        with patch("Pyro5.api.Proxy", return_value=_make_proxy(False)):
            assert client.send_command("set x 1") is False

    def test_returns_false_when_no_leader_found(self) -> None:
        client = RaftClient()
        with patch("Pyro5.api.locate_ns", side_effect=Exception("no ns")):
            assert client.send_command("set x 1") is False

    def test_looks_up_leader_when_uri_is_none(self) -> None:
        client = RaftClient()
        ns = _make_ns("PYRO:node2@localhost:9002")
        with (
            patch("Pyro5.api.locate_ns", return_value=ns),
            patch("Pyro5.api.Proxy", return_value=_make_proxy(True)),
        ):
            client.send_command("set x 1")
        assert client._leader_uri == "PYRO:node2@localhost:9002"

    def test_clears_leader_uri_on_proxy_error(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        proxy = _make_proxy()
        proxy.__enter__.side_effect = Exception("connection refused")
        with patch("Pyro5.api.Proxy", return_value=proxy):
            result = client.send_command("set x 1")
        assert result is False
        assert client._leader_uri is None

    def test_forwards_command_string_verbatim(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        proxy = _make_proxy(True)
        with patch("Pyro5.api.Proxy", return_value=proxy):
            client.send_command("delete key_abc")
        proxy.submit_command.assert_called_once_with("delete key_abc")


# ---------------------------------------------------------------------------
# TestRun
# ---------------------------------------------------------------------------


class TestRun:
    def test_exits_on_quit(self) -> None:
        client = RaftClient()
        with (
            patch("raft.client.Prompt.ask", side_effect=["quit"]),
            patch.object(client._console, "print"),
        ):
            client.run()  # must return without error

    def test_exits_on_exit_alias(self) -> None:
        client = RaftClient()
        with (
            patch("raft.client.Prompt.ask", side_effect=["exit"]),
            patch.object(client._console, "print"),
        ):
            client.run()

    def test_exits_on_keyboard_interrupt(self) -> None:
        client = RaftClient()
        with (
            patch("raft.client.Prompt.ask", side_effect=KeyboardInterrupt),
            patch.object(client._console, "print"),
        ):
            client.run()  # must not propagate KeyboardInterrupt

    def test_exits_on_eof(self) -> None:
        client = RaftClient()
        with (
            patch("raft.client.Prompt.ask", side_effect=EOFError),
            patch.object(client._console, "print"),
        ):
            client.run()

    def test_skips_empty_input(self) -> None:
        client = RaftClient()
        with (
            patch("raft.client.Prompt.ask", side_effect=["", "   ", "quit"]),
            patch.object(client._console, "print"),
        ):
            client.run()  # must not crash or submit empty commands

    def test_status_shows_leader_uri(self) -> None:
        client = RaftClient()
        ns = _make_ns("PYRO:node3@localhost:9003")
        from rich.panel import Panel as RichPanel

        panels: list[RichPanel] = []

        def capture(*args: object, **_kw: object) -> None:
            for arg in args:
                if isinstance(arg, RichPanel):
                    panels.append(arg)

        with (
            patch("raft.client.Prompt.ask", side_effect=["status", "quit"]),
            patch("Pyro5.api.locate_ns", return_value=ns),
            patch.object(client._console, "print", side_effect=capture),
        ):
            client.run()
        assert any(
            "PYRO:node3@localhost:9003" in str(p.renderable) for p in panels
        )

    def test_status_shows_no_leader(self) -> None:
        client = RaftClient()
        with (
            patch("raft.client.Prompt.ask", side_effect=["status", "quit"]),
            patch("Pyro5.api.locate_ns", side_effect=Exception("no ns")),
            patch.object(client._console, "print"),
        ):
            client.run()  # must not raise

    def test_submits_user_command(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        proxy = _make_proxy(True)
        with (
            patch("raft.client.Prompt.ask", side_effect=["set x 42", "quit"]),
            patch("Pyro5.api.Proxy", return_value=proxy),
            patch.object(client._console, "print"),
        ):
            client.run()
        proxy.submit_command.assert_called_once_with("set x 42")

    def test_shows_success_message_on_commit(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        proxy = _make_proxy(True)
        printed: list[str] = []
        with (
            patch("raft.client.Prompt.ask", side_effect=["set x 1", "quit"]),
            patch("Pyro5.api.Proxy", return_value=proxy),
            patch.object(
                client._console,
                "print",
                side_effect=lambda *a, **_kw: printed.append(str(a)),
            ),
        ):
            client.run()
        assert any("Committed" in s for s in printed)

    def test_shows_failure_message_on_no_commit(self) -> None:
        client = RaftClient()
        client._leader_uri = "PYRO:node1@localhost:9001"
        proxy = _make_proxy(False)
        printed: list[str] = []
        with (
            patch("raft.client.Prompt.ask", side_effect=["set x 1", "quit"]),
            patch("Pyro5.api.Proxy", return_value=proxy),
            patch.object(
                client._console,
                "print",
                side_effect=lambda *a, **_kw: printed.append(str(a)),
            ),
        ):
            client.run()
        assert any("Failed" in s for s in printed)

    def test_help_command_prints_table(self) -> None:
        from rich.table import Table

        client = RaftClient()
        tables: list[Table] = []

        def capture(*args: object, **_kw: object) -> None:
            for arg in args:
                if isinstance(arg, Table):
                    tables.append(arg)

        with (
            patch("raft.client.Prompt.ask", side_effect=["help", "quit"]),
            patch.object(client._console, "print", side_effect=capture),
        ):
            client.run()
        assert len(tables) == 1
        assert tables[0].title == "Available Commands"
        # help, status, quit/exit, <any text>
        assert len(tables[0].rows) == 4
