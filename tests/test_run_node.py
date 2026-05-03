from unittest.mock import MagicMock, patch

import pytest

from raft.config import NODES
from run_node import run


class TestRun:
    def test_starts_node_with_valid_id(self) -> None:
        mock_node = MagicMock()
        with (
            patch("run_node.RaftNode", return_value=mock_node),
            patch("run_node.time.sleep", side_effect=KeyboardInterrupt),
        ):
            run("node1")
        mock_node.start.assert_called_once()
        mock_node.stop.assert_called_once()

    def test_passes_correct_config_and_peers(self) -> None:
        mock_node = MagicMock()
        with (
            patch("run_node.RaftNode", return_value=mock_node) as raft_cls,
            patch("run_node.time.sleep", side_effect=KeyboardInterrupt),
        ):
            run("node2")
        expected_cfg = next(n for n in NODES if n.node_id == "node2")
        raft_cls.assert_called_once_with(expected_cfg, NODES)

    def test_exits_on_invalid_node_id(self) -> None:
        with pytest.raises(SystemExit):
            run("invalid_id")

    def test_stops_node_even_if_start_raises(self) -> None:
        mock_node = MagicMock()
        mock_node.start.side_effect = RuntimeError("port in use")
        with patch("run_node.RaftNode", return_value=mock_node):
            with pytest.raises(RuntimeError):
                run("node3")
        mock_node.stop.assert_called_once()
