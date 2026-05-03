from unittest.mock import patch

from main import main


class TestMain:
    def test_runs_client(self) -> None:
        with patch("main.RaftClient") as mock_cls:
            main()
        mock_cls.return_value.run.assert_called_once()
