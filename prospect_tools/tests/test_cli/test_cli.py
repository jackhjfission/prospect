from click.testing import CliRunner

from prospect_tools.cli import hello_world, main


class TestCLI:
    """Test cases for the CLI commands."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_hello_world_command(self) -> None:
        """Test the hello_world command executes correctly."""
        result = self.runner.invoke(main, ["hello-world"])
        assert result.exit_code == 0
        assert "Say 'hello' to prospect_tools" in result.output

    def test_hello_world_direct_invoke(self) -> None:
        """Test invoking hello_world command directly."""
        result = self.runner.invoke(hello_world)
        assert result.exit_code == 0
        assert "Say 'hello' to prospect_tools" in result.output
