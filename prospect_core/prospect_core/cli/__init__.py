import click


@click.group()
def main() -> None:
    pass


@main.command()
def hello_world() -> None:
    """Exists to test cli."""
    click.echo("Say 'hello' to prospect_core")
