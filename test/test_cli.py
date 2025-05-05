import pytest
from click.testing import CliRunner

from transto.cli import cli


def test_cli_help():
    '''Test that the CLI can be invoked with --help without error.'''
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Usage:' in result.output


@pytest.mark.parametrize(
    'command', ['recat', 'credit', 'current', 'etrade import', 'etrade rba', 'mapping to-yaml', 'mapping to-gsheet']
)
def test_subcommand_help(command):
    '''Test that each subcommand can be invoked with --help without error.'''
    runner = CliRunner()
    result = runner.invoke(cli, [*command.split(), '--help'])
    assert result.exit_code == 0
    assert 'Usage:' in result.output


def test_cli_commands_loaded():
    '''Test that all expected commands are loaded in the CLI.'''
    commands = cli.commands.keys()
    assert set(commands) == {'recat', 'credit', 'current', 'etrade', 'mapping'}


def test_etrade_subcommands_loaded():
    '''Test that all expected etrade subcommands are loaded.'''
    etrade_cmd = cli.commands['etrade']
    etrade_commands = etrade_cmd.commands.keys()
    assert set(etrade_commands) == {'import', 'rba'}


def test_mapping_subcommands_loaded():
    '''Test that all mapping subcommands are loaded.'''
    mapping_cmd = cli.commands['mapping']
    mapping_commands = mapping_cmd.commands.keys()
    assert set(mapping_commands) == {'to-yaml', 'to-gsheet'}
