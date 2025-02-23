import io
import logging
import os

import click

from transto import __version__
from transto.bom import offset
from transto.etrade import main as process_etrade
from transto.etrade import refresh_rba_exchange_rate_history
from transto.hsbc import cc
from transto.lib import recategorise
from transto.mapping import write_mapping_sheet_from_yaml, write_yaml_from_mapping_sheet

logger = logging.getLogger('transto')
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.INFO)


@click.group
@click.version_option(__version__)
@click.option('--debug', is_flag=True, default=False)
def cli(debug):
    # Set DEBUG logging based on ENV or --debug CLI flag
    if debug or os.environ.get('DEBUG'):
        logger.setLevel(logging.DEBUG)


@cli.command
@click.option('--sheet', default=None, type=click.Choice(['credit', 'offset']))
def recat(sheet: str | None):
    'Re-categorise all transactions'
    recategorise(sheet)


@cli.command
@click.argument('file', type=click.File('rb'))
def credit(file: io.BufferedReader):
    '''
    Categorise HSBC credit account CSV

    \b
    1. Log into HSBC
    2. Select the credit card account
    3. Scroll to the bottom of the page
    4. Click "Download"
    5. Click "Download" on the popup

    FILE - PDF monthly statement with transactions
    '''
    cc(file)


@cli.command
@click.argument('file', type=click.File('rb'))
def current(file: io.BufferedReader):
    '''
    Categorise BOM offset account CSV

    \b
    1. View the CC account page
    2. Select "All" transactions
    3. Scroll to the bottom of the page
    4. Select "Include categories"
    5. Click "Export Transaction History"

    FILE - Raw CSV of bank transactions
    '''
    offset(file)


@cli.group
def etrade():
    pass


@etrade.command('import')
@click.argument('vestfile', type=click.Path(exists=True))
@click.argument('cgfile', type=click.Path(exists=True))
def import_(vestfile: str, cgfile: str):
    '''
    Process the etrade vesting & selling reports.

    \b
    Vesting report:
    - At Work > My Account > Benefit History
    - Click Download > Download Expanded

    \b
    Capital gain report:
    - At Work > My Account > Gains & Losses
    - Select "Custom Date" in the "Tax Year" dropdown
    - Enter '01/01/2021' as the start date
    - Click Apply
    - Click Download > Download Expanded

    \b
    VESTFILE - Vesting report from etrade
    CGFILE - Gains & Losses report from etrade
    '''
    process_etrade(vestfile, cgfile)


@etrade.command
def rba():
    'Import fresh exchange rate data from RBA'
    refresh_rba_exchange_rate_history()


@cli.group
def mapping():
    'Subcommands to work with the transaction mapping metadata'


@mapping.command
def to_yaml():
    'Write YAML to mapping sheet'
    write_yaml_from_mapping_sheet()


@mapping.command
def to_gsheet():
    'Write mapping sheet from YAML'
    write_mapping_sheet_from_yaml()
