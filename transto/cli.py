import io
import os
import logging

import click


from transto.bom import cc, offset
from transto.etrade import main as process_etrade
from transto.mapping import write_mapping_sheet_from_yaml, write_yaml_from_mapping_sheet
from transto.lib import recategorise


logger = logging.getLogger('transto')
sh = logging.StreamHandler()
logger.addHandler(sh)
logger.setLevel(logging.INFO)


@click.group()
@click.option('--debug', is_flag=True, default=False)
def cli(debug):
    # Set DEBUG logging based on ENV or --debug CLI flag
    if debug or os.environ.get('DEBUG'):
        logger.setLevel(logging.DEBUG)


@cli.command()
def recat():
    'Re-categorise all transactions'
    recategorise()


@cli.command()
@click.argument('file', type=click.File('rb'))
def credit(file: io.BufferedReader):
    '''
    Categorise BOM CSV

    \b
    1. View the CC account page
    2. Select "All" transactions
    3. Scroll to the bottom of the page
    4. Select "Include categories"
    5. Click "Export Transaction History"

    FILE - Raw CSV of bank transactions
    '''
    cc(file)


@cli.command()
@click.argument('file', type=click.File('rb'))
def current(file: io.BufferedReader):
    '''
    Categorise BOM CSV

    \b
    1. View the CC account page
    2. Select "All" transactions
    3. Scroll to the bottom of the page
    4. Select "Include categories"
    5. Click "Export Transaction History"

    FILE - Raw CSV of bank transactions
    '''
    offset(file)


@cli.command()
@click.argument('vestfile')
@click.argument('cgfile')
def etrade(vestfile: str, cgfile: str):
    '''
    Process the etrade vesting & selling reports.

    \b
    Vesting report:
    - At Work > My Account > Benefit History
    - Download Expanded

    \b
    Capital gain report:
    - At Work > My Account > Gains & Losses
    - Choose 'Custom Date'
    - Enter '01/01/2021' as the start date
    - Apply
    - Download Expanded

    \b
    VESTFILE - Vesting report from etrade
    CGFILE - Gains & Losses report from etrade
    '''
    process_etrade(vestfile, cgfile)


@cli.group()
def mapping():
    'Subcommands to work with the transaction mapping metadata'


@mapping.command()
def to_yaml():
    'Write YAML to mapping sheet'
    write_yaml_from_mapping_sheet()


@mapping.command()
def to_gsheet():
    'Write mapping sheet from YAML'
    write_mapping_sheet_from_yaml()
