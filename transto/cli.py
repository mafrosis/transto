import io
import os
import logging

import click


from transto.bom import cc, offset
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
