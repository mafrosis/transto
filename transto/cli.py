import io
import os
import logging

import click


from transto.main import bom as bom_, nab as nab_


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
@click.argument('file', type=click.File('rb'))
def bom(file: io.BufferedReader):
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
    bom_(file)


@cli.command()
@click.argument('file', type=click.File('rb'))
def nab(file: io.BufferedReader):
    '''
    Categorise NAB CSV

    FILE - Raw CSV of bank transactions
    '''
    nab_(file)
