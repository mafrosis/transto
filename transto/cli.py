import io
import os
import logging

import click


from transto.main import main


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
def nab(file: io.BufferedReader):
    '''
    Categorise CSV transactions

    FILE - Raw CSV of bank transactions
    '''
    main(file)
