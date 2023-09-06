import io
import hashlib
import os
import logging
import sys

import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import yaml


logger = logging.getLogger('transto')


SPREADO_ID = '1laIR3SmaKnxCg4CeNPGyzhb04NOiMSfD_lDymsGD5wE'


def auth_gsuite():
    oauth_creds_path = os.environ.get('GSUITE_OAUTH_CREDS')
    if not oauth_creds_path:
        logger.error('You must export your GCP oAuth path as GSUITE_OAUTH_CREDS')
        sys.exit(2)

    return gspread.oauth(
        credentials_filename=oauth_creds_path,
        authorized_user_filename='authorized_user.json'
    )


def bom(file: io.BufferedReader):
    df = pd.DataFrame()

    with open('mapping.yaml', encoding='utf8') as f:
        mapping = yaml.safe_load(f).get('mapping')

    # Process input CSV data into new DataFrame
    df = pd.read_csv(file, index_col=False)

    # Create source column for matching against
    df.rename(columns={'Description': 'source'}, inplace=True)

    # Date formatting
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

    # Make Debits negative & merge Credits into Debit column
    df['Debit'] = df['Debit'] * -1
    df['Amount'] = df['Debit'].fillna(df['Credit'])
    df = df.drop(columns=['Debit', 'Credit'])

    def match(searchterm):
        for topcat, categories in mapping.items():
            for seccat, patterns in categories.items():
                for pat in patterns:
                    if pat.lower() in searchterm.lower():
                        return topcat, seccat, pat
        return '', '', ''

    df['topcat'], df['seccat'], df['searchterm'] = zip(*(df['source'].apply(match)))

    # Handle payments
    df.loc[df['Category'] == 'Deposits', ['topcat', 'seccat', 'searchterm']] = ['payment', 'payment', 'Deposits']

    # Handle fees
    df.loc[df['Category'].str.contains('FEES|DEBIT ADJUSTMENTS|MISCELLANEOUS CREDIT'), ['topcat', 'seccat', 'searchterm']] = ['bills', 'bankfees', 'FEES']

    # Drop Category now we're done with it
    df = df.drop(columns=['Category'])

    df['hash'] = df.apply(lambda x: hashlib.sha256(f"{x['Date']}{x['Amount']}{x['source']}".encode('utf8')).hexdigest(), axis=1)

    # Sort the column order to match target
    df = df.reindex(['hash', 'Date', 'Amount', 'source', 'topcat', 'seccat', 'searchterm'], axis=1)

    gc = auth_gsuite()
    spreado = gc.open_by_key(SPREADO_ID)
    sheet = spreado.worksheet('NAB_2022')
    upstream = get_as_dataframe(sheet)
    set_with_dataframe(
        sheet,
        pd.concat([upstream, df], ignore_index=True),
        resize=True,
        #include_column_header=False
    )


def nab(file: io.BufferedReader):
    df = pd.DataFrame()

    with open('mapping.yaml', encoding='utf8') as f:
        mapping = yaml.safe_load(f).get('mapping')

    ## Flatten and invert the category:tag YAML data
    #mapping = {
    #    tag: category
    #    for category, tags in data['mapping'].items()
    #    for tag in tags
    #}

    # Process input CSV data into new DataFrame
    df = pd.read_csv(file)

    # Create source column for matching against
    df['source'] = df['Merchant Name'].fillna('') + ' ' + df['Transaction Details']

    def match(searchterm):
        for topcat, categories in mapping.items():
            for seccat, patterns in categories.items():
                for pat in patterns:
                    if pat.lower() in searchterm.lower():
                        return topcat, seccat, pat
        return '', '', ''

    df['topcat'], df['seccat'], df['searchterm'] = zip(*(df['source'].apply(match)))

    # Handling for cash advance
    df.loc[df['Transaction Type'] == 'CREDIT CARD CASH ADVANCE', ['topcat', 'seccat', 'searchterm']] = ['bills', 'cash', 'CREDIT CARD CASH ADVANCE']

    # Handle payments
    df.loc[df['Transaction Type'] == 'CREDIT CARD PAYMENT', ['topcat', 'seccat', 'searchterm']] = ['payment', 'payment', 'CREDIT CARD PAYMENT']

    # Handle fees
    df.loc[df['Transaction Type'].str.contains('FEES|DEBIT ADJUSTMENTS|MISCELLANEOUS CREDIT'), ['topcat', 'seccat', 'searchterm']] = ['bills', 'bankfees', 'FEES']

    # TODO
    # in one operation, pull the entire dataset, reclassify and push
    # in diff operation, classify and push new data to an existing sheet

    gc = auth_gsuite()
    spreado = gc.open_by_key(SPREADO_ID)
    sheet = spreado.worksheet('NAB_2022')
    set_with_dataframe(sheet, df)
