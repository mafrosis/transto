import functools
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


@functools.lru_cache(maxsize=1)
def load_mapping() -> dict:
    'Load transaction mapping data'
    with open('mapping.yaml', encoding='utf8') as f:
        return yaml.safe_load(f).get('mapping')


def prepare_source(s: pd.Series) -> pd.Series:
    'Preprocess the transaction source data'
    return s.apply(lambda x: " ".join(x.split()))


def match(df):
    def _match(searchterm):
        for topcat, categories in load_mapping().items():
            for seccat, patterns in categories.items():
                for pat in patterns:
                    if pat.lower() in searchterm.lower():
                        return topcat, seccat, pat
        return '', '', ''

    df['topcat'], df['seccat'], df['searchterm'] = zip(*(df['source'].apply(_match)))
    return df


def deduplicate(df: pd.DataFrame):
    '''
    If same amount was spent on same day at same vendor, then we have a duplicate.
    Add a deterministic suffix to these dupes.
    '''
    prev = ''
    for i, row in df[df.duplicated(subset=['date', 'amount', 'source'], keep=False)].iterrows():
        if f'{row.date}{row.amount}{row.source}' != prev:
            count = 0
        count += 1
        df.loc[df.index==i, 'source'] += f' {count}'
        prev = f'{row.date}{row.amount}{row.source}'


def commit(df: pd.DataFrame, provider: str):
    '''
    Fetch, merge, push data into the upstream Google sheet
    '''
    # Sort the column order to match target
    df = df.reindex(['date', 'amount', 'source', 'topcat', 'seccat', 'searchterm'], axis=1)

    # Fix duplicates before hashing
    if df.duplicated().any():
        deduplicate(df)

    # Append CC provider
    df['provider'] = provider

    # Add the hash to imported data
    df['hash'] = df.apply(
        lambda x: hashlib.sha256(f"{x['date']}{x['amount']}{x['source']}".encode('utf8')).hexdigest(),
        axis=1,
    )

    # Auth
    gc = auth_gsuite()
    spreado = gc.open_by_key(SPREADO_ID)
    sheet = spreado.worksheet('transactions')

    try:
        # Fetch
        upstream = get_as_dataframe(sheet)

        # Cast date column to np.datetime64
        upstream['date'] = pd.to_datetime(upstream['date'], format='%Y-%m-%d 00:00:00')

    except pd.errors.EmptyDataError:
        upstream = pd.DataFrame()

    # Filter out rows which have been overriden upstream in gsheets
    df = df[~df.hash.isin(upstream.loc[upstream.override==1, 'hash'])]

    # Combine imported data with upstream
    df = pd.concat([upstream, df], ignore_index=True)

    # Deduplicate, dropping entries from upstream DataFrame
    df.drop_duplicates(subset=['hash'], keep='last', inplace=True)

    # Deterministic sort
    df = df.sort_values(by=['date','hash'], ascending=False)

    set_with_dataframe(sheet, df, resize=True)


def bom(file: io.BufferedReader):
    df = pd.read_csv(file, index_col=False)

    # Create source column for this CSV format
    df.rename(columns={'Description': 'source'}, inplace=True)

    df['source'] = prepare_source(df['source'])

    # Date formatting
    df['date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    df.drop(columns=['Date'], inplace=True)

    # Make Debits negative & merge Credits into Debit column
    df['Debit'] = df['Debit'] * -1
    df['amount'] = df['Debit'].fillna(df['Credit'])
    df = df.drop(columns=['Debit', 'Credit'])

    df = match(df)

    # Handle payments
    df.loc[df['Category'] == 'Deposits', ['topcat', 'seccat', 'searchterm']] = ['payment', 'payment', 'Deposits']

    # Handle fees
    df.loc[df['Category'].str.contains('Foreign Transaction Fee'), ['topcat', 'seccat', 'searchterm']] = ['bills', 'bankfees', 'FEES']

    # Drop Category now it's finished with
    df = df.drop(columns=['Category'])

    commit(df, 'BOM')


def nab(file: io.BufferedReader):
    df = pd.read_csv(file)

    # Create source column for this CSV format
    df['source'] = df['Merchant Name'].fillna('') + ' ' + df['Transaction Details']

    # Date formatting
    df['date'] = pd.to_datetime(df['Date'], format='%d %b %y')

    # Rename important columns
    df.rename(columns={'Amount': 'amount'}, inplace=True)

    # Drop useless columns
    df.drop(columns=['Date', 'Account Number', 'Unnamed: 3', 'Transaction Details', 'Balance', 'Category', 'Merchant Name'], inplace=True)

    df = match(df)

    # Handling for cash advance
    df.loc[df['Transaction Type'] == 'CREDIT CARD CASH ADVANCE', ['topcat', 'seccat', 'searchterm']] = ['bills', 'cash', 'CREDIT CARD CASH ADVANCE']

    # Handle payments
    df.loc[df['Transaction Type'] == 'CREDIT CARD PAYMENT', ['topcat', 'seccat', 'searchterm']] = ['payment', 'payment', 'CREDIT CARD PAYMENT']

    # Handle fees
    df.loc[df['Transaction Type'].str.contains('FEES|DEBIT ADJUSTMENTS|MISCELLANEOUS CREDIT'), ['topcat', 'seccat', 'searchterm']] = ['bills', 'bankfees', 'FEES']

    # Drop Transaction Type now it's finished with
    df = df.drop(columns=['Transaction Type'])

    commit(df, 'NAB')
