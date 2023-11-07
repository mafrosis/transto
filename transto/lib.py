import hashlib
import logging
import re
from typing import Tuple

import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd

from transto.auth import gsuite as auth_gsuite
from transto.mapping import load_mapping


logger = logging.getLogger('transto')


SPREADO_ID = '1laIR3SmaKnxCg4CeNPGyzhb04NOiMSfD_lDymsGD5wE'


def match(df):
    def _match(searchterm):
        for topcat, categories in load_mapping().items():
            for seccat, patterns in categories.items():
                for pat in patterns:
                    try:
                        if re.search('(.*)'.join(pat.split(' ')), searchterm, re.IGNORECASE):
                            return topcat, seccat, pat
                    except re.error:
                        logger.error('Failed parsing regex: %s', pat)
        return '', '', ''

    df['topcat'], df['seccat'], df['searchterm'] = zip(*(df['source'].apply(_match)))

    # Any deposit which is not a transfer, is a refund
    df.loc[(df.amount.gt(0)) & (~df['topcat'].isin(['transfer','income'])), ['topcat', 'seccat', 'searchterm']] = ['transfer', 'refund', 'n/a']

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


def fetch_transactions_sheet(sheet_name: str) -> Tuple[pd.DataFrame, gspread.Worksheet]:
    '''
    Fetch the full set of transactions as a DataFrame

    Params:
        sheet_name      Name of the gsheet to retrieve
    '''
    # Auth
    gc = auth_gsuite()
    spreado = gc.open_by_key(SPREADO_ID)
    sheet = spreado.worksheet(sheet_name)

    try:
        # Fetch
        upstream = get_as_dataframe(sheet)

        # Cast date column to np.datetime64
        upstream['date'] = pd.to_datetime(upstream['date'], format='%Y-%m-%d 00:00:00')

        try:
            # Filter out rows which have been overriden upstream in gsheets
            upstream = upstream[~upstream.hash.isin(upstream.loc[upstream['override']==1, 'hash'])]
        except KeyError:
            upstream.insert(6, 'override', '')

    except (pd.errors.EmptyDataError, KeyError):
        upstream = pd.DataFrame()

    return upstream, sheet


def commit(df: pd.DataFrame, provider: str, sheet_name: str):
    '''
    Fetch, merge, push data into the upstream Google sheet

    Params:
        df          DataFrame of goodness
        provider    Name of source bank
        sheet_name  Name of target sheet
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

    upstream, sheet = fetch_transactions_sheet(sheet_name)

    # Combine imported data with upstream
    df = pd.concat([upstream, df], ignore_index=True)

    # Deduplicate, dropping entries from upstream DataFrame
    df.drop_duplicates(subset=['hash'], keep='last', inplace=True)

    # Deterministic sort
    df = df.sort_values(by=['date','hash'], ascending=False)

    set_with_dataframe(sheet, df, resize=True)
