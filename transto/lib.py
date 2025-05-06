import hashlib
import logging
import re
import readline
from typing import Tuple

import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread_formatting import cellFormat, format_cell_range

from transto import SPREADO_ID
from transto.auth import gsuite as auth_gsuite
from transto.mapping import load_mapping, write_mapping

logger = logging.getLogger('transto')


def categorise(df) -> (pd.DataFrame, int):
    def _match(searchterm):
        for topcat, categories in load_mapping().items():
            for seccat, patterns in categories.items():
                for pat in patterns:
                    if pat:
                        try:
                            if re.search('(.*)'.join(pat.split(' ')), searchterm, re.IGNORECASE):
                                return topcat, seccat, pat
                        except re.error:
                            logger.error('Failed parsing regex: %s', pat)
        return pd.NA, pd.NA, pd.NA

    # Include mandatory columns
    if 'override' not in df.columns:
        df['override'] = False
    if 'topcat' not in df.columns:
        df['topcat'] = pd.Series(dtype=str)
        df['seccat'] = pd.Series(dtype=str)
        df['searchterm'] = pd.Series(dtype=str)

    # Apply match function against all non-override transactions
    matched = (
        df[~df.override]
        .apply(lambda row: _match(row.source), axis=1, result_type='expand')
        .rename(columns={0: 'topcat', 1: 'seccat', 2: 'searchterm'})
    )
    df.loc[matched.index, ['topcat', 'seccat', 'searchterm']] = matched[['topcat', 'seccat', 'searchterm']]

    # Any deposit which is not a transfer, is a refund
    df.loc[(df.amount.gt(0)) & (~df['topcat'].isin(['transfer', 'income'])), ['topcat', 'seccat', 'searchterm']] = [
        'transfer',
        'refund',
        'n/a',
    ]
    return df, len(matched[~matched.topcat.isna()])


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
        df.loc[df.index == i, 'source'] += f' {count}'
        prev = f'{row.date}{row.amount}{row.source}'


def _fetch_transactions_sheet(sheet_name: str) -> Tuple[pd.DataFrame, gspread.Worksheet]:
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
        upstream = get_as_dataframe(
            sheet,
            dtype={
                'amount': float,
                'source': 'string',
                'topcat': 'string',
                'seccat': 'string',
                'searchterm': 'string',
                'override': 'boolean',
                'provider': 'string',
                'hash': 'string',
            },
            parse_dates=True,
            date_format={'date': '%Y-%m-%d'},
            true_values=[True],
            false_values=[False],
        )

        # Cast date column to np.datetime64
        upstream['date'] = pd.to_datetime(upstream['date'], format='%Y-%m-%d')

        # Remove all the Unnamed columns
        upstream = upstream.loc[:, ~upstream.columns.str.contains('^Unnamed')]

        # Force NaN override to False
        upstream.loc[upstream.override.isna(), 'override'] = False

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
    df = df.reindex(columns=['date', 'amount', 'source', 'topcat', 'seccat', 'searchterm'])

    # Fix duplicates before hashing
    if df.duplicated().any():
        deduplicate(df)

    # Append CC provider
    df['provider'] = provider

    # Add the hash to imported data
    df['hash'] = df.apply(
        lambda x: hashlib.sha256(f"{x['date']}{x['amount']}{x['source']}".encode()).hexdigest(),
        axis=1,
    )

    upstream, sheet = _fetch_transactions_sheet(sheet_name)

    # Combine imported data with upstream
    df = pd.concat([upstream, df], ignore_index=True)

    # Deduplicate, dropping entries from upstream DataFrame
    df.drop_duplicates(subset=['hash'], keep='last', inplace=True)

    write(sheet, df)


def write(sheet, df: pd.DataFrame):
    'Persist the current DataFrame to gsheets'
    # Deterministic sort, and clean up index
    df = df.sort_values(by=['date', 'hash'], ascending=False).reset_index(drop=True)

    # Add the seccat dropdown formula column, based on the index
    df['seccat_formula'] = df.apply(
        lambda row: f"=transpose(filter('mapping-agg'!B:B,'mapping-agg'!A:A=D{row.name + 2}))",
        axis=1,
    )

    # Write the DataFrame to gsheets and escape the plus prefix
    set_with_dataframe(sheet, df, string_escaping=re.compile(r'^[+].*').search)

    format_cell_range(sheet, 'A', cellFormat(numberFormat={'type': 'DATE', 'pattern': 'yyyy-mm-dd'}))
    format_cell_range(sheet, 'B', cellFormat(numberFormat={'type': 'CURRENCY', 'pattern': '$####.00'}))
    format_cell_range(sheet, 'C', cellFormat(numberFormat={'type': 'TEXT'}))


def recategorise(sheet_name: str | None, interactive: bool = False):
    def recat(sheet_name: str):
        'Fetch, re-match, push'
        upstream, sheet = _fetch_transactions_sheet(sheet_name)

        # Run a full match
        updated, count = categorise(upstream)
        print(f'Auto-matched {count} transactions')

        if interactive:
            _interactive_categorise(updated)

        write(sheet, updated)

    if sheet_name:
        recat(sheet_name)
    else:
        for sh in (
            'credit',
            'offset',
        ):
            recat(sh)


def _interactive_categorise(df: pd.DataFrame):
    'Prompt user to categorize unmatched transactions'

    # Get unmatched transaction sorted by frequency
    unmatched = df[df.topcat.isnull()].copy()
    if unmatched.empty:
        print('Everything is categorised')
        return

    # Track if the top/sec category mapping gets modified
    mapping_modified = False

    # Count frequency of each source using groupby
    source_count = unmatched['source'].value_counts().reset_index()
    source_count.columns = ['source', 'count']

    # Get available category
    mapping = load_mapping()
    top_category = list(mapping.keys())

    class ExitInteractive(Exception):
        pass

    class SkipThisItem(Exception):
        pass

    def _choice(items: list[str], title: str) -> str:
        print(f'\n{title}:')
        for i, cat in enumerate(items, 1):
            print(f'{i:>2}. {cat}')
        print('s.  Skip')
        print('q.  Quit')

        while True:
            try:
                ch = input('> ')
                if ch == 's':
                    raise SkipThisItem
                if ch == 'q':
                    raise ExitInteractive
                if 1 <= int(ch) <= len(items):
                    return items[int(ch) - 1]
                    break
            except ValueError:
                print('Invalid selection')

    for _, row in source_count.iterrows():
        print(40 * '-')
        print(f'Source: {row["source"]}')
        print(f'Count: {row["count"]}\n')
        print(df[df['source'] == row['source']][['date', 'amount']].to_string(index=False))

        try:
            # Select top category & secondary category
            topcat = _choice(top_category, title='Category')
            seccat = _choice(list(mapping[topcat].keys()), title='Secondary')
        except SkipThisItem:
            continue
        except ExitInteractive:
            break

        # Any manual transaction mapping will be treated as override, unless a regex pattern is created
        override = True
        regex_pattern = ''

        # Ask if user wants to add regex pattern
        add_regex = input('\nCreate a regex for this mapping? [y/N]: ').lower()
        if add_regex == 'y':
            while True:
                # Default editable prompt to transaction source
                readline.set_startup_hook(lambda src=row['source']: readline.insert_text(src))

                try:
                    regex_pattern = input('Enter pattern: ').strip()
                    if not regex_pattern:
                        break
                    re.compile(regex_pattern)

                    # Update mapping with new pattern
                    if seccat not in mapping[topcat]:
                        mapping[topcat][seccat] = []
                    mapping[topcat][seccat].append(regex_pattern)
                    mapping_modified = True
                    override = False
                    break

                except re.error:
                    print('Invalid regex pattern. Please try again.')
                finally:
                    readline.set_startup_hook()

        # Update all matching transactions
        mask = (df['source'] == row['source']) & (df['topcat'].isnull())
        df.loc[mask, ['topcat', 'seccat', 'searchterm', 'override']] = [topcat, seccat, regex_pattern, override]
        print(f'\nUpdated {mask.sum()} transactions to {topcat} > {seccat}')

    if mapping_modified:
        write_mapping(mapping)
        print('Updated mapping in Google Sheets')
