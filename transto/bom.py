import io

import pandas as pd

from transto.lib import commit, match


def prepare_source(s: pd.Series) -> pd.Series:
    'Preprocess the transaction source data'
    return s.apply(lambda x: " ".join(x.split()))


def bom(df):
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

    return match(df)


def cc(file: io.BufferedReader):
    df = pd.read_csv(file, index_col=False)
    df = bom(df)

    commit(df, 'BOM', 'credit')


def offset(file: io.BufferedReader):
    df = pd.read_csv(file, index_col=False)
    df = bom(df)

    commit(df, 'BOM', 'offset')
