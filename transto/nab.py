import io

import pandas as pd

from transto.lib import categorise, commit


def nab(file: io.BufferedReader):
    df = pd.read_csv(file)

    # Create source column for this CSV format
    df['source'] = df['Merchant Name'].fillna('') + ' ' + df['Transaction Details']

    # Date formatting
    df['date'] = pd.to_datetime(df['Date'], format='%d %b %y')

    # Rename important columns
    df.rename(columns={'Amount': 'amount'}, inplace=True)

    # Drop useless columns
    df.drop(
        columns=[
            'Date',
            'Account Number',
            'Unnamed: 3',
            'Transaction Details',
            'Transaction Type',
            'Balance',
            'Category',
            'Merchant Name',
        ],
        inplace=True,
    )

    df, _ = categorise(df)

    commit(df, 'NAB', 'credit')
