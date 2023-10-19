import io

import pandas as pd

from transto.lib import commit, match


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

    commit(df, 'NAB', 'transactions')
