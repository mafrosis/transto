import datetime
import io
import logging
import os
import warnings
from typing import Any, List

import pandas as pd
from cryptography.utils import CryptographyDeprecationWarning
from pypdf import PdfReader

with warnings.catch_warnings(action='ignore', category=CryptographyDeprecationWarning):
    pass

from transto.exceptions import MissingHsbcPdfPassword
from transto.lib import commit, match

pd.set_option('future.no_silent_downcasting', True)


logger = logging.getLogger('transto')


def parsepdf(file: io.BufferedReader) -> List[List[Any]]:
    '''
    Parse statement PDF into list of transactions
    '''
    reader = PdfReader(file)
    if reader.is_encrypted:
        hsbc_pdf_password = os.environ.get('HSBC_PDF_PASSWORD')
        if not hsbc_pdf_password:
            raise MissingHsbcPdfPassword

        reader.decrypt(hsbc_pdf_password)

    parts: List[List[Any]] = [[]]

    def font_matcher(text, _cm, _tm, font_dict, _font_size):
        'Accumlate text matching named font into list of lists'
        if isinstance(font_dict, dict) and font_dict.get('/BaseFont') == '/UniversLT':
            # Create a new list on every newline
            if text == '\n':
                parts.append([])

            # Ignore empty lines
            if not text.strip():
                return

            parts[len(parts) - 1].append(text.strip())

    for i, page in enumerate(reader.pages):
        if i == 0:
            continue
        page.extract_text(visitor_text=font_matcher)

    # Post process into list of transactions
    transactions = []

    for t in parts:
        # Handle fee items appended to another transaction
        if len(t) == 2 and 'fee' in t[0].lower():  # noqa: PLR2004
            t.insert(0, '')
            t.insert(0, transactions[-1][0].strftime('%d/%m/%y'))

        # Skip items with anything other than 3 or 4 columns
        if len(t) not in {3, 4}:
            continue

        # Add in an extra column where a card number not used for a transaction
        if len(t) == 3:  # noqa: PLR2004
            t.insert(1, '')

        # Skip OPENING BALANCE and CLOSING BALANCE items
        if t[2].strip() in {'OPENING BALANCE', 'CLOSING BALANCE'}:
            continue

        try:
            # Skip items where item zero is not a date
            t[0] = datetime.datetime.strptime(t[0], '%d/%m/%y')  # noqa: DTZ007
        except ValueError:
            continue

        transactions.append(t)

    return transactions


def cc(file: io.BufferedReader):
    trans = parsepdf(file)

    df = pd.DataFrame(trans, columns=['date', 'card', 'source', 'amount'])

    # Date formatting
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%y')

    # Drop comma, dollar sign
    df['amount'] = df['amount'].replace('[$,]', '', regex=True)

    # Extract negative amounts into credits column, dropping negative sign
    df['credits'] = df[df['amount'].str.startswith('-')]['amount'].replace('[-]', '', regex=True)

    # Extract positive amounts into debits column
    df['debits'] = df[~df['amount'].str.startswith('-')]['amount']

    # Make debits negative
    df['debits'] = df['amount'].astype(float) * -1

    # Merge debits and credits into amount column
    df['amount'] = df['credits'].fillna(df['debits']).astype(float)
    df.drop(columns=['credits', 'debits'], inplace=True)

    df = match(df)

    logger.info('Found %d transactions, and matched %s', len(df), (df['searchterm'].values != '').sum())  # noqa: PLC1901

    commit(df, 'HSBC', 'credit')
