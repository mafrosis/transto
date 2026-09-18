import datetime
import io
import logging
import re
from collections.abc import Iterable
from typing import Any

import pandas as pd
from pypdf import PdfReader

from transto.lib import categorise, commit

logger = logging.getLogger('transto')

# Match a transaction row in layout-mode extracted text, eg:
#   '08 Dec 25         18.62WOOLWORTHS 3162 MALVERN AUS'
#   '16 Dec 25         63.93 -CRED VOUCHER'
TRAN_RE = re.compile(r'^(\d{2} [A-Za-z]{3} \d{2})\s+([\d,]+\.\d{2})( -)?(.*)$')


def parsepdf(file: io.BufferedReader) -> list[list[Any]]:
    '''
    Parse statement PDF into list of transactions

    Westpac PDFs are not password protected, however the default pypdf text
    extraction returns each table column separately. Layout mode preserves rows,
    allowing each transaction to be matched with a regex.
    '''
    reader = PdfReader(file)

    lines: list[str] = []
    for page in reader.pages:
        lines.extend(page.extract_text(extraction_mode='layout').splitlines())

    return parse_transactions(lines)


def parse_transactions(lines: Iterable[str]) -> list[list[Any]]:
    '''
    Parse layout-mode statement text lines into a list of transactions

    Args:
        lines: Text lines extracted from a statement PDF

    Returns:
        List of [date, card, source, amount] transactions
    '''
    transactions: list[list[str]] = []

    for line in lines:
        m = TRAN_RE.match(line.strip())
        if not m:
            continue

        date, amount, credit, desc = m.groups()

        # Collapse the multi-space runs emitted by layout mode
        desc = re.sub(r'\s+', ' ', desc).strip()

        # Skip rows without a description
        if not desc:
            continue

        # Credits are suffixed with " -", emit as a negative to match the hsbc parser
        if credit:
            amount = f'-{amount}'

        t_date = datetime.datetime.strptime(date, '%d %b %y')  # noqa: DTZ007
        transactions.append([t_date, '', desc, amount])

    return transactions


def cc(file: io.BufferedReader):
    trans = parsepdf(file)

    df = pd.DataFrame(trans, columns=['date', 'card', 'source', 'amount'])

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

    df, matched_count = categorise(df)

    logger.info('Found %d transactions, and matched %s', len(df), matched_count)

    commit(df, 'WESTPAC', 'credit')
