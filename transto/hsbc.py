import io
from typing import List

import pandas as pd
from py_pdf_parser import tables
from py_pdf_parser.loaders import load

from transto.lib import commit, match


def parsepdf(file: io.BufferedReader) -> List[List[str]]:
    '''
    Parse statement PDF into list of transactions
    '''
    doc = load(file, font_mapping={
        'AAAAAK+UniversLT,9.0': 'table',
        'AAAAAI+UniversLT-Bold,8.0': 'header',
        'AAAAAI+UniversLT-Bold,10.0': 'titles',
    })

    tsection = doc.sectioning.create_section(
        name='transactions',
        start_element=doc.elements.filter_by_font('header').filter_by_text_equal('Transaction Date').extract_single_element(),
        end_element=doc.elements.filter_by_font('titles').filter_by_text_equal('Promotional Transactions').extract_single_element(),
        include_last_element=False,
    )

    return [
        t for t in tables.extract_table(tsection.elements.filter_by_font('table'), as_text=True)
        if t[2] not in ('OPENING BALANCE', 'CLOSING BALANCE')
    ]


def cc(file: io.BufferedReader):
    trans = parsepdf(file)

    df = pd.DataFrame(trans, columns=['date', 'card', 'source', 'amount'])

    # Date formatting
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%y')

    # Parse currency
    df['amount'] = df['amount'].str.extract(r'([0-9.]+)').astype(float)

    df = match(df)

    commit(df, 'HSBC', 'credit')
