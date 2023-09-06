import io
import logging

import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import yaml


logger = logging.getLogger('transto')


SPREADO_ID = '1laIR3SmaKnxCg4CeNPGyzhb04NOiMSfD_lDymsGD5wE'


def main(file: io.BufferedReader):
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

    gc = gspread.oauth()
    spreado = gc.open_by_key(SPREADO_ID)
    sheet = spreado.worksheet('NAB_2022')
    set_with_dataframe(sheet, df)
