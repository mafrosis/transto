import datetime
import dataclasses
import decimal
from typing import Optional, Tuple

import gspread
from gspread_dataframe import set_with_dataframe as set_with_dataframe_
from gspread_formatting import cellFormat
from gspread_formatting.dataframe import BasicFormatter, format_with_dataframe
import pandas as pd

from transto.auth import gsuite as auth_gsuite


SPREADO_ID = '1Kej_528L0RH_cyNKtCe2jUCyfR9ITp0WcGZBiMHGCiw'


@dataclasses.dataclass
class Grant:
    date: datetime.date
    number: str
    qty: int
    vested: int


@dataclasses.dataclass
class Vest:
    grant: Grant
    period: int
    date: datetime.date
    qty: int
    taxable: Optional[decimal.Decimal]=None


def main(vestfile: str, sellfile: str):
    '''
    Parse etrade reports into Google Sheets
    '''
    sh = auth_gsuite().open_by_key(SPREADO_ID).worksheet('ESS_')

    df = pd.read_excel(
        vestfile,
        sheet_name=1,
        parse_dates=['Grant Date', 'Vest Date'],
        date_format={'Grant Date': '%d-%b-%Y', 'Vest Date': '%m/%d/%Y'},
    )
    grants, vests = vesting(df)

    df = pd.read_excel(
        sellfile,
        skiprows=[1],
        parse_dates=['Date Sold', 'Grant Date', 'Vest Date', 'Purchase Date'],
        date_format='%m/%d/%Y',
    )
    rs, espp = selling(df)

    def char_to_col(char: str) -> int:
        'Convert a character to a column number'
        return ord(char) - 64

    # Grants
    sh.update('A1', [['Grants']])
    set_with_dataframe(sh, grants, row=2)

    # Vests
    sh.update(f'A{len(grants)+4}', [['Vests']])
    set_with_dataframe(sh, vests, row=len(grants)+5)

    # ESPP
    sh.update('H1', [['ESPP']])
    set_with_dataframe(sh, espp, row=2, col=char_to_col('H'))

    # Sales
    sh.update(f'H{len(espp)+4}', [['Sales']])
    set_with_dataframe(sh, rs, row=len(espp)+5, col=char_to_col('H'))


def set_with_dataframe(sh: gspread.Worksheet, df: pd.DataFrame, row: int=1, col: int=1):
    'Set a DataFrame on a Google Sheet'
    class Formatter(BasicFormatter):
        def format_for_column(self, column, col_number, dataframe):
            'https://numpy.org/doc/stable/reference/generated/numpy.dtype.kind.html'
            # M type is numpy datetime
            if column.dtype.kind == 'M':
                return cellFormat(numberFormat=self.date_format, horizontalAlignment='RIGHT')
            else:
                return super().format_for_column(column, col_number, dataframe)

    df_fmtr = Formatter(decimal_format='$ #,##0.00', date_format='yyyy-mm-dd')

    set_with_dataframe_(sh, df, row=row, col=col)
    format_with_dataframe(sh, df, df_fmtr, row=row, col=col, include_column_header=False)


def vesting(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    'Parse the grant & vesting data, and return as DataFrames'
    grants = []
    vests = []

    for _, series in df.iterrows():
        if series['Record Type'] == 'Grant':
            grant = Grant(
                date=series['Grant Date'],
                number=series['Grant Number'],
                qty=series['Granted Qty.'],
                vested=series['Vested Qty.'],
            )
            grants.append(grant)

        if series['Record Type'] == 'Vest Schedule':
            # Ignore unvested shares
            if series['Vested Qty..1'] == 0:
                continue

            vest = Vest(
                grant=grant,
                period=series['Vest Period'],
                date=series['Vest Date'],
                qty=series['Vested Qty..1'],
            )
            vests.append(vest)

        if series['Record Type'] == 'Tax Withholding':
            vest.taxable = series['Taxable Gain']

    # Expand list of grants into a DataFrame
    dfg = pd.json_normalize(
        dataclasses.asdict(obj) for obj in grants  # type: ignore[arg-type]
    ).convert_dtypes().rename(
        columns={
            'number': 'Grant Number',
            'date': 'Grant Date',
            'qty': 'Grant Qty',
            'vested': 'Vested Qty',
        }
    )
    df_grants = dfg[['Grant Number', 'Grant Date', 'Grant Qty', 'Vested Qty']].sort_values(
        'Grant Date', ascending=False
    ).reset_index(drop=True)

    # Expand list of vests into a DataFrame
    dfv = pd.json_normalize(
        dataclasses.asdict(obj) for obj in vests  # type: ignore[arg-type]
    ).convert_dtypes().rename(
        columns={
            'period': 'Vest Period',
            'date': 'Vest Date',
            'qty': 'Vest Qty',
            'taxable': 'Taxable USD',
            'grant.number': 'Grant Number',
        }
    )

    # Add period to grant number
    dfv['Grant Number'] = dfv.apply(lambda x: f"{x['Grant Number']}-{x['Vest Period']}", axis=1)

    # Drop extraneous columns, sort by vest date
    df_vests = dfv[
        ['Grant Number', 'Vest Date', 'Vest Qty', 'Taxable USD']
    ].sort_values('Vest Date').reset_index(drop=True)

    # Create formula columns
    df_vests['Exch Rate'] = df_vests.apply(
        lambda x: f'=1/VLOOKUP(B{x.name+len(df_grants)+6}, RBA!$A:$B, 2, True)', axis=1
    )
    df_vests['Taxable AUD'] = df_vests.apply(
        lambda x: f'=D{x.name+len(df_grants)+6}*E{x.name+len(df_grants)+6}', axis=1
    )

    return df_grants, df_vests


def selling(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    'Parse the selling data and put into Google Sheets'
    df_rs = df[df['Plan Type'] == 'RS'][[
        'Date Sold', 'Vest Date', 'Qty.', 'Adjusted Cost Basis Per Share', 'Total Proceeds',
        'Proceeds Per Share', 'Adjusted Gain/Loss Per Share', 'Adjusted Gain/Loss',
        'Capital Gains Status', 'Grant Number',
    ]].rename(
        columns={
            'Qty.': 'Qty',
            'Adjusted Cost Basis Per Share': 'Cost Basis',
            'Total Proceeds': 'Proceeds',
            'Adjusted Gain/Loss Per Share': 'CG Per Share',
            'Adjusted Gain/Loss': 'CG Total',
            'Capital Gains Status': 'CG Status',
        }
    ).reset_index(drop=True)

    df_espp = df[df['Plan Type'] == 'ESPP'][[
        'Purchase Date', 'Purchase Price', 'Purchase Date Fair Mkt. Value', 'Qty.',
        'Ordinary Income Recognized Per Share', 'Ordinary Income Recognized'
    ]].rename(
        columns={
            'Qty.': 'Qty',
            'Purchase Date Fair Mkt. Value': 'Market Value',
            'Ordinary Income Recognized Per Share': 'Income Per Share',
            'Ordinary Income Recognized': 'Total Income',
        }
    ).reset_index(drop=True)

    # Create formula columns
    df_espp['Income Per Share'] = df_espp.apply(
        lambda x: f'=J{x.name+3}-I{x.name+3}', axis=1
    )
    df_espp['Total Income'] = df_espp.apply(
        lambda x: f'=L{x.name+3}*K{x.name+3}', axis=1
    )
    df_rs.insert(2, '30 Day Rule', df_rs.apply(
        lambda x: f'=IF(H{x.name+len(df_espp)+6}<I{x.name+len(df_espp)+6}+30, "Yes", "No")', axis=1
    ))

    return df_rs, df_espp
