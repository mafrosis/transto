import dataclasses
import datetime
import decimal
from typing import Optional, Tuple

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe as set_with_dataframe_
from gspread_formatting import cellFormat, format_cell_range, numberFormat, textFormat
from gspread_formatting.dataframe import BasicFormatter, format_with_dataframe

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
    taxable: Optional[decimal.Decimal] = None


def main(vestfile: str, sellfile: str):
    '''
    Parse etrade reports into Google Sheets
    '''
    sh = auth_gsuite().open_by_key(SPREADO_ID).worksheet('ESS')

    df = pd.read_excel(
        vestfile,
        sheet_name=1,
        parse_dates=['Grant Date', 'Vest Date'],
        date_format={'Grant Date': '%d-%b-%Y', 'Vest Date': '%m/%d/%Y'},
    )
    grants, vests = vesting(df)

    # ESPP & Sales starts in which column?
    loffset_col = 'I'

    df = pd.read_excel(
        vestfile,
        sheet_name=0,
        parse_dates=['Purchase Date', 'Grant Date'],
        date_format='%d-%b-%Y',
    )
    espp = espping(df, loffset_col)

    df = pd.read_excel(
        sellfile,
        skiprows=[1],
        parse_dates=['Date Sold', 'Grant Date', 'Date Acquired'],
        date_format='%m/%d/%Y',
    )
    rs = selling(df, len(espp) + 6, loffset_col)

    # Grants
    sh.update('A1', [['Grants']])
    fmt_set_bold(sh, 'A1')
    set_with_dataframe(sh, grants, row=2)
    fmt_set_leftalign(sh, 'A2:D2')

    # Vests
    sh.update(f'A{len(grants) + 4}', [['Vests']])
    fmt_set_bold(sh, f'A{len(grants) + 4}')
    set_with_dataframe(sh, vests, row=len(grants) + 5)
    fmt_set_decimal(sh, f'F{len(grants) + 6}:F', 4)
    fmt_set_aud(sh, f'D{len(grants) + 6}:E')
    fmt_set_aud(sh, f'G{len(grants) + 6}:G')
    fmt_set_leftalign(sh, f'A{len(grants) + 5}:G{len(grants) + 5}')

    loffset = char_to_col(loffset_col)

    # ESPP
    sh.update(f'{loffset_col}1', [['ESPP']])
    fmt_set_bold(sh, f'{loffset_col}1')
    set_with_dataframe(sh, espp, row=2, col=char_to_col(loffset_col))
    fmt_set_aud(sh, f'{col_to_char(loffset + 4)}1:{col_to_char(loffset + 10)}{len(espp) + 3}')
    fmt_set_leftalign(sh, '{loffset_col}2:S2')

    # Sales
    sh.update(f'{loffset_col}{len(espp) + 4}', [['Sales']])
    fmt_set_bold(sh, f'{loffset_col}{len(espp) + 4}')
    set_with_dataframe(sh, rs, row=len(espp) + 5, col=char_to_col(loffset_col))
    fmt_set_aud(sh, f'{col_to_char(loffset + 4)}{len(espp) + 3}:{col_to_char(loffset + 10)}')
    fmt_set_leftalign(sh, f'{loffset_col}{len(espp) + 5}:{col_to_char(loffset + 12)}{len(espp) + 5}')


def char_to_col(char: str) -> int:
    'Convert a character to a column number'
    return ord(char) - 64

def col_to_char(col: int) -> str:
    'Convert a column number to a character'
    return chr(col + 64)


def set_with_dataframe(sh: gspread.Worksheet, df: pd.DataFrame, row: int = 1, col: int = 1):
    'Set a DataFrame on a Google Sheet'

    class Formatter(BasicFormatter):
        def format_for_column(self, column, col_number, dataframe):
            if column.dtype.kind == 'M':
                return cellFormat(numberFormat=self.date_format, horizontalAlignment='RIGHT')
            return super().format_for_column(column, col_number, dataframe)

    df_fmtr = Formatter(decimal_format='$ #,##0.00', date_format='yyyy-mm-dd')

    set_with_dataframe_(sh, df, row=row, col=col)
    format_with_dataframe(sh, df, df_fmtr, row=row, col=col, include_column_header=False)


def fmt_set_bold(sh: gspread.Worksheet, range_: str):
    'Set bold text on range of cells'
    format_cell_range(sh, range_, cellFormat(textFormat=textFormat(bold=True)))


def fmt_set_decimal(sh: gspread.Worksheet, range_: str, places: int):
    'Set decimal format on range of cells'
    format_cell_range(
        sh,
        range_,
        cellFormat(
            numberFormat=numberFormat(type='TEXT', pattern=f'0.{"0" * places}'),
            horizontalAlignment='RIGHT',
        ),
    )


def fmt_set_aud(sh: gspread.Worksheet, range_: str):
    'Set AUD currency format on range of cells'
    format_cell_range(
        sh,
        range_,
        cellFormat(
            numberFormat=numberFormat(type='CURRENCY', pattern='$ ##0.00'),
            horizontalAlignment='RIGHT',
        ),
    )


def fmt_set_leftalign(sh: gspread.Worksheet, range_: str):
    'Set left alignment on range of cells'
    format_cell_range(sh, range_, cellFormat(horizontalAlignment='LEFT'))


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
    dfg = (
        pd.json_normalize(
            dataclasses.asdict(obj)
            for obj in grants  # type: ignore[arg-type]
        )
        .convert_dtypes()
        .rename(
            columns={
                'number': 'Grant Number',
                'date': 'Grant Date',
                'qty': 'Grant Qty',
                'vested': 'Vested Qty',
            }
        )
    )
    df_grants = (
        dfg[['Grant Number', 'Grant Date', 'Grant Qty', 'Vested Qty']]
        .sort_values('Grant Date', ascending=False)
        .reset_index(drop=True)
    )

    # Expand list of vests into a DataFrame
    dfv = (
        pd.json_normalize(
            dataclasses.asdict(obj)
            for obj in vests  # type: ignore[arg-type]
        )
        .convert_dtypes()
        .rename(
            columns={
                'period': 'Vest Period',
                'date': 'Vest Date',
                'qty': 'Vest Qty',
                'taxable': 'Taxable USD',
                'grant.number': 'Grant Number',
            }
        )
    )

    # Add period to grant number
    dfv['Grant Number'] = dfv.apply(lambda x: f"{x['Grant Number']}-{x['Vest Period']}", axis=1)

    # Drop extraneous columns, sort by vest date
    df_vests = (
        dfv[['Grant Number', 'Vest Date', 'Vest Qty', 'Taxable USD']].sort_values('Vest Date').reset_index(drop=True)
    )

    # Vertical row offset for vests
    offset = len(df_grants) + 6

    # Create formula columns
    df_vests['Cost Basis'] = df_vests.apply(
        lambda x: f'=D{x.name + offset}/C{x.name + offset}', axis=1
    )
    df_vests['Exch Rate'] = df_vests.apply(
        lambda x: f'=1/VLOOKUP(B{x.name + offset}, RBA!$A:$B, 2, True)', axis=1
    )
    df_vests['Taxable AUD'] = df_vests.apply(
        lambda x: f'=D{x.name + offset}*F{x.name + offset}', axis=1
    )

    return df_grants, df_vests


def selling(df: pd.DataFrame, offset: int, loffset_col: str) -> pd.DataFrame:
    '''
    Parse the selling data, and return as DataFrame

    Params
        df:           DataFrame
        offset:       Vertical row offset
        loffset_col:  ESPP & Sales starts in which column?
    '''
    df_rs = (
        df[
            [
                'Date Sold',
                'Date Acquired',
                'Qty.',
                'Adjusted Cost Basis Per Share',
                'Proceeds Per Share',
                'Total Proceeds',
                'Adjusted Gain/Loss Per Share',
                'Adjusted Gain/Loss',
                'Capital Gains Status',
                'Grant Number',
            ]
        ]
        .rename(
            columns={
                'Qty.': 'Qty',
                'Adjusted Cost Basis Per Share': 'Cost Basis',
                'Total Proceeds': 'Proceeds USD',
                'Adjusted Gain/Loss Per Share': 'CG Per Share',
                'Adjusted Gain/Loss': 'CG Total',
                'Capital Gains Status': 'CG Status',
            }
        )
        .reset_index(drop=True)
    )

    # Mark ESPP sales
    df_rs['Grant Number'] = df_rs['Grant Number'].fillna('ESPP')

    loffset = char_to_col(loffset_col)

    # Create formula columns
    df_rs.insert(
        2, '30 Day Rule', df_rs.apply(lambda x: f'=IF({col_to_char(loffset)}{x.name + offset}<{col_to_char(loffset + 1)}{x.name + offset}+30, "Yes", "No")', axis=1)
    )
    df_rs.insert(
        7, 'Proceeds AUD', df_rs.apply(lambda x: f'=1/VLOOKUP({col_to_char(loffset)}{x.name + offset}, RBA!$A:$B, 2, True)*{col_to_char(loffset + 6)}{x.name + offset}', axis=1)
    )
    df_rs.insert(
        10, 'CG Total AUD', df_rs.apply(lambda x: f'=1/VLOOKUP({col_to_char(loffset)}{x.name + offset}, RBA!$A:$B, 2, True)*{col_to_char(loffset + 9)}{x.name + offset}', axis=1)
    )
    return df_rs


def espping(df: pd.DataFrame, loffset_col: str) -> pd.DataFrame:
    '''
    Parse the ESPP data, and return as DataFrame

    Params
        df:           DataFrame
        loffset_col:  ESPP & Sales starts in which column?
    '''
    df_espp = (
        df[df['Record Type'] == 'Purchase'][
            [
                'Grant Date',
                'Grant Date FMV',
                'Purchase Date',
                'Purchased Qty.',
                'Purchase Price',
                'Purchase Date FMV',
            ]
        ]
        .rename(columns={'Purchased Qty.': 'Qty'})
        .reset_index(drop=True)
    )

    loffset = char_to_col(loffset_col)

    # Fix column types
    df_espp['Qty'] = df_espp['Qty'].astype(int)
    df_espp['Purchase Date FMV'] = df_espp['Purchase Date FMV'].apply(lambda x: decimal.Decimal(x[1:])).astype(float)

    # Create formula columns
    df_espp['Income Per Share'] = df_espp.apply(lambda x: f'={col_to_char(loffset + 5)}{x.name + 3}-{col_to_char(loffset + 4)}{x.name + 3}', axis=1)
    df_espp['Total Income'] = df_espp.apply(lambda x: f'={col_to_char(loffset + 3)}{x.name + 3}*{col_to_char(loffset + 6)}{x.name + 3}', axis=1)
    df_espp['Total Cost USD'] = df_espp.apply(lambda x: f'={col_to_char(loffset + 3)}{x.name + 3}*{col_to_char(loffset + 4)}{x.name + 3}', axis=1)
    df_espp['Total Cost AUD'] = df_espp.apply(
        lambda x: f'={col_to_char(loffset + 8)}{x.name + 3}*1/VLOOKUP({col_to_char(loffset + 2)}{x.name + 3}, RBA!$A:$B, 2, True)', axis=1
    )

    return df_espp
