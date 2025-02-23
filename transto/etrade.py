import dataclasses
import datetime
import decimal
from typing import Optional, Tuple

import gspread
import numpy as np
import pandas as pd
import pytz
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


def to_col(char: str) -> int:
    'Convert a character to a column number'
    return ord(char) - 64


def to_char(col: int) -> str:
    'Convert a column number to a character'
    return chr(col + 64)


VEST_QUANTITY = 'E'
VEST_DATE = 'B'
VEST_TAXABLE_USD = 'D'
VEST_EXCH_RATE = 'F'

ESPP_COLUMN = 'K'
SALES_COLUMN = ESPP_COLUMN

ESPP_QUANTITY = to_char(to_col(ESPP_COLUMN) + 3)
ESPP_PURCHASE_DATE = to_char(to_col(ESPP_COLUMN) + 2)
ESPP_PURCHASE_PRICE = to_char(to_col(ESPP_COLUMN) + 4)
ESPP_PURCHASE_DATE_FMV = to_char(to_col(ESPP_COLUMN) + 5)
ESPP_INCOME_PER_SHARE = to_char(to_col(ESPP_COLUMN) + 6)
ESPP_TOTAL_COST_USD = to_char(to_col(ESPP_COLUMN) + 8)

SALES_QUANTITY = to_char(to_col(SALES_COLUMN) + 3)
SALES_DATE_SOLD = SALES_COLUMN
SALES_COST_BASIS = to_char(to_col(SALES_COLUMN) + 4)
SALES_PROCEEDS_USD = to_char(to_col(SALES_COLUMN) + 5)
SALES_CG_TOTAL = to_char(to_col(SALES_COLUMN) + 9)
SALES_CG_TOTAL_AUD = to_char(to_col(SALES_COLUMN) + 10)
SALES_GRANT_NUMBER = to_char(to_col(SALES_COLUMN) + 12)


def main(vestfile: str, sellfile: str):
    '''
    Parse etrade reports into Google Sheets
    '''
    export(*load_csvs(vestfile, sellfile))


def load_csvs(
    vestfile: str, sellfile: str
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    'Import the Etrade reports from CSV'
    df = pd.read_excel(
        vestfile,
        sheet_name=1,
        parse_dates=['Grant Date', 'Vest Date'],
        date_format={'Grant Date': '%d-%b-%Y', 'Vest Date': '%m/%d/%Y'},
    )
    grants, vests, schedule = vesting(df)

    df = pd.read_excel(
        vestfile,
        sheet_name=0,
        parse_dates=['Purchase Date', 'Grant Date'],
        date_format='%d-%b-%Y',
    )
    espp = espping(df, ESPP_COLUMN)

    df = pd.read_excel(
        sellfile,
        parse_dates=['Date Sold', 'Grant Date', 'Date Acquired'],
        date_format='%m/%d/%Y',
    )
    rs = selling(df, len(espp) + 6, SALES_COLUMN)

    return grants, vests, schedule, espp, rs


def export(grants: pd.DataFrame, vests: pd.DataFrame, schedule: pd.DataFrame, espp: pd.DataFrame, rs: pd.DataFrame):
    'Export the parsed data to Google Sheets'
    sh = auth_gsuite().open_by_key(SPREADO_ID).worksheet('ESS')

    # Grants
    set_title_cell(sh, 'A1', 'Grants')
    set_with_dataframe(sh, grants, row=2)
    fmt_set_leftalign(sh, 'A2:H2')
    fmt_set_rightalign(sh, f'D3:H{len(grants) + 2}')
    fmt_set_aud(sh, f'H3:I{len(grants) + 2}')

    # Vesting Schedule
    SCHEDULE_ROW = len(grants) + 4
    set_title_cell(sh, f'A{SCHEDULE_ROW}', 'Vesting Schedule')
    set_with_dataframe(sh, schedule, row=SCHEDULE_ROW + 1)
    fmt_set_leftalign(sh, f'A{SCHEDULE_ROW + 1}')
    for x in range(3, 10, 2):
        range_ = f'{to_char(x)}{SCHEDULE_ROW + 2}:{to_char(x)}{SCHEDULE_ROW + len(schedule) + 1}'
        fmt_set_rightalign(sh, range_)
        fmt_set_aud(sh, range_)

    # Vests
    VESTS_ROW = SCHEDULE_ROW + len(schedule) + 3
    set_title_cell(sh, f'A{VESTS_ROW}', 'Vests')
    set_with_dataframe(sh, vests, row=VESTS_ROW + 1)
    fmt_set_decimal(sh, f'F{VESTS_ROW + 2}:F', 4)
    fmt_set_aud(sh, f'D{VESTS_ROW + 2}:E')
    fmt_set_aud(sh, f'G{VESTS_ROW + 2}:G')
    fmt_set_leftalign(sh, f'A{VESTS_ROW + 1}:G{VESTS_ROW + 1}')

    # ESPP
    set_title_cell(sh, f'{ESPP_COLUMN}1', 'ESPP')
    set_with_dataframe(sh, espp, row=2, col=to_col(ESPP_COLUMN))
    fmt_set_aud(sh, f'{ESPP_PURCHASE_PRICE}1:{ESPP_TOTAL_COST_USD}{len(espp) + 3}')
    fmt_set_leftalign(sh, f'{ESPP_COLUMN}2:{ESPP_TOTAL_COST_USD}2')

    # Sales
    SALES_ROW = len(espp) + 4
    set_title_cell(sh, f'{SALES_COLUMN}{SALES_ROW}', 'Sales')
    set_with_dataframe(sh, rs, row=SALES_ROW + 1, col=to_col(SALES_COLUMN))
    fmt_set_aud(sh, f'{SALES_COST_BASIS}{SALES_ROW}:{SALES_CG_TOTAL_AUD}')
    fmt_set_leftalign(sh, f'{SALES_COLUMN}{SALES_ROW + 1}:{SALES_GRANT_NUMBER}{SALES_ROW + 1}')
    # fmt_set_plaintext(sh, f'{SALES_GRANT_NUMBER}{SALES_ROW + 1}:{SALES_GRANT_NUMBER}{SALES_ROW + 1}')


def set_with_dataframe(sh: gspread.Worksheet, df: pd.DataFrame, row: int = 1, col: int = 1):
    'Set a DataFrame on a Google Sheet'

    class Formatter(BasicFormatter):
        def format_for_column(self, column, col_number, dataframe):
            'https://numpy.org/doc/stable/reference/generated/numpy.dtype.kind.html'
            # M type is numpy datetime
            if column.dtype.kind == 'M':
                return cellFormat(numberFormat=self.date_format, horizontalAlignment='RIGHT')
            return super().format_for_column(column, col_number, dataframe)

    df_fmtr = Formatter(decimal_format='$ #,##0.00', date_format='yyyy-mm-dd')

    set_with_dataframe_(sh, df, row=row, col=col)
    format_with_dataframe(sh, df, df_fmtr, row=row, col=col, include_column_header=False)


def set_title_cell(sh: gspread.Worksheet, cell: str, title: str):
    'Set a title cell'
    sh.update(cell, [[title]])
    fmt_set_bold(sh, cell)
    fmt_set_leftalign(sh, cell)


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


def fmt_set_plaintext(sh: gspread.Worksheet, range_: str):
    'Set range of cells as plain text'
    format_cell_range(sh, range_, cellFormat(numberFormat=numberFormat(type='TEXT')))


def fmt_set_aud(sh: gspread.Worksheet, range_: str):
    'Set AUD currency format on range of cells'
    format_cell_range(
        sh,
        range_,
        cellFormat(
            numberFormat=numberFormat(type='CURRENCY', pattern='$ #,##0.00'),
            horizontalAlignment='RIGHT',
        ),
    )


def fmt_set_leftalign(sh: gspread.Worksheet, range_: str):
    'Set left alignment on range of cells'
    format_cell_range(sh, range_, cellFormat(horizontalAlignment='LEFT'))


def fmt_set_rightalign(sh: gspread.Worksheet, range_: str):
    'Set right alignment on range of cells'
    format_cell_range(sh, range_, cellFormat(horizontalAlignment='RIGHT'))


def fmt_set_centrealign(sh: gspread.Worksheet, range_: str):
    'Set center alignment on range of cells'
    format_cell_range(sh, range_, cellFormat(horizontalAlignment='CENTER'))


def vesting(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    # Add formula columns
    df_grants['Remaining'] = df_grants.apply(lambda x: f'=D{x.name + 3}-E{x.name + 3}', axis=1)
    df_grants['Quarterly'] = df_grants.apply(lambda x: f'=ROUNDDOWN(D{x.name + 3}/16)', axis=1)
    df_grants['Total USD'] = df_grants.apply(lambda x: f'=Overview!B2*F{x.name + 3}', axis=1)
    df_grants['Approx AUD'] = df_grants.apply(lambda x: f'=H{x.name + 3}*GOOGLEFINANCE("Currency:USDAUD")', axis=1)

    # Add Grants totals row
    df_grants.loc[len(df_grants)] = [
        '',
        np.datetime64('NaT'),
        np.nan,
        np.nan,
        '',
        f'=SUM(G3:G{len(df_grants) + 2})',
        f'=SUM(H3:H{len(df_grants) + 2})',
        f'=SUM(I3:I{len(df_grants) + 2})',
    ]

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

    # Add first vest column
    df_grants = df_grants.join(dfv[dfv['Vest Period'] == 1].set_index('Grant Number')['Vest Date'], on='Grant Number')
    df_grants.insert(2, 'First Vest', df_grants.pop('Vest Date'))

    mel_tz = pytz.timezone('Australia/Melbourne')

    # Create unvested grants DataFrame, filtered for current year-1 thru current year+2
    df_unvested = dfv.copy(deep=False)
    df_unvested['Vest Year'] = df_unvested['Vest Date'].dt.year
    df_unvested = df_unvested[
        (df_unvested['Vest Year'] >= datetime.datetime.now(mel_tz).year - 1)
        & (df_unvested['Vest Year'] <= datetime.datetime.now(mel_tz).year + 2)
    ]

    # Fill in estimated vest quantity for unvested grants
    def vest_qty_for_unvested(x):
        if x['Vest Qty'] == 0:
            return round(x['grant.qty'] / 16)
        return x['Vest Qty']

    df_unvested['Vest Qty'] = df_unvested.apply(vest_qty_for_unvested, axis=1)

    # Determine year-on-year vesting schedule
    df_vest_schedule = (
        df_unvested.groupby(['Vest Year', 'Grant Number'])
        .agg({'Vest Qty': 'sum'})
        .reset_index()
        .pivot(index='Grant Number', columns='Vest Year', values='Vest Qty')
        .fillna(0)
        .astype(int)
        .sort_values('Grant Number', ascending=False)
        .reset_index()
    )

    for i, year in enumerate(range(datetime.datetime.now(mel_tz).year - 1, datetime.datetime.now(mel_tz).year + 3)):
        col_num = (i * 2) + 2
        df_vest_schedule.insert(
            col_num,
            f'{year}.1',
            [f'=Overview!$B$2*{to_char(col_num)}{len(df_grants) + 6 + x}' for x in range(len(df_grants) - 1)],
            allow_duplicates=True,
        )

    SCHD_TOTALS_STRT = len(df_grants) + 6
    SCHD_TOTALS_END = SCHD_TOTALS_STRT + len(df_vest_schedule) - 1

    # Add Vest Schedule totals row
    df_vest_schedule.loc[len(df_vest_schedule)] = [
        '',
        f'=SUM(B{SCHD_TOTALS_STRT}:B{SCHD_TOTALS_END})',
        f'=SUM(C{SCHD_TOTALS_STRT}:C{SCHD_TOTALS_END})',
        f'=SUM(D{SCHD_TOTALS_STRT}:D{SCHD_TOTALS_END})',
        f'=SUM(E{SCHD_TOTALS_STRT}:E{SCHD_TOTALS_END})',
        f'=SUM(F{SCHD_TOTALS_STRT}:F{SCHD_TOTALS_END})',
        f'=SUM(G{SCHD_TOTALS_STRT}:G{SCHD_TOTALS_END})',
        f'=SUM(H{SCHD_TOTALS_STRT}:H{SCHD_TOTALS_END})',
        f'=SUM(I{SCHD_TOTALS_STRT}:I{SCHD_TOTALS_END})',
    ]
    # Add Vest Schedule AUD conversion totals row
    df_vest_schedule.loc[len(df_vest_schedule)] = [
        '',
        '',
        f'=C{SCHD_TOTALS_END + 1}*GOOGLEFINANCE("Currency:USDAUD")',
        '',
        f'=E{SCHD_TOTALS_END + 1}*GOOGLEFINANCE("Currency:USDAUD")',
        '',
        f'=G{SCHD_TOTALS_END + 1}*GOOGLEFINANCE("Currency:USDAUD")',
        '',
        f'=I{SCHD_TOTALS_END + 1}*GOOGLEFINANCE("Currency:USDAUD")',
    ]

    # Add period to grant number
    dfv['Grant Number'] = dfv.apply(lambda x: f"{x['Grant Number']}-{x['Vest Period']}", axis=1)

    # Drop extraneous columns, sort by vest date
    df_vests = (
        dfv[dfv['Vest Qty'] > 0][['Grant Number', 'Vest Date', 'Vest Qty', 'Taxable USD']]
        .sort_values('Vest Date')
        .reset_index(drop=True)
    )

    # Vertical row offset for vests
    offset = len(df_grants) + len(df_vest_schedule) + 9

    # Create formula columns
    df_vests['Cost Basis'] = df_vests.apply(lambda x: f'=D{x.name + offset}/C{x.name + offset}', axis=1)
    df_vests['Exch Rate'] = df_vests.apply(lambda x: f'=1/VLOOKUP(B{x.name + offset}, RBA!$A:$B, 2, True)', axis=1)
    df_vests['Taxable AUD'] = df_vests.apply(lambda x: f'=D{x.name + offset}*F{x.name + offset}', axis=1)

    return df_grants, df_vests, df_vest_schedule


def selling(df: pd.DataFrame, offset: int, loffset_col: str) -> pd.DataFrame:
    '''
    Parse the selling data, and return as DataFrame

    Params
        df:           DataFrame
        offset:       Vertical row offset
        loffset_col:  ESPP & Sales starts in which column?
    '''
    # Drop summary rows above symbols (SQ/XYZ), sort all by date sold
    df = df[df['Record Type'] == 'Sell'].sort_values('Date Sold')

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

    loffset = to_col(loffset_col)

    # Create formula columns
    df_rs.insert(
        2,
        '30 Day Rule',
        df_rs.apply(
            lambda x: f'=IF({to_char(loffset)}{x.name + offset}<{to_char(loffset + 1)}{x.name + offset}+30, "Yes", "No")',
            axis=1,
        ),
    )
    df_rs.insert(
        7,
        'Proceeds AUD',
        df_rs.apply(
            lambda x: f'=1/VLOOKUP({to_char(loffset)}{x.name + offset}, RBA!$A:$B, 2, True)*{to_char(loffset + 6)}{x.name + offset}',
            axis=1,
        ),
    )
    df_rs.insert(
        10,
        'CG Total AUD',
        df_rs.apply(
            lambda x: f'=1/VLOOKUP({to_char(loffset)}{x.name + offset}, RBA!$A:$B, 2, True)*{to_char(loffset + 9)}{x.name + offset}',
            axis=1,
        ),
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

    loffset = to_col(loffset_col)

    # Fix column types
    df_espp['Qty'] = df_espp['Qty'].astype(int)
    df_espp['Purchase Date FMV'] = df_espp['Purchase Date FMV'].apply(lambda x: decimal.Decimal(x[1:])).astype(float)

    # Create formula columns
    df_espp['Income Per Share'] = df_espp.apply(
        lambda x: f'={to_char(loffset + 5)}{x.name + 3}-{to_char(loffset + 4)}{x.name + 3}', axis=1
    )
    df_espp['Total Income'] = df_espp.apply(
        lambda x: f'={to_char(loffset + 3)}{x.name + 3}*{to_char(loffset + 6)}{x.name + 3}', axis=1
    )
    df_espp['Total Cost USD'] = df_espp.apply(
        lambda x: f'={to_char(loffset + 3)}{x.name + 3}*{to_char(loffset + 4)}{x.name + 3}', axis=1
    )
    df_espp['Total Cost AUD'] = df_espp.apply(
        lambda x: f'={to_char(loffset + 8)}{x.name + 3}*1/VLOOKUP({to_char(loffset + 2)}{x.name + 3}, RBA!$A:$B, 2, True)',
        axis=1,
    )

    return df_espp


def refresh_rba_exchange_rate_history():
    'Refresh the RBA exchange rate history'
    sh = auth_gsuite().open_by_key(SPREADO_ID).worksheet('RBA')

    df1 = pd.read_excel(
        'https://www.rba.gov.au/statistics/tables/xls-hist/2018-2022.xls',
        skiprows=11,
        usecols=[0, 1],
        header=None,
        parse_dates=[0],
        date_format=['%d-%b-%Y'],
    )
    df2 = pd.read_excel(
        'https://www.rba.gov.au/statistics/tables/xls-hist/2023-current.xls',
        skiprows=11,
        usecols=[0, 1],
        header=None,
        parse_dates=[0],
        date_format=['%d-%b-%Y'],
    )
    df = pd.concat([df1, df2])
    df[0] = pd.to_datetime(df[0])
    set_with_dataframe_(sh, df, resize=True, include_column_header=False)
    fmt_set_decimal(sh, 'B:B', 4)
