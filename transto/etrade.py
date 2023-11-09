import datetime
import dataclasses
import decimal

from gspread_dataframe import set_with_dataframe
import pandas as pd

from transto import SPREADO_ID
from transto.auth import gsuite as auth_gsuite


@dataclasses.dataclass
class Grant:
    date: datetime.date
    number: str
    qty: int


@dataclasses.dataclass
class Vest:
    grant: Grant
    period: int
    date: datetime.date
    qty: int
    taxable: decimal.Decimal=None


def main(filename: str) -> pd.DataFrame:
    df = vesting(filename)

    import ipdb; ipdb.set_trace()
    gc = auth_gsuite()
    spreado = gc.open_by_key(SPREADO_ID)
    set_with_dataframe(spreado.worksheet('ESS'), df)

    import ipdb; ipdb.set_trace()
    pass


def espp(filename: str) -> pd.DataFrame:
    'Read the ESPP data'
    df = pd.read_excel(filename, sheet_name=0)

    
    return df


def vesting(filename: str) -> pd.DataFrame:
    'Read the vesting data'
    df = pd.read_excel(
        filename,
        sheet_name=1,
        parse_dates=['Grant Date', 'Vest Date'],
    )

    vests = []

    for _, series in df.iterrows():
        if series['Record Type'] == 'Grant':
            grant = Grant(
                date=series['Grant Date'],
                number=series['Grant Number'],
                qty=series['Granted Qty.'],
            )

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

    return pd.json_normalize(dataclasses.asdict(obj) for obj in vests)
