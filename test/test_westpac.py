import datetime
from pathlib import Path
from unittest.mock import patch

from transto.westpac import cc, parse_transactions, parsepdf


def dt(datestr: str) -> datetime.datetime:
    'Build the naive datetimes emitted by the parser'
    return datetime.datetime.strptime(datestr, '%d %b %y')  # noqa: DTZ007


def write_pdf(tmp_path: Path, lines: list[str]) -> Path:
    '''
    Build a minimal single-page PDF containing one text row per line, mimicking
    the layout-mode output of a Westpac statement
    '''
    content = '\n'.join(f'BT /F1 10 Tf 40 {700 - 20 * i} Td ({line}) Tj ET' for i, line in enumerate(lines))
    stream = content.encode()

    objs = [
        b'<< /Type /Catalog /Pages 2 0 R >>',
        b'<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
        (
            b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 842 595] '
            b'/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>'
        ),
        b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>',
        b'<< /Length ' + str(len(stream)).encode() + b' >>\nstream\n' + stream + b'\nendstream',
    ]

    pdf = b'%PDF-1.4\n'
    offsets = []
    for i, obj in enumerate(objs, 1):
        offsets.append(len(pdf))
        pdf += f'{i} 0 obj\n'.encode() + obj + b'\nendobj\n'

    xref_pos = len(pdf)
    pdf += f'xref\n0 {len(objs) + 1}\n0000000000 65535 f \n'.encode()
    pdf += b''.join(f'{off:010d} 00000 n \n'.encode() for off in offsets)
    pdf += f'trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF'.encode()

    path = tmp_path / 'statement.pdf'
    path.write_bytes(pdf)
    return path


def test_parse_transactions_debit():
    'Debit rows have the amount glued to the description'
    lines = ['08 Dec 25                                        8.40EXAMPLE CAFE            MELBOURNE       AUS']

    assert parse_transactions(lines) == [[dt('08 Dec 25'), '', 'EXAMPLE CAFE MELBOURNE AUS', '8.40']]


def test_parse_transactions_credit():
    'Credit rows carry a trailing " -" flag and are emitted as negative amounts'
    lines = ['16 Dec 25                                       63.93 -CRED VOUCHER']

    assert parse_transactions(lines) == [[dt('16 Dec 25'), '', 'CRED VOUCHER', '-63.93']]


def test_parse_transactions_comma_amount():
    'Thousands separators survive parsing'
    lines = ['07 Jan 26                                   7,833.39 -PAYMENT-BPAY-THANK YOU']

    assert parse_transactions(lines) == [[dt('07 Jan 26'), '', 'PAYMENT-BPAY-THANK YOU', '-7,833.39']]


def test_parse_transactions_skips_non_transaction_rows():
    'Headers, continuation rows and prose are skipped'
    lines = [
        'Date of              Description                                Debits               Credits (-)',
        'Transaction',
        '                                        EXAMPLE MERCHANT SOMEWHERE AUS',
        'Page 3 of 4',
        '08 Dec 25                                        5.00',
    ]

    assert parse_transactions(lines) == []


def test_parse_transactions_fee_rows():
    'Card fee rows parse like any other transaction'
    lines = ['22 Dec 25                                      150.00CARD FEE']

    assert parse_transactions(lines) == [[dt('22 Dec 25'), '', 'CARD FEE', '150.00']]


def test_parsepdf(tmp_path):
    'End-to-end: rows extracted from a generated PDF in layout mode'
    path = write_pdf(
        tmp_path,
        [
            'Some header prose which should be ignored',
            '08 Dec 25                                        8.40EXAMPLE CAFE            MELBOURNE       AUS',
            '09 Dec 25                                       12.34EXAMPLE MERCHANT        SOMEWHERE       AUS',
            '16 Dec 25                                       63.93 -CRED VOUCHER',
        ],
    )

    with path.open('rb') as f:
        trans = parsepdf(f)

    assert trans == [
        [dt('08 Dec 25'), '', 'EXAMPLE CAFE MELBOURNE AUS', '8.40'],
        [dt('09 Dec 25'), '', 'EXAMPLE MERCHANT SOMEWHERE AUS', '12.34'],
        [dt('16 Dec 25'), '', 'CRED VOUCHER', '-63.93'],
    ]


@patch('transto.westpac.categorise', side_effect=lambda df: (df, 0))
@patch('transto.westpac.commit')
def test_cc(mock_commit, mock_categorise, tmp_path):
    'Signs match the hsbc convention: spend negative, credits positive'
    path = write_pdf(
        tmp_path,
        [
            '08 Dec 25                                        8.40EXAMPLE CAFE            MELBOURNE       AUS',
            '16 Dec 25                                       63.93 -CRED VOUCHER',
            '07 Jan 26                                    1,234.56EXAMPLE MERCHANT        SOMEWHERE       AUS',
        ],
    )

    with path.open('rb') as f:
        cc(f)

    df = mock_commit.call_args[0][0]
    assert mock_commit.call_args[0][1] == 'WESTPAC'
    assert mock_commit.call_args[0][2] == 'credit'

    assert list(df['amount']) == [-8.40, 63.93, -1234.56]
