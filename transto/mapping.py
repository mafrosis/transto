import functools
import re

import pandas as pd
import yaml
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from transto import SPREADO_ID
from transto.auth import gsuite as auth_gsuite


@functools.lru_cache(maxsize=1)
def load_mapping() -> (dict[str, dict[str, list[str]]], dict[str, str]):
    '''
    Load transaction mapping data

    Returns:
        mapping:  Nested dict of topcat->seccat->pattern
        comments: Dict of pattern->comment, which must be persisted via write_mapping
    '''
    # Fetch mapping as DataFrame
    df = get_as_dataframe(_get_mapping_sheet(), usecols=[0, 1, 2, 3], header=None)
    df.columns = ['topcat', 'seccat', 'pattern', 'comment']

    # Convert NA values in pattern & comment to empty string
    df[['pattern', 'comment']] = df[['pattern', 'comment']].fillna('')

    # Convert tablular data to a tree
    mapping: dict[str, dict[str, list[str]]] = {}
    comments: dict[str, str] = {}

    for _, item in df.iterrows():
        if item['topcat'] not in mapping:
            mapping[item['topcat']] = {}

        if item['seccat'] not in mapping[item['topcat']]:
            mapping[item['topcat']][item['seccat']] = []

        mapping[item['topcat']][item['seccat']].append(item['pattern'])
        comments[item['pattern']] = item['comment']

    return mapping, comments


def write_mapping(mapping: dict[str, dict[str, list[str]]], comments: dict[str, str]):
    '''
    Write mapping dictionary back to Google Sheets

    Args:
        mapping: Dict in same format as returned by load_mapping()
    '''
    # Convert mapping dict to flattened DataFrame
    data = [
        {'topcat': topcat, 'seccat': seccat, 'pattern': pattern, 'comment': comments.get(pattern, '')}
        for topcat, seccats in mapping.items()
        for seccat, patterns in seccats.items()
        for pattern in patterns
    ]

    df = pd.DataFrame(data).sort_values(['topcat', 'seccat', 'pattern'])

    # Write to sheet
    set_with_dataframe(_get_mapping_sheet(), df, resize=True)


def write_mapping_sheet_from_yaml():
    'Read YAML and merge with gsheet data, before updating gsheet'
    with open('mapping.yaml', encoding='utf8') as f:
        tree = yaml.safe_load(f).get('mapping')

    # Convert YAML tree to a flattened list
    data = [
        (topcat, seccat, pattern)
        for topcat, seccats in sorted(tree.items())
        for seccat, patterns in sorted(seccats.items())
        for pattern in sorted(patterns)
    ]

    sheet = _get_mapping_sheet()

    # Fetch current gsheet as DataFrame
    df = get_as_dataframe(sheet).fillna('')

    # Join YAML data with upstream gsheet to persist comments
    merged = df.merge(
        pd.DataFrame(data, columns=['topcat', 'seccat', 'pattern']),
        on=['topcat', 'seccat', 'pattern'],
        how='outer',
    )

    set_with_dataframe(sheet, merged, resize=True)


def write_yaml_from_mapping_sheet():
    'Pull gsheet mapping and write to YAML'
    mapping, _ = load_mapping()

    class Dumper(yaml.Dumper):
        def increase_indent(self, *args, flow=False, **kwargs):  # noqa: ARG002
            return super().increase_indent(flow=flow, indentless=False)

    # Inject newline above topcat
    lines = yaml.dump({'mapping': mapping}, indent=2, Dumper=Dumper)

    output = []
    for line in reversed(list(lines.splitlines())):
        output.append(line)

        # Match top category and insert a newline next
        if re.match(r'[ ]{2}[\w]*:', line):
            output.append('')

    with open('mapping.yaml', 'w', encoding='utf8') as f:
        f.write(output)


def _get_mapping_sheet():
    gc = auth_gsuite()
    spreado = gc.open_by_key(SPREADO_ID)
    return spreado.worksheet('mapping')
