import functools
import re
from typing import Dict, List

import pandas as pd
import yaml
from gspread_dataframe import get_as_dataframe, set_with_dataframe

from transto import SPREADO_ID
from transto.auth import gsuite as auth_gsuite


@functools.lru_cache(maxsize=1)
def load_mapping() -> dict:
    'Load transaction mapping data'
    # Fetch mapping as DataFrame
    df = get_as_dataframe(_get_mapping_sheet(), usecols=[0, 1, 2])

    # Convert tablular data to a tree
    mapping: Dict[str, Dict[str, List[str]]] = {}

    for _, item in df.iterrows():
        if item['topcat'] not in mapping:
            mapping[item['topcat']] = {}

        if item['seccat'] not in mapping[item['topcat']]:
            mapping[item['topcat']][item['seccat']] = []

        mapping[item['topcat']][item['seccat']].append(str(item['searchterm']))

    return mapping


def write_mapping_sheet_from_yaml():
    'Read YAML and merge with gsheet data, before updating gsheet'
    with open('mapping.yaml', encoding='utf8') as f:
        tree = yaml.safe_load(f).get('mapping')

    # Convert YAML tree to a flattened list
    data = [
        (topcat, seccat, searchterm)
        for topcat, seccats in sorted(tree.items())
        for seccat, searchterms in sorted(seccats.items())
        for searchterm in sorted(searchterms)
    ]

    sheet = _get_mapping_sheet()

    # Fetch current gsheet as DataFrame
    df = get_as_dataframe(sheet).fillna('')

    # Join YAML data with upstream gsheet to persist comments
    merged = df.merge(
        pd.DataFrame(data, columns=['topcat', 'seccat', 'searchterm']),
        on=['topcat', 'seccat', 'searchterm'],
        how='outer',
    )

    set_with_dataframe(sheet, merged, resize=True)


def write_yaml_from_mapping_sheet():
    'Pull gsheet mapping and write to YAML'
    mapping = load_mapping()

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
