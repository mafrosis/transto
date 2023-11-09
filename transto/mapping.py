import functools

from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
import re
import yaml

from transto import SPREADO_ID
from transto.auth import gsuite as auth_gsuite


@functools.lru_cache(maxsize=1)
def load_mapping() -> dict:
    'Load transaction mapping data'
    with open('mapping.yaml', encoding='utf8') as f:
        return yaml.safe_load(f).get('mapping')


def write_mapping_sheet_from_yaml():
    'Read YAML and merge with gsheet data, before updating gsheet'
    tree = load_mapping()

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

    set_with_dataframe(sheet, merged, include_column_header=False, resize=True)


def write_yaml_from_mapping_sheet():
    'Pull gsheet mapping and write to YAML'
    # Fetch mapping as DataFrame
    df = get_as_dataframe(_get_mapping_sheet())

    # Convert tablular data to a tree
    mapping = {}
    for _, item in df.iterrows():
        if item[0] not in mapping:
            mapping[item[0]] = {}

        if item[1] not in mapping[item[0]]:
            mapping[item[0]][item[1]] = []

        mapping[item[0]][item[1]].append(item[2])

    class Dumper(yaml.Dumper):
        def increase_indent(self, *args, flow=False, **kwargs):  # pylint: disable=unused-argument
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
