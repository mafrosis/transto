import logging
import os

import gspread

from transto.exceptions import MissingGsuiteOauthCreds

logger = logging.getLogger('transto')


def gsuite():
    '''
    Authenticate to Google APIs
    '''
    oauth_creds_path = os.environ.get('GSUITE_OAUTH_CREDS')
    if not oauth_creds_path:
        raise MissingGsuiteOauthCreds

    with open(oauth_creds_path, encoding='utf8') as f:
        if 'service_account' in f.read():
            return gspread.service_account(filename=oauth_creds_path)

    return gspread.oauth(credentials_filename=oauth_creds_path)
