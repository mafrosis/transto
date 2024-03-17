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

    return gspread.oauth(
        credentials_filename=oauth_creds_path,
        authorized_user_filename='authorized_user.json'
    )
