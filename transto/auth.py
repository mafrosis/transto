import logging
import os
import sys

import gspread


logger = logging.getLogger('transto')


def gsuite():
    '''
    Authenticate to Google APIs
    '''
    oauth_creds_path = os.environ.get('GSUITE_OAUTH_CREDS')
    if not oauth_creds_path:
        logger.error('You must export your GCP oAuth path as GSUITE_OAUTH_CREDS')
        sys.exit(2)

    return gspread.oauth(
        credentials_filename=oauth_creds_path,
        authorized_user_filename='authorized_user.json'
    )
