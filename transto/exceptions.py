from click import ClickException


class MissingEnvVar(ClickException):
    pass

class MissingHsbcPdfPassword(MissingEnvVar):
    def __init__(self):
        super().__init__('You must export HSBC_PDF_PASSWORD with the password for the PDF file.')

class MissingGsuiteOauthCreds(MissingEnvVar):
    def __init__(self):
        super().__init__('You must export your GCP oAuth path as GSUITE_OAUTH_CREDS')
