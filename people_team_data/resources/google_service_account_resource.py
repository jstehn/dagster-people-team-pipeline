import json

from dagster import EnvVar, InitResourceContext, resource
from google.oauth2.service_account import Credentials


@resource
def google_service_account(context: InitResourceContext) -> Credentials:
    """A resource that provides Google Service Account credentials."""
    credentials_info = json.loads(
        EnvVar("GOOGLE_SERVICE_ACCOUNT_CREDENTIALS").get_value()
    )
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return Credentials.from_service_account_info(
        credentials_info, scopes=scopes
    )
