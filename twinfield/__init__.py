import logging
import os
from .exceptions import EnvironmentVariablesError


logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(levelname)-5s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

MODULES = {
    "100": "openstaande_debiteuren",
    "200": "openstaande_crediteuren",
    "030_1": "mutaties",
    "040_1": "consolidatie",
    "dimensions_deb": "stamgegevens_debiteuren",
    "dimensions_crd": "stamgegevens_crediteuren",
    "dimensions_kpl": "stamgegevens_kpl",
}


# Test if environment variables are set for Twinfield login
TW_USER_LS = os.environ.get("TW_USER_LS")
TW_PW_LS = os.environ.get("TW_PW_LS")
TW_ORG_LS = os.environ.get("TW_ORG_LS")

if not all([TW_USER_LS, TW_PW_LS, TW_ORG_LS]):
    raise EnvironmentVariablesError(
        "One of the environment variables TW_USERS_LS, TW_PW_LS, TW_ORG_LS is not set"
    )
