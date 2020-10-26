import logging
import sys
import yaml
from scripts.functions import RunParameters, SessionParameters
import os
import sentry_sdk

## staffing-twinfield sentry url
sentry_sdk.init("https://0bb6ff7cb1f54bff82475d8c91c67c4b@o408709.ingest.sentry.io/5407767")


def get_settings(file_name):
    with open(file_name, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def check_shell_variables():

    count_shell_vars = len(sys.argv) - 1
    if count_shell_vars == 0:
        return False
    elif sys.argv[1].endswith(".yml") == False:
        return False
    else:
        logging.info("shell variables detected")
        return True


def get_run_settings(yml_file):
    shell_vars = check_shell_variables()

    if shell_vars:
        logging.info(f"using shell vars!, the name of the yml file is presumed to be: {sys.argv[1]}")
        settings = get_settings(sys.argv[1])
    else:
        settings = get_settings(yml_file)

    logging.info("*** using setting file: {} ***".format(settings))

    return settings


def get_twinfield_settings():

    client_settings = get_settings("yml/custom/twinfield_settings.yml")

    USER = client_settings["user"]
    PASSWORD = client_settings["password"]
    ORGANISATION = client_settings["organisation"]

    login = SessionParameters(user=USER, pw=PASSWORD, organisation=ORGANISATION)

    logging.info("ingelogd met gebruiker {}".format(USER))

    return login


def get_blob_settings():

    yml = get_settings("yml/custom/blob.yml")
    CONNECTION_STRING = yml["connection_string"]
    CONTAINER_NAME = yml["container_name"]

    return CONNECTION_STRING, CONTAINER_NAME


def set_run_parameters(yml_file):

    settings = get_run_settings(yml_file)

    JAAR = settings["jaar"]
    REFRESH = settings["refresh"]
    UPLOAD = settings["upload"]
    MODULES = UPLOAD = settings["modules"]
    run_params = RunParameters(jaar=JAAR, refresh=REFRESH, upload=UPLOAD, modules=MODULES)

    return run_params


def auth_azure():

    uid = os.environ.get("SQL_USER_1")
    password = os.environ.get("SQL_PW_1")
    server = os.environ.get("SQL_SERVER_1")
    database = "landing"
    driver = "ODBC Driver 17 for SQL Server"

    connectionstring = f"mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}"

    return connectionstring
