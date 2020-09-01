import logging
from .functions import SessionParameters


def twinfield_login_faheem():
    user = "Baksif"
    password = "staffing420"
    organisation = "Associates"

    login = SessionParameters(user=user, pw=password, organisation=organisation)

    logging.info("ingelogd met gebruiker {}".format(user))

    return login


def auth_azure():

    uid = os.environ.get("SQL_USER_1")
    password = os.environ.get("SQL_PW_1")
    server = os.environ.get("SQL_SERVER_1")
    database = "landing"
    driver = "ODBC Driver 17 for SQL Server"

    connectionstring = f"mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}"

    return connectionstring
