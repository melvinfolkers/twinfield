import logging

import pandas as pd
from sqlalchemy import create_engine

from .functions import SessionParameters

def twinfield_login_faheem():
    user = 'Baksif'
    password = 'staffing420'
    organisation = 'Associates'

    login = SessionParameters(user=user,
                              pw=password,
                              organisation=organisation)

    logging.info('ingelogd met gebruiker {}'.format(user))

    return login

def auth_azure():
    engine = create_engine('sqlite:////Users/melvinfolkers/Documents/github/twinfield_flask/twinfield.db')
    df = pd.read_sql_table(table_name='db_settings', con=engine).loc[0]

    uid = df.username
    password = df.password
    server = df.server
    database = df.database
    driver = 'ODBC Driver 17 for SQL Server'

    connectionstring = 'mssql+pyodbc://{}:{}@{}:1433/{}?driver={}'.format(uid, password, server, database, driver)
    logging.info('ingelogd met gebruiker {}'.format(df.username))

    return connectionstring


def get_blob_credentials():
    engine = create_engine('sqlite:////Users/melvinfolkers/Documents/github/twinfield_flask/twinfield.db')
    df = pd.read_sql_table(table_name='az_settings', con=engine).loc[0]

    container = df.container
    account_name = df.account_name
    account_key = df.account_key

    return container, account_name, account_key
