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

    uid = os.environ.get('SQL_USER_1')
    password = os.environ.get('SQL_PW_1')
    server = os.environ.get('SQL_SERVER_1')
    database = 'landing'
    driver = 'ODBC Driver 17 for SQL Server'

    connectionstring = f'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'

    return connectionstring


def get_blob_credentials():
    engine = create_engine('sqlite:////Users/melvinfolkers/Documents/github/twinfield_flask/twinfield.db')
    df = pd.read_sql_table(table_name='az_settings', con=engine).loc[0]

    container = df.container
    account_name = df.account_name
    account_key = df.account_key

    return container, account_name, account_key
