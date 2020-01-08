from .functions import SessionParameters
import pandas as pd
from sqlalchemy import create_engine
import logging

def twinfield_login():

    engine = create_engine('sqlite:////Users/melvinfolkers/Documents/github/twinfield_flask/twinfield.db')
    df = pd.read_sql_table(table_name='tw_settings', con=engine).loc[0]

    user = df.username
    password = df.password
    organisation = df.organisation

    login = SessionParameters(user=user,
                              pw=password,
                              organisation= organisation)

    logging.info('ingelogd met gebruiker {}'.format(df.username))

    return login


def auth_azure():
    engine = create_engine('sqlite:////Users/melvinfolkers/Documents/github/twinfield_flask/twinfield.db')
    df = pd.read_sql_table(table_name='db_settings', con=engine).loc[0]

    uid = df.username
    password = df.password
    server = df.server
    database = df.database
    driver = 'ODBC Driver 13 for SQL Server'

    connectionstring = 'mssql+pyodbc://{}:{}@{}:1433/{}?driver={}'.format(uid,password,server,database, driver)
    logging.info('ingelogd met gebruiker {}'.format(df.username))

    return connectionstring


def get_blob_credentials():
    engine = create_engine('sqlite:////Users/melvinfolkers/Documents/github/twinfield_flask/twinfield.db')
    df = pd.read_sql_table(table_name='az_settings', con=engine).loc[0]

    container = df.container
    account_name = df.account_name
    account_key = df.account_key

    return container, account_name, account_key
