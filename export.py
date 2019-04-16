
import codecs
from sqlalchemy import create_engine
from azure.storage.blob import BlockBlobService
import os
import logging

def auth_azure():
    uid = 'lavastormadmin'
    password = 'cGFzc0B3b3JkMQ==\n'
    server = 'sa-lavastorm.database.windows.net'
    database = 'landing'
    driver = 'ODBC Driver 13 for SQL Server'

    connectionstring = f'mssql+pyodbc://{uid}:{str_decoding(password)}@{server}:1433/{database}?driver={driver}'

    return connectionstring


def str_decoding(base64_str):
    decoded = codecs.decode(base64_str.encode(), 'base64').decode()

    return decoded


def push_to_azure(df, tablename):


    df = remove_special_chars(df)

    connectionstring = auth_azure()
    engn = create_engine(connectionstring, pool_size=10, max_overflow=20)
    df.to_sql(tablename, engn, chunksize=100000, if_exists='replace', index=False)
    numberofcolumns = str(len(df.columns))

    result = f'push successful ({tablename}):', len(df), 'records pushed to Microsoft Azure', f'({numberofcolumns} columns)'
    logging.info(result)

def upload_to_blob(df, tablename,stagingdir, container = 'staffing-twinfield'):

    df = remove_special_chars(df)

    full_path_to_file = os.path.join(stagingdir, tablename + '.csv')
    df.to_csv(full_path_to_file, index=False)    # export file to staging

    try:
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(account_name='staffingeu2',
                                              account_key='hRR5Mkf7i6JFGa18RC3gA5nClnzpnr0+74hTSMql2oHzcLmxyHyiAz7nljNDHyh0dUyCnUz+EE6BIGs6rpMLhw==')

        block_blob_service.create_container(container)

        logging.info(f'Uploading to Blob storage as blob {tablename}')

        # Upload the  file, use tablename for the blob name
        block_blob_service.create_blob_from_path(container, tablename, full_path_to_file)

        logging.info(f'Upload {tablename} to blob done!')
    except Exception as e:
        print(e)


def remove_special_chars(df):

    oldlist = df.columns
    newlist = [(x.replace('/', '_').replace('-', '_').replace(' ', '_')) for x in oldlist]
    df.columns = newlist

    #df = df.apply(lambda x: x.str.replace(r'\n', ''), axis=0)

    return df


