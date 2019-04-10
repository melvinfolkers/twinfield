import pandas_gbq as gbq
from datetime import datetime
import codecs
from sqlalchemy import create_engine
from azure.storage.blob import BlockBlobService
import os


def push_bigquery(df, folder,filename):
    # 5.2 exporteren naar google bigquery

    #exporteer het naar google bigquery
    starttime = datetime.now()
    print('aantal rijen:', len(df), 'aantal kolommen:', len(df.columns))
    print('start met uploaden van', len(df), 'records naar google bigquery...')
    gbq.to_gbq(df, f'{folder}.{filename}', 'yellowstacks-217623', if_exists ='replace')

    print('cloud upload success! tijd:', datetime.now() - starttime)


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
    print(result)

def upload_to_blob(df, tablename, container = 'staffing-twinfield'):

    df = remove_special_chars(df)

    df.to_csv(tablename + '.csv', index=False)

    try:
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(account_name='staffingeu1',
                                              account_key='NTBe2jAEUgrDZZ+J0caCVOTGEwksnYaHpOczEHkyz7d2JbgXvugr7n6Am2GSFUhIyuNIIpVsLMri2IukhgPgUw==')


        block_blob_service.create_container(container)

        # Determine file to upload
        full_path_to_file = os.path.join(os.getcwd(), tablename +  '.csv')

        print("\nUploading to Blob storage as blob " + tablename)

        # Upload the created file, use local_file_name for the blob name
        block_blob_service.create_blob_from_path(container, tablename, full_path_to_file)

    except Exception as e:
        print(e)


def remove_special_chars(df):

    #.replace('.', '_')

    oldlist = df.columns
    newlist = [(x.replace('/', '_').replace('-', '_').replace(' ', '_')) for x in oldlist]
    df.columns = newlist

    df = df.apply(lambda x: x.str.replace(r'\n', ''), axis=0)

    return df


