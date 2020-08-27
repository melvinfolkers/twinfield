
import codecs
from sqlalchemy import create_engine
from azure.storage.blob import BlockBlobService
import os
import logging
from datetime import datetime

from scripts.credentials import auth_azure
from .mailing import send_mail
from scripts.credentials import get_blob_credentials

import pandas_gbq as gbq




def str_decoding(base64_str):
    decoded = codecs.decode(base64_str.encode(), 'base64').decode()

    return decoded


def push_to_azure(df, tablename):


    df = remove_special_chars(df)

    connectionstring = auth_azure()
    engn = create_engine(connectionstring, pool_size=10, max_overflow=20)
    df.to_sql(tablename, engn, chunksize=100000, if_exists='replace', index=False)
    numberofcolumns = str(len(df.columns))

    result = 'push successful ({}):'.format(tablename), len(df), 'records pushed to Microsoft Azure','({} columns)'.format(numberofcolumns)
    logging.info(result)

def upload_to_blob(df, tablename,stagingdir):
    container, account_name, account_key = get_blob_credentials()

    df = remove_special_chars(df)

    full_path_to_file = os.path.join(stagingdir, tablename + '.csv')
    df.to_csv(full_path_to_file, index=False)    # export file to staging

    try:
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(account_name=account_name,
                                              account_key=account_key)

        block_blob_service.create_container(container)

        logging.info('Uploading to Blob storage as blob {}'.format(tablename))

        # Upload the  file, use tablename for the blob name
        block_blob_service.create_blob_from_path(container, tablename + '/' + tablename, full_path_to_file)

        logging.info('Upload {} to blob done!'.format(tablename))
    except Exception as e:
        print(e)


def remove_special_chars(df):

    oldlist = df.columns
    newlist = [(x.replace('/', '_').replace('-', '_').replace(' ', '_')) for x in oldlist]
    df.columns = newlist

    #df = df.apply(lambda x: x.str.replace(r'\n', ''), axis=0)

    return df


def upload_data(name, data,start,run_params):

    tablename = 'twinfield_{}'.format(name)

    push_to_azure(data.head(n=0), tablename) # zorg dat het schema in met juiste veldeigenschappen klaarstaat in Azure (o regels)
    upload_to_blob(data, tablename, run_params.stagingdir)

    send_mail(subject='ADF: Twinfield data {} geupload'.format(name),
                  message='uploaddtijd: {} \naantal transacties: {}'.format(str(datetime.now() - start,len(data) )))


    logging.info('Finished in {} \n number of transactions: {}'.format(datetime.now() - start,len(data) ))




def push_bigquery(df, containername, foldername ,tablename):


    df['transactie_omschrijving'] = df.transactie_omschrijving.str.replace('\W+',' ')
    starttime = datetime.now()
    logging.info(f'aantal rijen: {len(df)} aantal kolommen: {len(df.columns)}')
    logging.info(f'start met uploaden van {len(df)} records naar google bigquery... ({containername} - {foldername} - {tablename})')
    gbq.to_gbq(df, f'{foldername}.{tablename}', containername, if_exists ='replace')

    logging.info('cloud upload success! tijd: {}'.format(str(datetime.now() - starttime)))


