import logging
import os
from datetime import datetime

import pandas_gbq as gbq
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine

from scripts.run_settings import get_blob_settings, auth_azure


def push_to_azure(df, tablename):
    df = remove_special_chars(df)

    connectionstring = auth_azure()
    engn = create_engine(connectionstring, pool_size=10, max_overflow=20)
    df.to_sql(tablename, engn, chunksize=100000, if_exists="replace", index=False)
    numberofcolumns = str(len(df.columns))

    result = (
        "push successful ({}):".format(tablename),
        len(df),
        "records pushed to Microsoft Azure",
        "({} columns)".format(numberofcolumns),
    )
    logging.info(result)


def create_blob_client(tablename):

    connect_str, container_name = get_blob_settings()
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    try:
        blob_service_client.create_container(container_name)
    except:
        logging.info("Container already exists.")

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{tablename}/{tablename}")

    return blob_client


def upload_to_blob(df, tablename, stagingdir):
    df = remove_special_chars(df)

    full_path_to_file = os.path.join(stagingdir, tablename + ".csv")
    df.to_csv(full_path_to_file, index=False)  # export file to staging

    blob_client = create_blob_client(tablename)

    logging.info(f"start uploading blob {tablename}...")
    with open(full_path_to_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {tablename}!")


def remove_special_chars(df):
    oldlist = df.columns
    newlist = [(x.replace("/", "_").replace("-", "_").replace(" ", "_")) for x in oldlist]
    df.columns = newlist

    # df = df.apply(lambda x: x.str.replace(r'\n', ''), axis=0)

    return df


def upload_data(name, data, start, run_params):
    tablename = "{}".format(name)

    push_to_azure(data.head(n=0), tablename)
    upload_to_blob(data, tablename, run_params.stagingdir)

    logging.info("Finished in {} \n number of transactions: {}".format(datetime.now() - start, len(data)))


def push_bigquery(df, containername, foldername, tablename):
    df["transactie_omschrijving"] = df.transactie_omschrijving.str.replace("\W+", " ")
    starttime = datetime.now()
    logging.info(f"aantal rijen: {len(df)} aantal kolommen: {len(df.columns)}")
    logging.info(
        f"start met uploaden van {len(df)} records naar google bigquery... ({containername} - {foldername} - {tablename})"
    )
    gbq.to_gbq(df, f"{foldername}.{tablename}", containername, if_exists="replace")

    logging.info("cloud upload success! tijd: {}".format(str(datetime.now() - starttime)))
