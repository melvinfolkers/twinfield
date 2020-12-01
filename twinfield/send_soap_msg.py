import logging
import os
import pickle
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient

from . import responses
from . import templates
from .credentials import twinfield_login
from .exceptions import ServerError
from .functions import select_office, RunParameters
from .report import send_insert_message


def check_response_errors(status_dict):
    if status_dict.get("msgtype", "") == "error":
        subject = status_dict["msg"].split("//")[0]
        body = status_dict["msg"].split("//")[1]
        logging.info(
            f"error bij het inschieten van transactie:\nOnderwerp: {subject}\nDetail: {body}"
        )
        raise ServerError(
            f"fout bij inschieten van transactie:\nOnderwerp: {subject}\nDetail: {body}"
        )


def save_xml_locally(run_params, response, msg_id):

    foldername = datetime.now().strftime("%Y%m%d")
    path = os.path.join(run_params.responsedir, foldername)
    if not os.path.exists(path):
        filedir = RunParameters.create_dir(path)
    else:
        filedir = path
    filename = f"response_{msg_id}_{datetime.now().strftime('%H_%M_%S')}.xml"
    file_path = os.path.join(filedir, filename)
    f = open(file_path, "w")
    f.write(response)
    f.close()

    if run_params.debug:
        filename_blob = os.path.join("xml_responses", "test", foldername, filename)
    else:
        filename_blob = os.path.join("xml_responses", run_params.modules, foldername, filename)

    return file_path, filename_blob


def create_blob_service_client():

    connect_str = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={}".format(
        os.environ.get("ls_blob_account_name"), os.environ.get("ls_blob_account_key")
    )

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    return blob_service_client


def response_to_blob(file_path, filename):

    blob_service_client = create_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=os.environ.get("ls_blob_container_name"),
        blob=filename,
    )

    logging.info(f"start uploading blob {filename}...")
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {filename}!")


def export_response(run_params, response, msg_id):

    file_path, filename = save_xml_locally(run_params, response, msg_id)
    response_to_blob(file_path, filename)


def parse_errors(run_params, data):
    if "msgtype" in data.columns:
        errors = data.loc[data.msgtype == "error"]
        # raise KostenPlaatsError(f"{len(errors)} kostenplaatsen ontbreken in TwinField.")
    else:
        logging.info("geen errors")
        errors = pd.DataFrame()

    errors.to_pickle(os.path.join(run_params.pickledir, "response_errors.pkl"))
    logging.info(f"{len(errors)} errors geexporteerd.")


def get_response(messages, run_params, login):
    ttl = pd.DataFrame()
    msg_id = 0
    for msg in tqdm(messages):
        msg_id += 1

        soap_body = msg.get("xml_msg")
        officecode = msg.get("office_code")
        select_office(officecode=officecode, param=login)

        if run_params.modules == "vrk":

            soap_msg = templates.import_xml("xml_templates/template_transactions.xml").format(
                login.session_id, soap_body
            )
        elif run_params.modules == "ink":
            soap_msg = templates.import_xml("xml_templates/template_transactions.xml").format(
                login.session_id, soap_body
            )
        elif run_params.modules == "memo":
            soap_msg = templates.import_xml("xml_templates/template_transactions.xml").format(
                login.session_id, soap_body
            )
        elif run_params.modules == "ljp":
            soap_msg = templates.import_xml("xml_templates/template_concept.xml").format(
                login.session_id, soap_body
            )
        elif run_params.modules == "salesinvoice":
            soap_msg = templates.import_xml("xml_templates/template_salesinvoices.xml").format(
                login.session_id, soap_body
            )
        elif run_params.modules == "read_dimensions":
            soap_msg = templates.import_xml("xml_templates/read_dimensions.xml").format(
                login.session_id, msg.get("dim_type")
            )
        elif run_params.modules == "upload_dimensions":
            soap_msg = templates.import_xml("xml_templates/upload_dimensions.xml").format(
                login.session_id, soap_body
            )
        else:
            raise ServerError(f"geen routine voor {run_params.modules}")

        url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(login.cluster)
        response = requests.post(url=url, headers=login.header, data=soap_msg.encode("utf16"))

        if run_params.modules != "read_dimensions" and run_params.modules != "upload_dimensions":
            export_response(run_params, response.text, msg_id)

        data = responses.parse_response(run_params, response, login)
        data["msg_id"] = msg_id
        ttl = pd.concat([ttl, data], sort=False, ignore_index=True)

    export_response_data(ttl, run_params)

    send_insert_message(table=ttl, messages=messages, run_params=run_params)

    return ttl


def export_response_data(df, run_params):
    if run_params.modules == "upload_dimensions":
        return None

    df["rundate"] = run_params.starttijd
    df["kenmerk"] = run_params.modules

    if run_params.modules == "read_dimensions":
        filename = "response_data.pkl"
    else:
        filename = f"response_data_{run_params.starttijd.strftime('%Y%m%d%H%M')}.pkl"

    df.to_pickle(os.path.join(run_params.pickledir, filename))


def upload_soap(run_params, messages) -> pd.DataFrame:
    login = twinfield_login()

    if run_params.upload:
        r = get_response(messages, run_params, login)

        parse_errors(run_params, r)
        logging.info("de soap messages zijn verstuurd!")

    else:
        r = pd.DataFrame()
        logging.info("uploaden staat uit.")

    return r


def run(run_params) -> pd.DataFrame:
    with open(os.path.join(run_params.pickledir, "messages.pkl"), "rb") as f:
        messages = pickle.load(f)

    r = upload_soap(run_params, messages)

    return r
