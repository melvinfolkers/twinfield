import logging
import os
import pandas as pd
import requests
from .functions import select_office
from . import TEMPLATES
from .templates import import_xml
from xml.etree import ElementTree as ET


def add_office_code_to_xml_header(officecode: str, soap_msg: str) -> str:
    """
    Parameters
    ----------
    officecode: str
        officecode used for creating the record in Twinfield
    soap_msg: str
        soap_message in XML containing the data to be inserted

    Returns
    -------
    changed_msg: str
        xml message where the office code is added (or replaced if existed) to the header
    -------

    """

    xml = ET.fromstring(soap_msg)
    xml.find("header/office").text = officecode
    changed_msg = ET.tostring(xml, encoding="utf-8", method="xml").decode("utf-8")

    return changed_msg


def parse_errors(data) -> pd.DataFrame:
    """
    Parameters
    ----------
    data
        dataset containing potential errors

    Returns
    -------
    errors: pd.DataFrame
        DataFrame with errors
    """
    if "msgtype" in data.columns:
        errors = data.loc[data.msgtype == "error"]
    else:
        logging.info("geen errors")
        errors = pd.DataFrame()

    logging.info(f"{len(errors)} errors.")

    return errors


def get_response(script, soap_msg, login, office):
    """
    Parameters
    ----------
    script
        Choosen script for creating record in Twinfield.
    soap_msg
        Soap message for selecte module.
    login
        login parameters (SessionParameters).
    office
        Office code for selected request.

    Returns
    -------
    response
        Response of the Twinfield API.
    """
    select_office(officecode=office, param=login)

    template_file = TEMPLATES.get(script)
    template_xml = import_xml(os.path.join("xml_templates", template_file))
    body = template_xml.format(login.session_id, soap_msg)

    url = f"https://{login.cluster}.twinfield.com/webservices/processxml.asmx?wsdl"
    response = requests.post(url=url, headers=login.header, data=body.encode("utf16"))

    return response
