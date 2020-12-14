import logging
from datetime import datetime
import pandas as pd
import requests
from . import templates
from twinfield.functions import parse_session_response, parse_response, get_metadata


def read_offices(param) -> pd.DataFrame:
    """
    Parameters
    ----------
    param
        login class with twinfield credentials

    Returns
    -------
    data: pd.DataFrame
        dataframe containing a list of offices available
    """

    url = f"https://{param.cluster}.twinfield.com/webservices/processxml.asmx?wsdl"
    body = templates.import_xml("xml_templates/template_list_offices.xml").format(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = parse_session_response(response, param)

    return data


def read_module(param, periode, module) -> pd.DataFrame:
    """
    Parameters
    ----------
    param
        login class with twinfield credentials
    periode
        scope of period in request.
    module
        module nummer om uit te vragen

    Returns
    -------
    data: pd.DataFrame
        dataframe containing data from browse code 100
    """

    start = datetime.now()

    logging.debug(f"start request periode van {periode['from']} t/m {periode['to']}")

    url = f"https://{param.cluster}.twinfield.com/webservices/processxml.asmx?wsdl"
    body = templates.import_xml(f"xml_templates/template_{module}.xml").format(
        param.session_id, periode["from"], periode["to"]
    )
    response = requests.post(url=url, headers=param.header, data=body)

    data = parse_response(response, param)
    logging.debug(f"{len(data)} records in {datetime.now() - start}")

    return data


def read_metadata(module, param) -> dict:
    """
    Parameters
    ----------
    module
        twinfield browse_code
    param
        login class of twinfield credentials

    Returns
    -------
    fieldmapping: dict
        dictionary of column names and labels
    """

    metadata = get_metadata(module=module, login=param)
    fieldmapping = metadata["label"].to_dict()

    return fieldmapping
