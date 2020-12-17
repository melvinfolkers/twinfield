import logging
from datetime import datetime
import pandas as pd
import requests
import os
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
    body = templates.import_xml(os.path.join("xml_templates", "template_list_offices.xml")).format(
        param.session_id
    )
    response = requests.post(url=url, headers=param.header, data=body)
    if not response:
        return pd.DataFrame(
            [{"faultcode": f"Twinfield server error. status_code='{response.status_code}'"}]
        )
    data = parse_session_response(response, param)

    return data


def read_module(param, periode, module, jaar=None) -> pd.DataFrame:
    """
    Parameters
    ----------
    param
        login class with twinfield credentials
    jaar
        jaar van scope
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

    # logging.debug(f"start request periode van {periode['from']} t/m {periode['to']}")

    url = f"https://{param.cluster}.twinfield.com/webservices/processxml.asmx?wsdl"
    if module in ["100", "200"]:
        body = templates.import_xml(os.path.join("xml_templates", f"template_{module}.xml")).format(
            param.session_id, periode["from"], periode["to"]
        )
    elif module in ["030_1", "040_1"]:
        if not jaar:
            raise ValueError(
                "Let op: je runt nu consolidatie / transacties, maar jaar is niet opgegeven."
            )
        body = templates.import_xml(os.path.join("xml_templates", f"template_{module}.xml")).format(
            param.session_id, jaar, periode["from"], jaar, periode["to"]
        )
    elif module.startswith("dimensions"):
        dim_type = module.split("_")[1].upper()
        body = templates.import_xml(
            os.path.join("xml_templates", "template_dimensions.xml")
        ).format(param.session_id, dim_type)
    else:
        logging.info("Let op module is nog niet ontwikkeld")
    response = requests.post(url=url, headers=param.header, data=body)
    if not response:
        return pd.DataFrame(
            [{"faultcode": f"Twinfield server error. status_code='{response.status_code}'"}]
        )
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
