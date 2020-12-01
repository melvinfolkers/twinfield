import logging
from datetime import datetime
import pandas as pd
import requests
from . import functions, templates


def read_offices(param) -> pd.DataFrame:
    """

    Parameters
    ----------
    param: login class with twinfield credentials

    Returns: dataframe containing a list of offices available
    -------

    """

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = templates.import_xml("xml_templates/template_list_offices.xml").format(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_session_response(response, param)

    return data


def read_100(param, run_params, periode) -> pd.DataFrame:
    """

    Parameters
    ----------
    param: login class with twinfield credentials
    run_params: run settings from run_settings.yml file
    periode: scope of period in request.

    Returns: dataframe containing data from browse code 100
    -------

    """

    start = datetime.now()

    logging.debug("start request periode van {} t/m {}".format(periode["from"], periode["to"]))

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = templates.import_xml("xml_templates/template_100.xml").format(
        param.session_id, periode["from"], periode["to"]
    )

    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)
    logging.debug("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_200(param, run_params, periode) -> pd.DataFrame:
    """

    Parameters
    ----------
    param: login class with twinfield credentials
    run_params: run settings from run_settings.yml file
    periode: scope of period in request.

    Returns: dataframe containing data from browse code 200
    -------

    """
    start = datetime.now()

    logging.debug("start request periode van {} t/m {}".format(periode["from"], periode["to"]))

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = templates.import_xml("xml_templates/template_200.xml").format(
        param.session_id, periode["from"], periode["to"]
    )
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)
    logging.debug("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_040_1(param, run_params, periode) -> pd.DataFrame:
    """

    Parameters
    ----------
    param: login class with twinfield credentials
    run_params: run settings from run_settings.yml file
    periode: scope of period in request.

    Returns: dataframe containing data from browse code 040_1
    -------

    """
    start = datetime.now()

    logging.debug(
        "start request {} periode van {} t/m {}".format(
            run_params.jaar, periode["from"], periode["to"]
        )
    )

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)

    body = templates.import_xml("xml_templates/template_040_1.xml").format(
        param.session_id, run_params.jaar, periode["from"], run_params.jaar, periode["to"]
    )

    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)
    logging.debug("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_030_1(param, run_params, periode) -> pd.DataFrame:
    """

    Parameters
    ----------
    param: login class with twinfield credentials
    run_params: run settings from run_settings.yml file
    periode: scope of period in request.

    Returns: dataframe containing data from browse code 030_1
    -------

    """
    start = datetime.now()

    logging.info(
        "start request {} periode van {} t/m {}".format(
            run_params.jaar, periode["from"], periode["to"]
        )
    )

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)

    body = templates.import_xml("xml_templates/template_030_1.xml").format(
        param.session_id, run_params.jaar, periode["from"], run_params.jaar, periode["to"]
    )

    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)

    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_metadata(module, param) -> dict:
    """

    Parameters
    ----------
    module: twinfield browse_code
    param: login class of twinfield credentials

    Returns dictionary of column names and labels
    -------

    """

    metadata = functions.get_metadata(module=module, login=param)
    fieldmapping = metadata["label"].to_dict()

    return fieldmapping


def read_164(param) -> pd.DataFrame:
    """

    Parameters
    ----------
    param: login class with twinfield credentials
    Returns: dataframe containing data from browse code 164
    -------

    """

    start = datetime.now()

    logging.info("start request credit management")

    url = "https://.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = templates.soap_164(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)

    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data
