import logging
import os
from typing import Union
import pandas as pd
import requests

from twinfield.pull_data import import_all
from twinfield.functions import RunParameters, import_files, select_office
from twinfield.templates import import_xml
from twinfield.credentials import SessionParameters
from twinfield.responses import parse_response
from twinfield.send_soap_msg import add_office_code_to_xml_header
from twinfield import TEMPLATES


def query(
    module: str,
    jaar: Union[int, str] = None,
    offices: list = None,
    rerun: bool = False,
    clean: bool = True,
) -> Union[pd.DataFrame, tuple]:
    """
    Import data from Twinfield using the API.

    Parameters
    ----------
    module: str
        Browse code for the Twinfield module to be imported.
    jaar: int or str
        Year of the scope.
    offices: list
        List of the offices in scope and to be imported.
    rerun: bool
        set to True in case some modules are not correctly imported in previous run.
    clean: bool
        clean up pickle directory and remove pickle files.
    Returns
    -------
    df: pd.DataFrame or tuple of dateframes
        DataFrame containing for requested module, year and all offices in scope.
    """
    run_params = RunParameters(jaar=jaar, module=module, offices=offices, rerun=rerun)

    logging.info(
        f"{3 * '*'} Starting import of {run_params.module_names.get(run_params.module)} "
        f"with user: {os.environ.get('TW_USER_LS')} {3 * '*'}"
    )
    import_all(run_params)

    if run_params.module in ["dimensions_deb", "dimensions_crd"]:
        dim = import_files(run_params, run_params.module)
        dim_address = import_files(run_params, f"{run_params.module}_addresses")
        return dim, dim_address

    df = import_files(run_params, run_params.module)
    # clean up directory where files are stored
    if clean:
        run_params.datadir.cleanup()

    return df


def insert(module: str, officecode: str, soap_msg: str, login: SessionParameters) -> pd.DataFrame:
    """
    Parameters
    ----------
    module: str
        chosen module for inserting the soap message into Twinfield
    officecode: str
        office code of administration in which data needs to be inserted
    soap_msg: str
        XML message containing the data for the insert
    login: SessionParameters
        class containing login information for current session

    Returns
    -------
    df: pd.DataFrame
        dataframe containing response of twinfield server.
    """

    select_office(officecode=officecode, param=login)

    template_file = TEMPLATES.get(module)
    template_xml = import_xml(os.path.join("xml_templates", template_file))
    soap_msg = add_office_code_to_xml_header(officecode, soap_msg)
    body = template_xml.format(login.session_id, soap_msg)

    url = f"https://{login.cluster}.twinfield.com/webservices/processxml.asmx?wsdl"
    response = requests.post(url=url, headers=login.header, data=body.encode("utf16"))
    df = parse_response(module, response, login)

    return df
