import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime
import shutil
import pandas as pd
import requests
import json
from .credentials import twinfield_login
from . import templates
import timeit


def import_files(run_params, patt) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params: default run parameters for script
    patt: pattern for files to import.

    Returns: import of pickle files
    -------

    """

    ttl = list()

    files = os.listdir(os.path.join(run_params.pickledir))
    files = [x for x in files if x.find(patt) != -1]
    if not files:
        return pd.DataFrame()
    for file in files:
        df = pd.read_pickle(os.path.join(run_params.pickledir, file))
        ttl.append(df)

    data = pd.concat(ttl, axis=0, sort=False, ignore_index=True)

    return data


def parse_session_response(response, param) -> pd.DataFrame:
    """

    Parameters
    ----------
    response: response xml from twinfield server
    param:  login parameters (SessionParameters)

    Returns: dataframe of parsed XML response
    -------

    """
    root = ET.fromstring(response.text)

    body = root.find("env:Body", param.ns)

    data = body.find("tw:ProcessXmlStringResponse/tw:ProcessXmlStringResult", param.ns)

    data = ET.fromstring(data.text)

    df_ttl = pd.DataFrame()

    for child in data:
        df = pd.DataFrame(data=child.attrib, index=[child.text])
        df_ttl = pd.concat([df_ttl, df], axis=0)

    return df_ttl


# metadata


def get_metadata(module, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    module: code of the module: see api documentation of twinfield for available codes.
    login:  login parameters (SessionParameters)

    Returns: dataframe of fieldnames and labels for the module
    -------

    """

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(login.cluster)
    body = templates.soap_metadata(login, module=module)

    response = requests.post(url=url, headers=login.header, data=body)

    root = ET.fromstring(response.text)

    body = root.find("env:Body", login.ns)

    if body.find("env:Fault", login.ns):
        error = parse_soap_error(body, login)
        return error

    data = body.find("tw:ProcessXmlStringResponse/tw:ProcessXmlStringResult", login.ns)

    data = ET.fromstring(data.text)

    metadata = parse_metadata_response(data)
    metadata.loc[metadata.label.isna(), "label"] = metadata.field

    metadata.set_index("field", inplace=True)

    return metadata


def parse_soap_error(body, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    body: body of the soap message
    login:  login parameters (SessionParameters)

    Returns
    -------

    """
    fault = body.find("env:Fault", login.ns)
    d = dict()
    d["faultcode"] = fault.find("faultcode").text
    d["faultstring"] = fault.find("faultcode").text
    d["faultactor"] = fault.find("faultactor").text

    detail = fault.find("detail")
    d["message"] = detail.find("message").text
    d["code"] = detail.find("code").text
    d["source"] = detail.find("source").text

    logging.info(d)

    return pd.DataFrame([d])


def parse_metadata_response(data) -> pd.DataFrame:
    """

    Parameters
    ----------
    data: layer of XML metadata response from twinfield server

    Returns dataframe of metadata
    -------

    """
    col = data.find("columns")

    rec = list()

    for records in col:

        ttl = dict()
        for record in records:
            ttl[record.tag] = record.text

        rec.append(ttl)

    df = pd.DataFrame(rec)

    return df


def parse_response(response, param) -> pd.DataFrame:
    """

    Parameters
    ----------
    response : initial response from twinfield server
    param:  login parameters (SessionParameters)

    Returns dataframe of records for the selected module
    -------

    """
    root = ET.fromstring(response.text)
    body = root.find("env:Body", param.ns)

    if body.find("env:Fault", param.ns):
        error = parse_soap_error(body, param)
        return error

    data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", param.ns)

    browse = data.find("browse")

    tr = browse.findall("tr")

    ttl_records = list()

    for trans in tr:

        info = dict()

        for col in trans:

            val = col.text

            if "field" in col.attrib.keys():
                name = col.attrib["field"]

                info[name] = val

        ttl_records.append(info)

    data = pd.DataFrame(ttl_records)

    return data


def select_office(officecode, param) -> None:
    """

    Parameters
    ----------
    officecode : officecode of Twinfield administration.
    param:  login parameters (SessionParameters)
    info: Because the function sometimes gets a 503 error from the twinfield server, a retry is
          added. There will be a new login attempt followed by a repetion of the select office
          request.

    Returns None. this function selects the office before starting the send a soap message.

    -------

    """
    logging.debug("selecting office: {}...".format(officecode))

    counter = 0
    run = True
    while run:
        url = "https://{}.twinfield.com/webservices/session.asmx?wsdl".format(param.cluster)
        body = templates.import_xml("xml_templates/template_select_office.xml").format(
            param.session_id, officecode
        )
        response = requests.post(url=url, headers=param.header, data=body)

        if response.status_code == 200 or counter == 10:
            run = False
        else:
            # opnieuw inloggen en vervolgens nog een keer proberen.
            param = twinfield_login()
            counter += 1
            logging.info(f"selecteren van office mislukt! start met poging {counter}")

    root = ET.fromstring(response.text)
    body = root.find("env:Body", param.ns)
    data = body.find("tw:SelectCompanyResponse/tw:SelectCompanyResult", param.ns)

    pass_fail = data.text

    logging.debug(pass_fail)


def periods_from_start(run_params) -> list:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    info: for the query modules, sometimes the datasets are to big to request in a full years
          period. In that case, the RunParameter setting 'rerun' can be used to make smaller
          periods.

    Returns: list of periods that will be iterated over when sending the request.
    -------

    """

    periodlist = [
        {"from": "2015/00", "to": "2019/55"},
        {"from": f"2020/00", "to": f"{datetime.now().year}/55"},
    ]

    if run_params.rerun:
        periodlist = []
        min_years = 2015
        years_to_add = datetime.now().year + 1 - min_years
        years = [x + min_years for x in range(years_to_add)]

        periods = [
            "00",
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
            "13",
            "55",
        ]

        for year in years:
            for period in periods:
                period = {"from": f"{year}/{period}", "to": f"{year}/{period}"}
                periodlist.append(period.copy())

    return periodlist


def period_groups(window="year") -> list:
    """

    Parameters
    ----------
    window: selected window for the request. pick a smaller window if expected response size of
            request is large. that way the Twinfield server does not timeout.

    Returns: list of periods that will be iterated over when sending the request.
    -------

    """
    if window == "year":

        period = [{"from": "00", "to": "55"}]

    elif window == "half-year":

        period = [{"from": "00", "to": "06"}, {"from": "07", "to": "55"}]

    elif window == "quarter":

        period = [
            {"from": "00", "to": "03"},
            {"from": "04", "to": "06"},
            {"from": "07", "to": "09"},
            {"from": "10", "to": "55"},
        ]

    elif window == "two_months":

        period = [
            {"from": "00", "to": "02"},
            {"from": "03", "to": "04"},
            {"from": "05", "to": "06"},
            {"from": "07", "to": "08"},
            {"from": "09", "to": "10"},
            {"from": "11", "to": "12"},
            {"from": "13", "to": "55"},
        ]

    elif window == "month":

        period = [
            {"from": "00", "to": "00"},
            {"from": "01", "to": "01"},
            {"from": "02", "to": "02"},
            {"from": "03", "to": "03"},
            {"from": "04", "to": "04"},
            {"from": "05", "to": "05"},
            {"from": "06", "to": "06"},
            {"from": "07", "to": "07"},
            {"from": "08", "to": "08"},
            {"from": "09", "to": "09"},
            {"from": "10", "to": "10"},
            {"from": "11", "to": "11"},
            {"from": "12", "to": "12"},
            {"from": "13", "to": "13"},
            {"from": "55", "to": "55"},
        ]

    else:
        logging.info("geen periode kunnen toewijzen")
        period = [{"from": "00", "to": "55"}]
    return period


class RunParameters:
    def __init__(self, jaar, refresh, upload, modules, offices, rerun):
        """

        Parameters
        ----------
        jaar :   parameter for choosing a year for the selected module. Not always necessary, but is
                 necessary when requesting the read modules "040_1" and "030_1". These modules are
                 split up in years.
        refresh: boolean. Set to True if request need to be made to the Twinfield server.
        upload:  boolean. Set to True if the data needs to be uploaded to Azure
        modules: mixed. can be list, containing multiple modules, in case of read statement.
                 in case of insert statements it will
                 be a string type containing the module that needs to be run.
        offices: list. list of officecodes. if list is empty, all offices will be selected.
        rerun:   parameter for rerunning the module, with smaller periods.
        """

        self.jaar = str(jaar)
        self.refresh = refresh
        self.rerun = rerun
        self.upload = upload
        self.modules = modules
        self.module_names = get_modules()
        self.offices = offices
        self.datadir = "/tmp/twinfield"
        self.logdir = create_dir(destination=os.path.join(self.datadir, "data", "log"))
        self.pickledir = create_dir(destination=os.path.join(self.datadir, "data", "pickles"))
        self.stagingdir = create_dir(destination=os.path.join(self.datadir, "data", "staging"))
        self.starttijd = datetime.now()
        self.start = timeit.default_timer()


def create_dir(destination) -> str:
    """

    Parameters
    ----------
    destination: the file path that needs to be created
    info: the function checks if the folder exists. If so, it will remove it. If not, the folder
          will be created.
    Returns : the original file_path
    -------

    """

    if os.path.exists(destination):
        pass
    else:
        logging.warning(f"tmp folder does not exists, creating {destination}")
        os.makedirs(destination)

    return destination


def remove_and_create_dir(destination) -> str:
    """

    Parameters
    ----------
    destination: the file path that needs to be created
    info: the function checks if the folder exists. If so, it will remove it. If not, the folder
          will be created.
    Returns : the original file_path
    -------

    """

    if os.path.exists(destination):
        logging.warning(f"tmp folder exists, removing {destination}")
        shutil.rmtree(destination)
        os.makedirs(destination)
    else:
        logging.warning(f"tmp folder does not exists, creating {destination}")
        os.makedirs(destination)

    return destination


def get_modules() -> dict:
    """

    Returns: modules dictionary loaded from static file modules.json
    -------

    """
    function_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(function_dir, "modules.json")
    file = open(file_path).read()
    modules = json.loads(file)

    return modules


def stop_time(start) -> str:
    """

    Parameters
    ----------
    start: starttime of script

    Returns str. parsed endtime of script
    -------

    """
    stop = timeit.default_timer()
    end_time = round(stop - start)

    m, s = divmod(end_time, 60)
    h, m = divmod(m, 60)
    logging.info(f"afgerond in {h:d}:{m:02d}:{s:02d}")

    return f"{h:d}:{m:02d}:{s:02d}"