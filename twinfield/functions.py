import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests
import json

from . import templates

import timeit

# offices


def import_files(run_params, patt) -> pd.DataFrame():
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


def parse_session_response(response, param):
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


def get_metadata(module, login):
    """

    :param param: session parameters
    :param module:  code van de module (zie dataservices twinfield)
    :return: metadata van de tabel
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


def parse_soap_error(body, login):
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


def parse_metadata_response(data):
    col = data.find("columns")

    rec = list()

    for records in col:

        ttl = dict()
        for record in records:
            ttl[record.tag] = record.text

        rec.append(ttl)

    df = pd.DataFrame(rec)

    return df


def parse_response(response, param):
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


def select_office(officecode, param):
    logging.debug("selecting office: {}...".format(officecode))

    counter = 0
    run = True
    while run:
        url = "https://{}.twinfield.com/webservices/session.asmx?wsdl".format(param.cluster)
        body = templates.import_xml("xml_templates/template_select_office.xml").format(
            param.session_id, officecode
        )
        response = requests.post(url=url, headers=param.header, data=body)

        if response.status_code == 200:
            run = False
        else:
            counter += 1
            logging.info(f"selecteren van office mislukt! start met poging {counter}")

    root = ET.fromstring(response.text)
    body = root.find("env:Body", param.ns)
    data = body.find("tw:SelectCompanyResponse/tw:SelectCompanyResult", param.ns)

    pass_fail = data.text

    logging.debug(pass_fail)


def periods_from_start(run_params):
    # all the records from 2015
    periodlist = [
        {"from": "2015/00", "to": f"{datetime.now().year -1}/55"},
        {"from": f"{datetime.now().year}/00", "to": f"{datetime.now().year}/55"},
    ]

    if run_params.rerun:
        periodlist = []
        years = range(datetime.now().year + 1)[-6:]
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


def periods_last_n_years(n):
    # last 10 years
    years = range(datetime.now().year + 1)[-n:]
    periods = list()
    for year in years:
        period = {"from": f"{year-1}/00", "to": f"{year}/55"}
        periods.append(period.copy())

    return periods


def period_groups(window="year"):
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


def soap_columns(metadata):
    ttl = ""

    for field, rows in metadata.iterrows():
        column_template = """<column xmlns="">
              <field>{}</field>
              <label>{}</label>
              <visible>{}</visible>
           </column>
           """.format(
            field, rows["label"], rows["visible"]
        )

        ttl = ttl + column_template

    return ttl


class RunParameters:
    def __init__(self, jaar, refresh, upload, modules, offices, rerun):

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


def create_dir(destination):

    try:
        if not os.path.exists(destination):
            os.makedirs(destination)
    except OSError:
        logging.warning("Error Creating directory. " + destination)

    return destination


def get_modules():
    function_dir = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(function_dir, "modules.json")
    file = open(file_path).read()
    modules = json.loads(file)

    return modules


def stop_time(start):
    stop = timeit.default_timer()
    end_time = round(stop - start)

    m, s = divmod(end_time, 60)
    h, m = divmod(m, 60)
    logging.info(f"afgerond in {h:d}:{m:02d}:{s:02d}")

    return f"{h:d}:{m:02d}:{s:02d}"
