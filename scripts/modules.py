import logging
from datetime import datetime

import requests

from . import functions, soap_bodies


def read_offices(param):
    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = soap_bodies.soap_offices(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_session_response(response, param)

    return data


def read_100(param, run_params, periode):
    start = datetime.now()

    logging.info("start request {} periode van {} t/m {}".format(run_params.jaar, periode["from"], periode["to"]))

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = soap_bodies.soap_100(param.session_id, run_params, periode)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)
    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_200(param, run_params, periode):
    start = datetime.now()

    logging.info("start request {} periode van {} t/m {}".format(run_params.jaar, periode["from"], periode["to"]))

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = soap_bodies.soap_200(param.session_id, run_params, periode)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)
    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_040_1(param, run_params, periode):
    start = datetime.now()

    logging.info("start request {} periode van {} t/m {}".format(run_params.jaar, periode["from"], periode["to"]))

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = soap_bodies.soap_040_1(param.session_id, run_params, periode)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)
    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_030_1(param, run_params, periode):
    start = datetime.now()

    logging.info("start request {} periode van {} t/m {}".format(run_params.jaar, periode["from"], periode["to"]))

    url = "https://{}.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = soap_bodies.soap_030_1(param.session_id, run_params, periode)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)

    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data


def read_metadata(module, param):

    metadata = functions.get_metadata(module=module, param=param)
    fieldmapping = metadata["label"].to_dict()

    return fieldmapping


def read_164(param):
    start = datetime.now()

    logging.info("start request credit management")

    url = "https://.twinfield.com/webservices/processxml.asmx?wsdl".format(param.cluster)
    body = soap_bodies.soap_164(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)

    logging.info("{} records in {}".format(len(data), datetime.now() - start))

    return data
