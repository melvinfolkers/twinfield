import requests
import soap_bodies
import functions
import logging


from datetime import datetime




def read_offices(param):

    url = 'https://c4.twinfield.com/webservices/processxml.asmx?wsdl'
    body = soap_bodies.soap_offices(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_session_response(response, param)

    # officecodes_in_scope = ['1038632', '1060253', '1060254', '1060255', '1060256', '1060257', '1060258', '1060259',
    #                         '1060260', '1060261', '1060262', '1060265', '1060266', '1060267', '1060269', '1060270',
    #                         '1060271', '1060272', '1060273', '1060274', '1063019', '1064667', '1064668', '1064670',
    #                         '1064671', '1064672', '1064673', '1064678', '1064679', '1064680', '1064681', '1064682_',
    #                         '1064683_','1064684', '1064685', '1066219', '1074700', '1074701', '1074702', '1074703',
    #                         '1074704']
    #
    # data = data[data.index.isin(officecodes_in_scope)]

    return data

def read_030_1(param, jaar, periode):

    start = datetime.now()

    logging.info('start request {} periode van {} t/m {}'.format(jaar, periode['from'],periode['to'] ))

    url = 'https://c4.twinfield.com/webservices/processxml.asmx?wsdl'
    body = soap_bodies.soap_030_1(param.session_id, jaar, periode)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)

    logging.info(f'{len(data)} records in {datetime.now() - start}')
    # metadata ophalen en gebruiken om velden te hernoemen
    #fieldmapping = functions.get_metadata(module='030_1', param=param)
    #fieldmapping = metadata['label'].to_dict()
    #data.rename(fieldmapping, axis=1, inplace=True)

    return data



def read_164(param):

    start = datetime.now()

    logging.info('start request credit management')

    url = 'https://c4.twinfield.com/webservices/processxml.asmx?wsdl'
    body = soap_bodies.soap_164(param.session_id)
    response = requests.post(url=url, headers=param.header, data=body)

    data = functions.parse_response(response, param)

    logging.info(f'{len(data)} records in {datetime.now() - start}')

    # metadata ophalen en gebruiken om velden te hernoemen
    #fieldmapping = functions.get_metadata(module='164', param=param)
    #fieldmapping = metadata['label'].to_dict()
    #data.rename(fieldmapping, axis=1, inplace=True)

    return data
