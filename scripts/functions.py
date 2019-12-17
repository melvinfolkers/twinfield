import codecs
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests

from scripts import soap_bodies


class SessionParameters():

    def __init__(self, user, pw, organisation):

        self.user = user
        self.password = pw
        self.organisation = organisation

        self.url = 'https://login.twinfield.com/webservices/session.asmx'
        self.header = {'Content-Type': 'text/xml',
                       'Accept-Charset': 'utf-8'}

        self.ns = {'env': 'http://schemas.xmlsoap.org/soap/envelope/',
                   'tw': 'http://www.twinfield.com/'}

        self.body = '''<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XmlSchema-instance" xmlns:xsd="http://www.w3.org/2001/XmlSchema">
          <soap:Body>
            <Logon xmlns="http://www.twinfield.com/">
              <user>{}</user>
              <password>{}</password>
              <organisation>{}</organisation>
            </Logon>
          </soap:Body>
        </soap:Envelope>'''.format(user, pw, organisation)

        self.session_id, self.cluster = SessionParameters.get_session_info(self, self.url, self.header, self.body)

    def parse_session_id(self, root):

        header = root.find('env:Header/tw:Header', self.ns)
        session_id = header.find('tw:SessionID', self.ns).text
        logging.info('session id: {}'.format(session_id))

        return session_id

    def parse_cluster(self, root):

        header = root.find('env:Body/tw:LogonResponse', self.ns)
        cluster = header.find('tw:cluster', self.ns).text
        cluster = cluster.split('//')[1].split('.')[0]
        logging.info('cluster: {}'.format(cluster))

        return cluster

    def get_session_info(self, url, header, body):

        response = requests.post(url=url, headers=header, data=body)

        if response:
            session_id = SessionParameters.parse_session_id(self,
                                                            root=ET.fromstring(response.text))  # lees de response uit
            cluster = SessionParameters.parse_cluster(self,
                                                      root=ET.fromstring(response.text))  # lees de response uit

        else:
            logging.info('niet gelukt om data binnen te halen')

        return [session_id, cluster]

# offices


def import_files(run_params, patt):
    ttl = pd.DataFrame()

    files = os.listdir(os.path.join(run_params.pickledir))
    files = [x for x in files if x.find(patt) != -1]
    for file in files:
        df = pd.read_pickle(os.path.join(run_params.pickledir, file))
        ttl = pd.concat([ttl, df], axis=0, sort=False, ignore_index=True)

    return ttl


def parse_session_response(response, param):
    root = ET.fromstring(response.text)

    body = root.find('env:Body', param.ns)

    data = body.find('tw:ProcessXmlStringResponse/tw:ProcessXmlStringResult', param.ns)

    data = ET.fromstring(data.text)

    df_ttl = pd.DataFrame()

    for child in data:
        df = pd.DataFrame(data=child.attrib, index=[child.text])
        df_ttl = pd.concat([df_ttl, df], axis=0)

    return df_ttl


# metadata

def get_metadata(module, param):
    '''

    :param param: session parameters
    :param module:  code van de module (zie dataservices twinfield)
    :return: metadata van de tabel
    '''

    url = 'https://{}.twinfield.com/webservices/processxml.asmx?wsdl'.format(param.cluster)
    body = soap_bodies.soap_metadata(param, module=module)

    response = requests.post(url=url, headers=param.header, data=body)

    root = ET.fromstring(response.text)

    body = root.find('env:Body', param.ns)

    data = body.find('tw:ProcessXmlStringResponse/tw:ProcessXmlStringResult', param.ns)

    data = ET.fromstring(data.text)

    metadata = parse_metadata_response(data)

    metadata.set_index('field', inplace=True)

    # fieldmapping = metadata['label'].to_dict()

    return metadata


def parse_metadata_response(data):
    col = data.find('columns')

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
    body = root.find('env:Body', param.ns)
    try:
        data = body.find('tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult', param.ns)
    except:
        return pd.DataFrame()

    browse = data.find('browse')

    tr = browse.findall('tr')

    ttl_records = list()

    for trans in tr:

        info = dict()

        for col in trans:

            val = col.text

            if 'field' in col.attrib.keys():
                name = col.attrib['field']

                info[name] = val

        ttl_records.append(info)

    data = pd.DataFrame(ttl_records)

    return data


def select_office(officecode, param):
    logging.info('selecting office: {}...'.format(officecode))

    url = 'https://{}.twinfield.com/webservices/session.asmx?wsdl'.format(param.cluster)

    body = soap_bodies.soap_select_office(param, officecode=officecode)

    response = requests.post(url=url, headers=param.header, data=body)

    root = ET.fromstring(response.text)

    body = root.find('env:Body', param.ns)

    data = body.find('tw:SelectCompanyResponse/tw:SelectCompanyResult', param.ns)

    pass_fail = data.text

    logging.info(pass_fail)


def period_groups(window='year'):
    if window == 'year':

        period = [{'from': '00', 'to': '55'}]

    elif window == 'half-year':

        period = [{'from': '00', 'to': '06'},
                  {'from': '07', 'to': '55'}]

    elif window == 'quarter':

        period = [{'from': '00', 'to': '03'},
                  {'from': '04', 'to': '06'},
                  {'from': '07', 'to': '09'},
                  {'from': '10', 'to': '55'}]

    elif window == 'two_months':

        period = [{'from': '00', 'to': '02'},
                  {'from': '03', 'to': '04'},
                  {'from': '05', 'to': '06'},
                  {'from': '07', 'to': '08'},
                  {'from': '09', 'to': '10'},
                  {'from': '11', 'to': '12'},
                  {'from': '13', 'to': '55'}]


    elif window == 'month':

        period = [{'from': '00', 'to': '00'},
                  {'from': '01', 'to': '01'},
                  {'from': '02', 'to': '02'},
                  {'from': '03', 'to': '03'},
                  {'from': '04', 'to': '04'},
                  {'from': '05', 'to': '05'},
                  {'from': '06', 'to': '06'},
                  {'from': '07', 'to': '07'},
                  {'from': '08', 'to': '08'},
                  {'from': '09', 'to': '09'},
                  {'from': '10', 'to': '10'},
                  {'from': '11', 'to': '11'},
                  {'from': '12', 'to': '12'},
                  {'from': '13', 'to': '13'},
                  {'from': '55', 'to': '55'}]

    else:
        info.error('geen periode kunnen toewijzen')

    return period


def soap_columns(metadata):
    ttl = ''

    for field, rows in metadata.iterrows():
        column_template = '''<column xmlns="">
              <field>{}</field>
              <label>{}</label>
              <visible>{}</visible>
           </column>
           '''.format(field, rows['label'], rows['visible'])

        ttl = ttl + column_template

    return ttl


def set_logging(run_params):
    start = datetime.now()

    logfilename = 'runlog_' + start.strftime(format='%Y%m%d') + '.log'
    full_path = os.path.join(run_params.logdir, logfilename)

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(filename=full_path, level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%H:%M:%S')


    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    return start


class RunParameters:

    def __init__(self,jaar):

        self.projectdir = os.getcwd()
        self.jaar = jaar
        self.logdir = RunParameters.create_dir(destination=os.path.join(self.projectdir, 'data', 'log',self.jaar))
        self.pickledir = RunParameters.create_dir(destination=os.path.join(self.projectdir, 'data', 'pickles',self.jaar))
        self.stagingdir = RunParameters.create_dir(destination=os.path.join(self.projectdir, 'data', 'staging', self.jaar))

    def create_dir(destination):

        try:
            if not os.path.exists(destination):
                os.makedirs(destination)
        except OSError:
            logging.warning('Error Creating directory. ' + destination)
        return destination
