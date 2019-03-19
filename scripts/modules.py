import requests
import pandas as pd
import xml.etree.ElementTree as ET
import soap_bodies
import functions



def read_030_1(param):

    period = '2019'
    url = 'https://c4.twinfield.com/webservices/processxml.asmx?wsdl'
    body = soap_bodies.soap_030_1(param.session_id, period)

    response = requests.post(url=url, headers=param.header, data=body)
    root = ET.fromstring(response.text)
    body = root.find('env:Body', param.ns)
    data = body.find('tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult', param.ns)

    data = parse_response(data, param)


    return data


def parse_response(data, param):
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

    # metadata ophalen

    metadata = functions.get_metadata(module = '030_1', param=param)
    fieldmapping = metadata['label'].to_dict()

    data.rename(fieldmapping, axis=1, inplace=True)

    return data


# read column metadata for browse module

