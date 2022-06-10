import logging
from xml.etree import ElementTree as Et

import pandas as pd
import requests

from twinfield.core import Base
from twinfield.exceptions import SelectOfficeError
from twinfield.messages import LIST_OFFICES_XML, SELECT_OFFICE


class Offices(Base):
    def __init__(self, access_token: str, cluster: str):
        """
        This class is for building the Browse SOAP requests for getting metadata of browse codes

        Parameters
        ----------
        access_token: str
            access_token obtained from TwinfieldApi class.
        cluster: str
            cluster obtained from TwinfieldApi class.
        """
        super().__init__()
        self.access_token = access_token
        self.cluster = cluster

    def select(self, officecode):
        """

        Parameters
        ----------
        officecode: str
            officecode of office that needs to be selected.

        Returns
        -------
        None:
            Selects the office.
        """

        logging.debug(f"selecting office: {officecode}...")
        body = SELECT_OFFICE.format(self.access_token, officecode)
        url = f"{self.cluster}/webservices/session.asmx?wsdl"
        result = self.do_request(url=url, headers=self.header_req, data=body)
        result = self.check_response(result)
        logging.debug(result)

    def list_offices(self):
        body = LIST_OFFICES_XML.format(self.access_token)
        url = f"{self.cluster}/webservices/processxml.asmx?wsdl"
        response = self.do_request(url=url, headers=self.header_req, data=body)
        df = self.parse_list_offices_response(response)
        return df

    def parse_list_offices_response(self, response: requests.Response) -> pd.DataFrame:
        """
        Parameters
        ----------
        response
            response xml from twinfield server

        Returns
        -------
        df_ttl: pd.DataFrame
            dataframe of parsed XML response
        """
        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)
        data = body.find("tw:ProcessXmlStringResponse/tw:ProcessXmlStringResult", self.namespaces)
        data = Et.fromstring(data.text)

        df_ttl = [pd.DataFrame(data=child.attrib, index=[child.text]) for child in data]
        df_ttl = pd.concat(df_ttl)

        return df_ttl

    def check_response(self, response: requests.Response):
        """

        Parameters
        ----------
        response: requests.Response

        Returns
        -------
        result: str
            result of the response
        """
        if not response:
            SelectOfficeError("Selecteren van office niet gelukt")

        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)
        result = body.find("tw:SelectCompanyResponse/tw:SelectCompanyResult", self.namespaces)

        return result
