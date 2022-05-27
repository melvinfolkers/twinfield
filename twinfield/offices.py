import logging
import time
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
        success = False
        retry = 1
        while not success:
            if retry > self.max_retries:
                logging.warning(f"Max retries ({self.max_retries}) exceeded, " f"stopping requests for this office.")
                break
            try:
                with requests.post(
                    url=f"{self.cluster}/webservices/session.asmx?wsdl",
                    headers={"Content-Type": "text/xml", "Accept-Charset": "utf-8"},
                    data=body,
                ) as response:
                    result = self.check_response(response)
                    logging.debug(result)
                success = True
            except ConnectionError:
                logging.info(f"No response, retrying in {self.sec_wait} seconds. Retry number: {retry}")
                time.sleep(self.sec_wait)
                retry += 1

    def list_offices(self):
        body = LIST_OFFICES_XML.format(self.access_token)
        success = False
        retry = 1
        while not success:
            if retry > self.max_retries:
                logging.warning(f"Max retries ({self.max_retries}) exceeded, " f"stopping requests for this office.")
                break
            try:
                with requests.post(
                    url=f"{self.cluster}/webservices/processxml.asmx?wsdl",
                    headers={"Content-Type": "text/xml", "Accept-Charset": "utf-8"},
                    data=body,
                ) as response:
                    df = self.parse_list_offices_response(response)
                success = True
            except ConnectionError:
                logging.info(f"No response, retrying in {self.sec_wait} seconds. Retry number: {retry}")
                time.sleep(self.sec_wait)
                retry += 1

        return df

    def parse_list_offices_response(self, response: requests.Response) -> pd.DataFrame:
        """
        Parameters
        ----------
        response
            response xml from twinfield server
        param
            login parameters (SessionParameters)

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
