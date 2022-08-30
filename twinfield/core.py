from xml.etree import ElementTree as Et

import requests

from twinfield.login import TwinfieldLogin


class Base(TwinfieldLogin):
    """
    Base class for Twinfield handling static settings (namespaces) and parsing of responses, which follows the same
    logic for each of the modules.
    """

    def __init__(self):

        super().__init__()
        self.namespaces = {
            "env": "http://schemas.xmlsoap.org/soap/envelope/",
            "tw": "http://www.twinfield.com/",
        }

        self.namespaces_txt = {k: "{" + v + "}" for k, v in self.namespaces.items()}
        self.access_token = self.refresh_access_token()
        self.header_req = {"Content-Type": "text/xml", "Accept-Charset": "utf-8"}

    def send_request(self, browse) -> requests.Response:
        """
        Parameters
        ----------
        browse
            Browse class for the xml request.

        Returns
        -------
        df: pd.DataFrame
            dataframe containing response records.

        """
        url = f"{self.cluster}/webservices/processxml.asmx?wsdl"
        output = self.do_request(url=url, headers=self.header_req, data=browse.body())

        return output

    def check_invalid_token(self, response: requests.Response) -> bool:
        """
        Checks if the response message is about the token being expired.

        Parameters
        ----------
        response: requests.Response
            response from twinfield server

        Returns
        -------
        True or False, depending on wether the token is expired.

        """
        fault_string = Et.fromstring(response.text).find("env:Body/env:Fault/faultstring", self.namespaces)

        if fault_string:
            # if the there is a faultcode, check if its about the token being expired.
            return fault_string.text == "Access denied. Token invalid."
        else:
            # if not, return False.
            return False
