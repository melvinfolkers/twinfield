import logging

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

    def send_request(self, body) -> requests.Response:
        """
        Parameters
        ----------
        body: str
            body for the processxml request.

        Returns
        -------
        df: pd.DataFrame
            dataframe containing response records.

        """
        success = False
        max_retries = 5
        retry = 1
        sec_wait = 10
        response = None
        while not success:
            if retry > max_retries:
                logging.warning(f"Max retries ({max_retries}) exceeded, " f"stopping requests for this office.")
                break
            response = requests.post(
                url=f"{self.cluster}/webservices/processxml.asmx?wsdl",
                headers={"Content-Type": "text/xml", "Accept-Charset": "utf-8"},
                data=body,
            )
            if not response:
                logging.info(f"No response, retrying in {sec_wait} seconds. Retry number: {retry}")
                retry += 1
            else:
                success = True

        return response
