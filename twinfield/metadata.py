from xml.etree import ElementTree as Et

import pandas as pd
import requests

from twinfield.core import Base
from twinfield.exceptions import ServerError
from twinfield.messages import METADATA_XML


class Metadata(Base):
    def __init__(self, code: str, company: str):
        """
        This class is for building the Browse SOAP requests for getting metadata of browse codes

        Parameters
        ----------
        code: str
            specific browsecode of which we want to get the metadata
        company: str
            specific the office code of the request
        """
        super().__init__()
        self.browsecode = code
        self.company = company

    def create_metadata_query(self) -> str:
        """

        Returns
        -------
        columns: str
            combination of fields and filters, that together make up for the <columns> section in
            the XML template.
        """

        metadata_request = f"""<read>
            <type>browse</type>
            <code>{self.browsecode}</code>
            </read>"""

        return metadata_request

    def body(self) -> str:
        """
        Returns
        -------
        body: str
            the full XML SOAP message for the request. The body is build up in a base template,
            string formatted with the current session_id , the module requested and the columns.
        """

        xml = self.create_metadata_query()
        body = METADATA_XML.format(self.access_token, self.company, xml)

        return body

    def parse_metadata_response(self, response: requests.Response) -> pd.DataFrame:
        """
        Parameters
        ----------
        response
            Response object containing the twinfield server response

        Returns
        -------
        df: pd.DataFrame
            dataframe of metadata
        """

        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)

        if body.find("env:Fault", self.namespaces):
            # TODO: toegevoegd voor debugging.
            fault = Et.tostring(body)
            print(fault)

            raise ServerError()

        data = body.find("tw:ProcessXmlStringResponse/tw:ProcessXmlStringResult", self.namespaces)
        data = Et.fromstring(data.text)

        col = data.find("columns")
        rec = list()

        for records in col:
            ttl = dict()
            for record in records:
                ttl[record.tag] = record.text
            rec.append(ttl)

        df = pd.DataFrame(rec)

        return df

    def send_request(self, cluster) -> pd.DataFrame:
        """

        Parameters
        ----------
        cluster: cluster obtained from TwinfieldApi class

        Returns
        -------
        df: pd.DataFrame
            dataframe containing the records.
        """

        body = self.body()
        url = f"{cluster}/webservices/processxml.asmx?wsdl"
        response = self.do_request(url=url, headers=self.header_req, data=body)
        metadata = self.parse_metadata_response(response)
        metadata.loc[metadata.label.isna(), "label"] = metadata.field
        metadata.set_index("field", inplace=True)

        return metadata
