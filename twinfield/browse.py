from xml.etree import ElementTree as Et

import pandas as pd

from twinfield.core import Base
from twinfield.exceptions import TwinfieldFaultCode
from twinfield.messages import COLUMN, COLUMN_FILTER, PROCESS_XML


class Browse(Base):
    def __init__(self, access_token: str, code: str, fields: list, filters: dict, company: str):
        """
        This class is for building the Browse SOAP requests that will be send to the Twinfield API.

        Parameters
        ----------
        access_token: str
            access_token obtained from TwinfieldLogin class.
        code: str
            specific browsecode for request (e.g. 100)
        fields: list
            list of fields for the dataset
        filters: dict
            dictionary containing specific fields that need to be filtered.
        company: str
            company code (office) for request
        """

        super().__init__()

        self.browsecode = code
        self.fields = [x for x in fields if x not in filters.keys()]
        self.filters = filters
        self.access_token = access_token
        self.company = company

    def set_fields(self) -> str:
        """

        Returns
        -------
        columns: str
            columns parsed in the XML template necessary for the SOAP message
        """

        column_list = []

        for field in self.fields:
            xml = COLUMN.format(field)
            column_list.append(xml)

        columns = "".join(column_list)

        return columns

    def set_filters(self) -> str:
        """

        Returns
        -------
        columns: str
            columns parsed in the XML template necessary for the SOAP message. These specific
            columns have filters with a 'from' and 'to' range.
        """

        filter_list = []

        for filter_column, filter_name_val in self.filters.items():
            # filter_name_val is a list with two items:
            # 1. filter name: between or equal
            # 2. filter dict: dict with from, to or only from
            filter_name, filter_dict = filter_name_val

            xml = COLUMN_FILTER.format(filter_column, filter_name, filter_dict.get("from"), filter_dict.get("to"))
            filter_list.append(xml)

        columns = "".join(filter_list)

        return columns

    def create_browse_query(self) -> str:
        """

        Returns
        -------
        columns: str
            combination of fields and filters, that together make up for the <columns> section in
            the XML template.
        """
        filters = self.set_filters()
        fields = self.set_fields()
        columns = filters + fields
        browse_request = f"""
        <columns code="{self.browsecode}">
            {columns}
        </columns>
        """
        return browse_request

    def body(self) -> str:
        """
        Returns
        -------
        body: str
            the full XML SOAP message for the request. The body is build up in a base template,
            string formatted with the current session_id , the module requested and the columns.
        """

        xml = self.create_browse_query()
        body = PROCESS_XML.format(self.access_token, self.company, xml)

        return body

    def parse_response(self, response) -> pd.DataFrame:
        """
        Parameters
        ----------
        response
            initial response from twinfield server
        Returns
        -------
        df: pd.DataFrame
            dataframe of records for the selected module
        """
        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)

        if body.find("env:Fault", self.namespaces):
            raise TwinfieldFaultCode()

        data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", self.namespaces)
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

        df = pd.DataFrame(ttl_records)

        return df
