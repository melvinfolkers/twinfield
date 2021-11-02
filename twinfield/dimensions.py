import logging
from xml.etree import ElementTree as Et

import pandas as pd

from twinfield.core import Base
from twinfield.messages import PROCESS_XML


class Dimensions(Base):
    def __init__(self, company: str, dim_type: str):
        """
        This class is for building the Browse SOAP requests for getting metadata of browse codes

        Parameters
        ----------
        company: str
            specific the office code of the request
        """
        super().__init__()

        self.access_token = self.refresh_access_token()
        self.company = company
        self.dimension_type = dim_type

    def create_dimensions_query(self) -> str:
        """

        Returns
        -------
        columns: str
            combination of fields and filters, that together make up for the <columns> section in
            the XML template.
        """

        dimensions_request = f"""<read xmlns="">
                    <type>dimensions</type>
                    <dimtype>{self.dimension_type}</dimtype>
                    <general>
                        <adresses>
                        </adresses>
                    </general>
                </read>"""

        return dimensions_request

    def body(self) -> str:
        """
        Returns
        -------
        body: str
            the full XML SOAP message for the request. The body is build up in a base template,
            string formatted with the current session_id , the module requested and the columns.
        """

        xml = self.create_dimensions_query()
        body = PROCESS_XML.format(self.access_token, self.company, xml)

        return body

    def parse_dimensions(self, dimensions) -> list:
        """

        Parameters
        ----------
        dimensions: xml layers of dimensions

        Returns list of dictionaries containing parsed data
        -------

        """
        records = []

        for dimension in dimensions:
            d_dim = self.parse_layer(dimension)
            # d_addr = parse_addresses(dimension)
            d_bank = self.parse_banks(dimension)
            d_fin = self.parse_financials(dimension)
            d_mod = self.parse_several_modules(dimension)

            record = {**d_dim, **d_bank, **d_fin, **d_mod}
            records.append(record.copy())

        return records

    def get_dimension_codes(self, dimension) -> dict:
        """

        Parameters
        ----------
        dimension: xml layers of dimensions

        Returns list of dictionaries containing parsed data
        -------

        """
        dimdata = self.parse_layer(dimension)
        fields = ["dimension.office", "dimension.type", "dimension.code"]
        d = {k: v for k, v in dimdata.items() if k in fields}

        return d

    def parse_response_dimension_addresses(self, response) -> pd.DataFrame:
        """

        Parameters
        ----------

        response: response from twinfield server
        login:  login parameters (SessionParameters)

        Returns None. exports the dimension address data to pickle file in the tmp directory
        -------

        """
        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)
        data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", self.namespaces)
        dimensions = data.findall("dimensions/dimension")
        records = []

        for dimension in dimensions:

            dim = self.get_dimension_codes(dimension)
            addresses = dimension.find("addresses")
            d_addresses = self.parse_layer(addresses)

            for address in addresses:
                attrib = address.attrib
                d_address = self.parse_layer(address)

                total = {**attrib, **d_address, **dim, **d_addresses}
                records.append(total)

        df = pd.DataFrame(records)

        if not len(df):
            logging.debug("geen addressen geexporteerd")
            return pd.DataFrame()

        return df

    def parse_response_dimensions(self, response) -> pd.DataFrame:
        """

        Parameters
        ----------

        response: response from twinfield server

        Returns None. exports the dimension address data to pickle file in the tmp directory
        -------

        """
        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)
        data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", self.namespaces)
        dimensions = data.findall("dimensions/dimension")
        records = self.parse_dimensions(dimensions)
        data = pd.DataFrame(records)

        return data

    def parse_several_modules(self, dimension) -> dict:
        """

        Parameters
        ----------
        dimension: xml layer of dimensions submodule

        Returns dictionary containing parsed data
        -------

        """
        credman = dimension.find("creditmanagement")
        d_credman = self.parse_layer(credman)

        remad = dimension.find("remittanceadvice")
        d_remad = self.parse_layer(remad)

        inv = dimension.find("invoicing")
        d_inv = self.parse_layer(inv)

        d = {**d_credman, **d_remad, **d_inv}

        return d

    def parse_financials(self, dimension) -> dict:
        """

        Parameters
        ----------
        dimension: xml layer of dimensions submodule

        Returns dictionary containing parsed data
        -------

        """
        financials = dimension.find("financials")
        d_financials = self.parse_layer(financials)

        mandate = financials.find("collectmandate")
        d_mandate = self.parse_layer(mandate)

        chilval = financials.find("childvalidations")
        d_chilval = self.parse_layer(chilval)

        financial_info = {**d_mandate, **d_financials, **d_chilval}

        return financial_info

    def parse_banks(self, dimension) -> dict:
        """

        Parameters
        ----------
        dimension: xml layer of dimensions submodule

        Returns dictionary containing parsed data
        -------

        """
        banks = dimension.find("banks")
        bank_info = self.parse_layer(banks)
        if banks:
            bank = banks.find("bank")
            d_bank = self.parse_layer(bank)
            bank_info = {**bank_info, **d_bank}

        return bank_info

    def parse_addresses(self, dimension) -> dict:
        """

        Parameters
        ----------
        dimension: xml layer of dimensions submodule

        Returns dictionary containing parsed data
        -------

        """
        addresses = dimension.find("addresses")
        d_addresses = self.parse_layer(addresses)
        address = addresses.find("address")
        d_address = self.parse_layer(address)

        address_info = {**d_address, **d_addresses}

        return address_info

    @staticmethod
    def parse_layer(dimension) -> dict:
        """

        Parameters
        ----------
        dimension: xml layer of dimensions submodule

        Returns dictionary containing parsed data
        -------

        """
        if dimension is None:
            return {}

        d = dict()
        for child in dimension:
            if len(child) == 0:
                d[f"{dimension.tag}.{child.tag}"] = child.text

            if len(child.attrib):
                info = {f"{dimension.tag}.{child.tag}.{k}": v for k, v in child.attrib.items()}
                d = {**d, **info}

        return d
