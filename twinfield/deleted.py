from datetime import datetime
from xml.etree import ElementTree as Et

import pandas as pd

from twinfield.core import Base
from twinfield.messages import DELETED_TRANSACTIONS_XML


class DeletedTransactions(Base):
    def __init__(self, company: str, date_from: str):
        """
        This class is for building the Browse SOAP requests for getting metadata of browse codes

        Parameters
        ----------
        company: str
            specific the office code of the request
        date_from: str
            starting date of the request.
        """
        super().__init__()

        self.company = company
        self.date_from = date_from
        self.date_to = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def get_deleted_transactions(self):

        body = DELETED_TRANSACTIONS_XML.format(
            self.access_token, self.company, self.company, self.date_from, self.date_to
        )
        additional_headers = {"SOAPAction": "http://www.twinfield.com/DeletedTransactionsService/Query"}
        headers = {**self.header_req, **additional_headers}
        url = f"{self.cluster}/webservices/DeletedTransactionsService.svc?wsdl"
        output = self.do_request(url=url, headers=headers, data=body)

        return output

    def parse_response(self, response):
        root = Et.fromstring(response.text)
        body = root.find("env:Body", self.namespaces)
        data = body.findall("tw:Result/del:DeletedTransactions/del:DeletedTransaction", self.namespaces)

        ttl_records = list()
        for trans in data:
            info = dict()
            for col in trans:
                val = col.text
                name = col.tag.replace(self.namespaces_txt.get("del"), "")
                info[name] = val

            ttl_records.append(info)

        df = pd.DataFrame(ttl_records)

        if df.empty:
            return pd.DataFrame()

        # add the unique id column.
        df["office"] = self.company
        df["id"] = df["office"] + " | " + df["TransactionNumber"] + " | " + df["Daybook"]

        return df
