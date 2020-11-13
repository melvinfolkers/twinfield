import logging
from xml.etree import ElementTree as ET

import requests


def twinfield_login_faheem():
    user = os.environ.get("TW_USER_LS")
    password = os.environ.get("TW_PW_LS")
    organisation = os.environ.get("TW_ORG_LS")

    logging.info(f"inlogpoging met gebruiker {user}")

    login = SessionParameters(user=user, pw=password, organisation=organisation)

    logging.info("ingelogd met gebruiker {}".format(user))

    return login


class SessionParameters:
    def __init__(self, user, pw, organisation):

        self.user = user
        self.password = pw
        self.organisation = organisation

        self.url = "https://login.twinfield.com/webservices/session.asmx"
        self.header = {"Content-Type": "text/xml", "Accept-Charset": "utf-8"}

        self.ns = {
            "env": "http://schemas.xmlsoap.org/soap/envelope/",
            "tw": "http://www.twinfield.com/",
        }

        self.ns_txt = {k: "{" + v + "}" for k, v in self.ns.items()}

        self.body = templates.import_xml("template_login.xml").format(user, pw, organisation)
        self.session_id, self.cluster = SessionParameters.get_session_info(
            self, self.url, self.header, self.body
        )

    def parse_session_id(self, root):

        header = root.find("env:Header/tw:Header", self.ns)
        session_id = header.find("tw:SessionID", self.ns).text
        logging.debug("session id: {}".format(session_id))

        return session_id

    def parse_cluster(self, root):

        header = root.find("env:Body/tw:LogonResponse", self.ns)
        cluster = header.find("tw:cluster", self.ns).text
        cluster = cluster.split("https://")[1].split(".twinfield.com")[0]
        logging.debug("cluster: {}".format(cluster))

        return cluster

    def get_session_info(self, url, header, body):

        response = requests.post(url=url, headers=header, data=body)

        if response:
            session_id = SessionParameters.parse_session_id(
                self, root=ET.fromstring(response.text)
            )  # lees de response uit
            cluster = SessionParameters.parse_cluster(
                self, root=ET.fromstring(response.text)
            )  # lees de response uit

        else:
            logging.info("niet gelukt om data binnen te halen")

        return [session_id, cluster]
