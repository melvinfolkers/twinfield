"""Twinfield API message templates"""
# flake8: noqa


PROCESS_XML = """<?xml version="1.0" encoding="utf-16"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
          <soap:Header>
             <Header xmlns="http://www.twinfield.com/">
              <AccessToken>{}</AccessToken>
            <CompanyCode>{}</CompanyCode>
            </Header>
          </soap:Header>
          <soap:Body>
             <ProcessXmlDocument xmlns="http://www.twinfield.com/">
                <xmlRequest>
                    {}
                </xmlRequest>
            </ProcessXmlDocument>
          </soap:Body>
        </soap:Envelope>"""

COLUMN = """<column xmlns="">
       <field>{}</field>
       <visible>{}</visible>
   </column>
   """

COLUMN_FILTER = """<column xmlns="">
       <field>{}</field>
       <operator>{}</operator>
       <from>{}</from>
       <to>{}</to>
       <visible>{}</visible>
   </column>
   """

SELECT_OFFICE = """<?xml version="1.0" encoding="utf-16"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <soap:Header>
            <Header xmlns="http://www.twinfield.com/">
                <AccessToken>{}</AccessToken>
            </Header>
        </soap:Header>
        <soap:Body>
            <SelectCompany xmlns="http://www.twinfield.com/">
                <company>{}</company>
            </SelectCompany>
        </soap:Body>
    </soap:Envelope>
    """

METADATA_XML = """<?xml version="1.0" encoding="utf-16"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:twin="http://www.twinfield.com/">
    <soapenv:Header>
        <twin:Header>
            <twin:AccessToken>{}</twin:AccessToken>
            <twin:CompanyCode>{}</twin:CompanyCode>
        </twin:Header>
    </soapenv:Header>
    <soapenv:Body>
        <twin:ProcessXmlString>
            <twin:xmlRequest><![CDATA[{}]]></twin:xmlRequest>
        </twin:ProcessXmlString>
    </soapenv:Body>
</soapenv:Envelope>"""


LIST_OFFICES_XML = """<?xml version="1.0" encoding="utf-16"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:twin="http://www.twinfield.com/">
    <soapenv:Header>
        <twin:Header>
            <twin:AccessToken>{}</twin:AccessToken>
        </twin:Header>
    </soapenv:Header>
    <soapenv:Body>
        <twin:ProcessXmlString>
            <twin:xmlRequest><![CDATA[<list><type>offices</type></list>]]></twin:xmlRequest>
        </twin:ProcessXmlString>
    </soapenv:Body>
</soapenv:Envelope>"""

DELETED_TRANSACTIONS_XML = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
    <s:Header>
        <h:Authentication xmlns:h="http://www.twinfield.com/" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
            <AccessToken xmlns="http://schemas.datacontract.org/2004/07/Twinfield.WebServices.Shared">{}</AccessToken>
            <CompanyCode xmlns="http://schemas.datacontract.org/2004/07/Twinfield.WebServices.Shared">{}</CompanyCode>
        </h:Authentication>
    </s:Header>
    <s:Body>
        <Query i:type="b:GetDeletedTransactions" xmlns="http://www.twinfield.com/" xmlns:a="http://schemas.datacontract.org/2004/07/Twinfield.WebServices" xmlns:i="http://www.w3.org/2001/XMLSchema-instance" xmlns:b="http://schemas.datacontract.org/2004/07/Twinfield.WebServices.DeletedTransactionsService">
            <b:CompanyCode>{}</b:CompanyCode>
            <b:DateFrom>{}</b:DateFrom>
            <b:DateTo>{}</b:DateTo>
        </Query>
    </s:Body>
</s:Envelope>
"""
