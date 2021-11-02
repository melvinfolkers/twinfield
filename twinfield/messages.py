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
