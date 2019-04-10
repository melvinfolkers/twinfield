


def soap_metadata(param, module):

    #   <office>{office}</office>

    xml = f'''<read>
    <type>browse</type>
    <code>{module}</code>
    </read>'''

    body = f'''<?xml version="1.0" encoding="utf-8"?>
           <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:twin="http://www.twinfield.com/">
           <soapenv:Header>
               <twin:Header>
               <twin:SessionID>{param.session_id}</twin:SessionID> 
                 </twin:Header>
              </soapenv:Header>
              <soapenv:Body>
                 <twin:ProcessXmlString>
                    <twin:xmlRequest><![CDATA[{xml}]]></twin:xmlRequest>
                 </twin:ProcessXmlString>
              </soapenv:Body>
           </soapenv:Envelope>'''

    return body

def soap_offices(session_id):
    '''

    :param session_id:  session id code
    :return: soap body voor de request van offices
    '''

    body = f'''<?xml version="1.0" encoding="utf-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:twin="http://www.twinfield.com/">
        <soapenv:Header>
            <twin:Header>
                 <twin:SessionID>{session_id}</twin:SessionID> 
              </twin:Header>
           </soapenv:Header>
           <soapenv:Body>
              <twin:ProcessXmlString>
                 <twin:xmlRequest><![CDATA[<list><type>offices</type></list>]]></twin:xmlRequest>
              </twin:ProcessXmlString>
           </soapenv:Body>
        </soapenv:Envelope>'''

    return body


def soap_select_office(param, officecode):
    '''

    :param session_id:  session id code
    :return: soap body voor de request van offices
    '''

    body = f'''<?xml version="1.0" encoding="utf-8"?>
     <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
         <soap:Header>
             <Header xmlns="http://www.twinfield.com/">
                 <SessionID>{param.session_id}</SessionID>
             </Header>
         </soap:Header>
         <soap:Body>
             <SelectCompany xmlns="http://www.twinfield.com/">
                 <company>{officecode}</company>
             </SelectCompany>
         </soap:Body>
     </soap:Envelope>'''

    return body



def soap_030_1(session_id, jaar):

    body = f'''<?xml version="1.0" encoding="utf-16"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <soap:Header>
    	 <Header xmlns="http://www.twinfield.com/">
    	  <SessionID>{session_id}</SessionID>
    	</Header>
      </soap:Header>
      <soap:Body>
    	 <ProcessXmlDocument xmlns="http://www.twinfield.com/">
    		<xmlRequest>
    <columns code="030_1">
    <column xmlns="">
          <field>fin.trs.head.office</field>
       </column>
    <column xmlns="">
          <field>fin.trs.head.officename</field>
       </column> 
       <column xmlns="">
          <field>fin.trs.head.yearperiod</field>
          <ask>true</ask>
          <operator>between</operator>
          <from>{jaar}/00</from>
          <to>{jaar}/55</to>
       </column> 
    <column xmlns="">
          <field>fin.trs.head.year</field>
       </column>
    <column xmlns="">
          <field>fin.trs.head.period</field>
       </column>
    <column xmlns="">
          <field>fin.trs.head.code</field>
       </column>
    <column xmlns="">
          <field>fin.trs.head.number</field>
          <label>Boekingsnummer</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.status</field>
          <label>Status</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.date</field>
          <label>Boekdatum</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.curcode</field>
          <label>Valuta</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.relationname</field>
          <label>Relatienaam</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.inpdate</field>
          <label>Invoerdatum</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.username</field>
          <label>Gebruikersnaam</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1</field>
          <label>Grootboekrek.</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1name</field>
          <label>Grootboekrek.naam</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1type</field>
          <label>Dimensietype 1</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim2</field>
          <label>Kpl./rel.</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim2name</field>
          <label>Kpl.-/rel.naam</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim2type</field>
          <label>Dimensietype 2</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim3</field>
          <label>Act./proj.</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim3name</field>
          <label>Act.-/proj.naam</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim3type</field>
          <label>Dimensietype 3</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.valuesigned</field>
          <label>Bedrag</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.basevaluesigned</field>
          <label>Basisbedrag</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.repvaluesigned</field>
          <label>Rapportagebedrag</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.debitcredit</field>
          <label>D/C</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.vatcode</field>
          <label>Btw-code</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.vatbasevaluesigned</field>
          <label>Btw-bedrag</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.quantity</field>
          <label>Aantal</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.chequenumber</field>
          <label>Cheque</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.description</field>
          <label>Omschrijving</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.invnumber</field>
          <label>Factuurnummer</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1group1</field>
          <label>Groep 1</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1group1name</field>
          <label>Groepnaam 1</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1group2</field>
          <label>Groep 2</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1group2name</field>
          <label>Groepnaam 2</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1group3</field>
          <label>Groep 3</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.dim1group3name</field>
          <label>Groepnaam 3</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.freetext1</field>
          <label>Vrij tekstveld 1</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.freetext2</field>
          <label>Vrij tekstveld 2</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.line.freetext3</field>
          <label>Vrij tekstveld 3</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.head.origin</field>
          <label>Boekingsoorsprong</label>
          <visible>true</visible>
       </column>
    <column xmlns="">
          <field>fin.trs.mng.type</field>
          <label>transactie type groep</label>
          <visible>true</visible>
       </column>
    </columns>
    		</xmlRequest>
    	</ProcessXmlDocument>
      </soap:Body>
    </soap:Envelope>'''

    return body



def soap_050_1(session_id):

    body = f'''<?xml version="1.0" encoding="utf-16"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <soap:Header>
    	 <Header xmlns="http://www.twinfield.com/">
    	  <SessionID>{session_id}</SessionID>
    	</Header>
      </soap:Header>
      <soap:Body>
    	 <ProcessXmlDocument xmlns="http://www.twinfield.com/">
    		<xmlRequest>
    <columns code="050_1">
    <column xmlns="">
    </columns>
    		</xmlRequest>
    	</ProcessXmlDocument>
      </soap:Body>
    </soap:Envelope>'''

    return body

