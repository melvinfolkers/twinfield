import logging
from xml.etree import ElementTree as ET
import pandas as pd


def parse_several_modules(dimension) -> dict:
    """

    Parameters
    ----------
    dimension: xml layer of dimensions submodule

    Returns dictionary containing parsed data
    -------

    """
    credman = dimension.find("creditmanagement")
    d_credman = parse_layer(credman)

    remad = dimension.find("remittanceadvice")
    d_remad = parse_layer(remad)

    inv = dimension.find("invoicing")
    d_inv = parse_layer(inv)

    d = {**d_credman, **d_remad, **d_inv}

    return d


def parse_financials(dimension) -> dict:
    """

    Parameters
    ----------
    dimension: xml layer of dimensions submodule

    Returns dictionary containing parsed data
    -------

    """
    financials = dimension.find("financials")
    d_financials = parse_layer(financials)

    mandate = financials.find("collectmandate")
    d_mandate = parse_layer(mandate)

    chilval = financials.find("childvalidations")
    d_chilval = parse_layer(chilval)

    financial_info = {**d_mandate, **d_financials, **d_chilval}

    return financial_info


def parse_banks(dimension) -> dict:
    """

    Parameters
    ----------
    dimension: xml layer of dimensions submodule

    Returns dictionary containing parsed data
    -------

    """
    banks = dimension.find("banks")
    bank_info = parse_layer(banks)
    if banks:
        bank = banks.find("bank")
        d_bank = parse_layer(bank)
        bank_info = {**bank_info, **d_bank}

    return bank_info


def parse_addresses(dimension) -> dict:
    """

    Parameters
    ----------
    dimension: xml layer of dimensions submodule

    Returns dictionary containing parsed data
    -------

    """
    addresses = dimension.find("addresses")
    d_addresses = parse_layer(addresses)
    address = addresses.find("address")
    d_address = parse_layer(address)

    address_info = {**d_address, **d_addresses}

    return address_info


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


def parse_transactions(transactions) -> list:
    """

    Parameters
    ----------
    transactions: xml layer of transaction related submodules

    Returns list of dictionaries containing parsed data
    -------

    """

    records = []

    for trx in transactions:
        record = parse_layer(trx)
        records.append(record.copy())

    return records


def parse_dimensions(dimensions) -> list:
    """

    Parameters
    ----------
    dimensions: xml layers of dimensions

    Returns list of dictionaries containing parsed data
    -------

    """
    records = []

    for dimension in dimensions:
        d_dim = parse_layer(dimension)
        # d_addr = parse_addresses(dimension)
        d_bank = parse_banks(dimension)
        d_fin = parse_financials(dimension)
        d_mod = parse_several_modules(dimension)

        record = {**d_dim, **d_bank, **d_fin, **d_mod}
        records.append(record.copy())

    return records


def get_dimension_codes(dimension) -> dict:
    """

    Parameters
    ----------
    dimensions: xml layers of dimensions

    Returns list of dictionaries containing parsed data
    -------

    """
    dimdata = parse_layer(dimension)
    fields = ["dimension.office", "dimension.type", "dimension.code"]
    d = {k: v for k, v in dimdata.items() if k in fields}

    return d


def parse_response_dimension_addresses(response, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    response: response from twinfield server
    login:  login parameters (SessionParameters)

    Returns None. exports the dimension address data to pickle file in the tmp directory
    -------

    """
    root = ET.fromstring(response.text)
    body = root.find("env:Body", login.ns)
    data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", login.ns)
    dimensions = data.findall("dimensions/dimension")
    records = []

    for dimension in dimensions:

        dim = get_dimension_codes(dimension)
        addresses = dimension.find("addresses")
        d_addresses = parse_layer(addresses)

        for address in addresses:
            attrib = address.attrib
            d_address = parse_layer(address)

            total = {**attrib, **d_address, **dim, **d_addresses}
            records.append(total)

    df = pd.DataFrame(records)

    if not len(df):
        logging.debug("geen addressen geexporteerd")
        return pd.DataFrame()

    return df


def parse_response_dimensions(response, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    response: response from twinfield server
    login:  login parameters (SessionParameters)

    Returns None. exports the dimension address data to pickle file in the tmp directory
    -------

    """
    root = ET.fromstring(response.text)
    body = root.find("env:Body", login.ns)
    data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", login.ns)
    dimensions = data.findall("dimensions/dimension")
    records = parse_dimensions(dimensions)
    data = pd.DataFrame(records)

    return data


def parse_response_transactions(run_params, response, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    response: response from twinfield server
    login:  login parameters (SessionParameters)

    Returns None. exports the dimension address data to pickle file in the tmp directory
    -------

    """
    # logging.info(f"start van parsen {run_params.modules}")
    root = ET.fromstring(response.text)
    body = root.find("env:Body", login.ns)
    data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", login.ns)

    if run_params.modules == "salesinvoice":
        trx = data.find("salesinvoices/salesinvoice")
    else:
        trx = data.find("transactions/transaction")

    records = parse_transactions(trx)
    data = pd.DataFrame(records)

    return data


def parse_result_status(data_xml, module) -> str:
    """

    Parameters
    ----------
    data_xml: response from twinfield server
    module: selected module

    Returns result of status. if not there, returns error
    -------

    """
    result_xml = parse_layer(data_xml)
    colname = "{http://www.twinfield.com/}ProcessXmlDocumentResult." + module + ".result"
    result = result_xml.get(colname, "error")

    return result


def parse_memo(run_params, response, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    response: response from twinfield server
    login:  login parameters (SessionParameters)

    Returns
    -------

    """

    # logging.info(f"start van parsen {run_params.modules}")
    root = ET.fromstring(response.text)
    body = root.find("env:Body", login.ns)
    data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", login.ns)
    trx = data.find("transactions/transaction")

    records = []

    header_d = parse_layer(trx.find("header"))
    header_d["result"] = parse_result_status(data, "transactions")
    lines = trx.find("lines")

    for line in lines:
        info = {f"{lines.tag}.line.{k}": v for k, v in line.attrib.items()}
        line_d = parse_layer(line)
        record = {**line_d.copy(), **header_d.copy(), **info.copy()}
        records.append(record.copy())

    data = pd.DataFrame(records)

    return data


def get_header_data(trx) -> dict:
    """

    Parameters
    ----------
    trx: transaction line in xml

    Returns dictionary containing data
    -------

    """
    info = dict()

    header = trx.find("header")
    if header.attrib:
        info = {**info, **header.attrib}

    for col in header:
        attr = col.attrib

        if len(attr):
            attr = {f"{col.tag}.{k}": v for k, v in attr.items()}

            value = col.find("value")
            if value:
                attr[f"{col.tag}.value"] = col.find("value").text
            if col.text:
                attr[f"{col.tag}.value"] = col.text

            info = {**attr, **info}
        else:
            val = col.text
            name = "header." + col.tag

            info = {**info, name: val}

        if len(col) != 0:
            info[f"{col.tag}.value"] = col.find("value").text

    return info


def parse_response(run_params, response, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    response: response from twinfield server
    login:  login parameters (SessionParameters)

    Returns datadrame of parsed responses
    -------

    """
    if run_params.modules == "vrk":
        data = parse_response_ljp(run_params, response, login)
    elif run_params.modules == "ink":
        data = parse_response_ljp(run_params, response, login)
    elif run_params.modules == "memo":
        data = parse_memo(run_params, response, login)
    elif run_params.modules == "ljp":
        data = parse_response_transactions(run_params, response, login)
    elif run_params.modules == "salesinvoice":
        data = parse_response_ljp(run_params, response, login)
    elif run_params.modules == "upload_dimensions":
        logging.debug(f"geen parsing mogelijk voor {run_params.modules}")
        data = pd.DataFrame()
        # data = parse_response_ljp(run_params, response, login)
    else:
        logging.info(f"response niet kunnen parsen voor script {run_params.modules}")
        data = pd.DataFrame()

    return data


def parse_response_ljp(run_params, response, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    response: response from twinfield server
    login:  login parameters (SessionParameters)

    Returns dataframe containing responsess
    -------

    """
    root = ET.fromstring(response.text)
    body = root.find("env:Body", login.ns)

    data = body.find("tw:ProcessXmlDocumentResponse/tw:ProcessXmlDocumentResult", login.ns)

    if run_params.modules == "vrk" or run_params.modules == "ink":
        trx = data.findall("transactions/transaction")[0]
    elif run_params.modules == "salesinvoice":
        trx = data.findall("salesinvoices/salesinvoice")[0]

    else:
        trx = None
        logging.info(f"geen routine gemaakt voor script {run_params.modules}")
    # raise KostenPlaatsError("Kostenplaats ontbreekt in TwinField.")

    ttl_records = list()

    if run_params.modules == "debtors":
        status_dict = {}
        lines = trx
    else:
        status_dict = trx.attrib
        lines = trx.find("lines")

    for line in lines:

        info = dict()

        for col in line:
            attr = col.attrib

            if len(attr):

                attr = {f"{col.tag}.{k}": v for k, v in attr.items()}

                value = col.find("value")
                if value:
                    attr[f"{col.tag}.value"] = col.find("value").text

                if col.text:
                    attr[f"{col.tag}.value"] = col.text

                info = {**attr, **info}
            else:
                info = {**info, col.tag: col.text}

        if run_params.modules != "debtors":
            headerdata = get_header_data(trx)
            info = {**info, **headerdata}

        info = {**status_dict, **info}
        ttl_records.append(info)

    data = pd.DataFrame(ttl_records)

    return data
