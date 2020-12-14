import os
import pickle
import numpy as np
import pandas as pd
import logging
from df_to_azure.export import auth_azure
from sqlalchemy import create_engine


def parse_line(row) -> str:
    """
    Parameters
    ----------
    row
        record in dataframe that will be parsed in the soap_xml template for twinfield.

    Returns
    -------
    line: str
        soap xml message for creating 'loonjournaalpost'
    """
    line = f"""<line id="{row['line_id']}" journalId="0">
      <dim1>{row['grootboek']}</dim1>
      <dim2>{row['kostenplaats']}</dim2>
      <debitcredit>{row['debit_credit']}</debitcredit>
      <value>{row['bedrag_abs']}</value>
      <description>{row['omschrijving']}</description>
    </line>"""

    return line


def parse_header(row) -> str:
    """
    Parameters
    ----------
    row
        record in dataframe that will be parsed in the soap_xml template for twinfield.

    Returns
    -------
    header: str
        soap xml message for creating the header
    """
    header = f"""<header>
        <office>{row['office_code']}</office>
        <code>MEMO</code>
        <number>0</number>
        <period>{row['year_period']}</period>
        <currency>EUR</currency>
        <date>{row['datum_tekst']}</date>
        <freetext1>loonjournaalpost</freetext1>
    </header>"""

    return header


def create_line(data: pd.DataFrame) -> pd.DataFrame:
    """
    Parameters
    ----------
    data: pd.DataFrame
        dataframe containing input for the soap request

    Returns
    -------
    data: pd.DataFrame
        the same dataset, with a new column called line_xml. this column contains the xml
        soap message that twinfield will understand.
    """
    data["line_xml"] = data.apply(lambda x: parse_line(x), axis=1)

    return data


def create_header(data: pd.DataFrame) -> pd.DataFrame:
    """
    Parameters
    ----------
    data: pd.DataFrame
        Dataframe containing input for the soap request

    Returns
    -------
    df_unique: pd.DataFrame
        dataframe with header xml messages for twinfield server
    """
    df_unique = data.drop_duplicates(
        subset=["BV", "office_code", "omschrijving", "datum", "periode"]
    ).copy()
    df_unique["year_period"] = (
        df_unique["jaar"].astype(str) + "/" + df_unique["periode"].str.zfill(2)
    )
    df_unique["datum_tekst"] = df_unique["datum"].dt.strftime("%Y%m%d")
    df_unique["header"] = df_unique.apply(lambda x: parse_header(x), axis=1)

    logging.info("Header voor de API communicatie is gecreeërd")

    return df_unique


def prep_data(data: pd.DataFrame, f: str) -> pd.DataFrame:
    """
    Parameters
    ----------
    data: pd.DataFrame
        dataframe containing input for the soap request
    f: str
        filename

    Returns
    -------
    data: pd.DataFrame
        Dataframe with content of xml messages for twinfield server
    """
    logging.info("Data wordt opgeschoond")

    data["datum"] = pd.to_datetime(data["datum"], format="%d-%m-%Y")
    data["jaar"] = data["datum"].dt.year
    data["BV"] = f.split()[0]
    data["line_id"] = data.index + 1
    data["bedrag"] = pd.to_numeric(data["bedrag"])
    data["kostenplaats"] = np.where(data["kostenplaats"].isna(), "", "K" + data["kostenplaats"])
    data["bedrag_abs"] = data["bedrag"].abs()
    data["debit_credit"] = np.where(data["bedrag"].lt(0), "credit", "debit")
    data["office_code"] = data["BV"].map({"BBF": "ZZ_1060270", "CAP": "ZZ_1060260"})

    logging.info("Data is opgeschoond en klaar om naar xml te schrijven")

    return data


def write_xml_file(run_params, message, filename) -> None:
    """
    Parameters
    ----------
    run_params
        input parameters of script (set at start of script)
    message
        xml message to be send to Twinfield server
    filename
        filename to write the xml message to.

    Returns
    -------
    None. Writes the XML message to temp 'xmldir' folder.
    """

    new_filename = filename.lower().replace(".csv", ".xml")
    f = open(os.path.join(run_params.xmldir, new_filename), "w")
    f.write(message)
    f.close()
    logging.debug("XML is geschreven")


def convert_xml(xml_msg: str) -> str:
    """
    Parameters
    ----------
    xml_msg: str
        XML soap message

    Returns
    -------
    r: str
        cleaned soap message without the label '</transaction>'
    """

    new = xml_msg.split("\n", 1)[1]
    new = [x for x in new.split() if x != "</transaction>"]
    r = "\n".join(new)

    return r


def create_dimensions_sql() -> list:
    """
    Returns
    -------
    xml_messages: list
        List of messages for module 'create_dimensions'
    """
    xml_messages = []
    data = read_and_delete_table("dimensie_wijziging_xml")
    data["soap_xml"] = data['Dimensies']

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Administratie_nummer"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def create_facturen_sql() -> list:
    """
    Returns
    -------
    xml_messages: list
        xml_messages for module 'salesinvoice'
    """
    xml_messages = []
    data = read_and_delete_table("facturen_xml")
    data["soap_xml"] = data['Samenvoegen']

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Twinfield_OfficeCode"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def create_memoriaal_journaalpost_sql() -> list:
    """
    Returns xml_messages for module 'memo'
    -------

    """

    xml_messages = []
    data = read_and_delete_table("memo_xml")
    data["soap_xml"] = data.Samenvoegen.apply(lambda x: convert_xml(x))
    data["soap_xml"] = data.Samenvoegen

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Twinfield_OfficeCode"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def create_inkoopjournaalpost_sql() -> list:
    """

    Returns xml_messages for module 'ink'
    -------

    """
    xml_messages = []
    data = read_and_delete_table("inkoop_xml")
    data["soap_xml"] = data.Samenvoegen.apply(lambda x: convert_xml(x))
    data["soap_xml"] = data.Samenvoegen

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Twinfield_OfficeCode"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def read_table(tablename) -> pd.DataFrame:
    """

    Parameters
    ----------
    tablename: tablename in DWH to read

    Returns: dataframe containing records of selected tablename
    -------

    """

    conn = auth_azure()
    engn = create_engine(conn, pool_size=10, max_overflow=20)
    data = pd.read_sql_table(table_name=tablename, con=engn)
    logging.info(f"{len(data)} regels gelezen.")

    return data


def read_and_delete_table(tablename) -> pd.DataFrame:
    """

    Parameters
    ----------
    tablename: tablename in DWH to read an delete

    Returns: dataframe containing records of selected tablename. original table will be deleted.
    -------

    """

    conn = auth_azure()
    engn = create_engine(conn, pool_size=10, max_overflow=20)
    query = f"SELECT TOP(1) * FROM {tablename};"
    data = pd.read_sql(query, con=engn)
    data.head(n=0).to_sql(tablename, engn, schema="dbo", if_exists="replace")
    logging.info(f"Tabel {tablename} gelezen. Tabel verwijderd")

    return data


def create_verkoopjournaalpost_sql() -> list:
    """

    Returns xml_messages for module 'vrk'
    -------

    """

    data = read_and_delete_table("verkoopjournaal_xml")
    data["soap_xml"] = data.Samenvoegen.apply(lambda x: convert_xml(x))
    data["soap_xml"] = data.Samenvoegen
    xml_messages = []

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Twinfield_OfficeCode"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def create_loonjournaalpost(run_params) -> list:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    Returns xml_messages for module 'ljp'
    -------

    """
    extension = ".csv"
    col_names = [
        "regel",
        "dagboek",
        "afschrift",
        "datum",
        "periode",
        "grootboek",
        "kostenplaats",
        "leeg1",
        "omschrijving",
        "bedrag",
        "leeg2",
    ]

    xml_messages = []
    for filename in os.listdir(run_params.rawdir):
        if filename.endswith(extension):
            df = pd.read_csv(
                os.path.join(run_params.rawdir, filename), sep=";", names=col_names, dtype="object"
            )
            df = prep_data(df, filename)
            xml_header = create_header(df)
            xml_lines = create_line(df)
            message = (
                xml_header["header"].iat[0]
                + "\n"
                + "<lines>\n"
                + "\n".join(xml_lines["line_xml"])
                + "\n"
                + "</lines>"
            )

            write_xml_file(run_params, message, filename)
            d = {"office_code": df.iloc[0].office_code, "xml_msg": message}

            xml_messages.append(d)

    return xml_messages


def get_dimension_parameters() -> list:
    """

    Returns xml messages for 'read_dimensions'
    -------

    """
    data = read_table("dim_requests")
    messages = []

    for index, row in data.iterrows():
        d = dict()

        d["office_code"] = row["OFFICECODE"]
        d["xml_msg"] = ""
        d["dim_type"] = row["dim_type"]

        messages.append(d.copy())

    return messages


def create_messages_files(run_params) -> list:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    Returns xml messages the will be uploaded to the Twinfield server.
    -------

    """

    if run_params.modules == "ljp":
        messages = create_loonjournaalpost(run_params)
    elif run_params.modules == "vrk":
        messages = create_verkoopjournaalpost_sql()
    elif run_params.modules == "memo":
        messages = create_memoriaal_journaalpost_sql()
    elif run_params.modules == "ink":
        messages = create_inkoopjournaalpost_sql()
    elif run_params.modules == "salesinvoice":
        # messages = create_verkoopjournaalpost(run_params)
        messages = create_facturen_sql()
    elif run_params.modules == "read_dimensions":
        messages = get_dimension_parameters()
    elif run_params.modules == "upload_dimensions":
        messages = create_dimensions_sql()
    else:
        messages = {}

    with open(os.path.join(run_params.pickledir, "messages.pkl"), "wb") as f:
        pickle.dump(messages, f)

    logging.info(f"aantal soap berichten {len(messages)}")

    return messages
