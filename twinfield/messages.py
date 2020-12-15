import os
import pickle
import numpy as np
import pandas as pd
import logging
from sqlalchemy import create_engine


def auth_azure():

    connection_string = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        os.environ.get("ls_sql_database_user"),
        os.environ.get("ls_sql_database_password"),
        os.environ.get("ls_sql_server_name"),
        os.environ.get("ls_sql_database_name"),
        "ODBC Driver 17 for SQL Server",
    )

    return connection_string


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
    with open(os.path.join(run_params.xmldir, new_filename), "w") as f:
        f.write(message)
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


def create_xml_messages(sql_table_name: str) -> list:
    """
    Parameters
    ----------
    sql_table_name: str
        Name of the table with XML message in SQL DB.

    Returns
    -------
    xml_messages: list
        List of messages for module 'create_dimensions'
    """
    xml_messages = []
    data = read_and_delete_table(sql_table_name)

    # voor tabel dimensie_wijziging_xml zijn er andere settings.
    dimensions = sql_table_name.lower() == "dimensie_wijziging_xml"

    for index, row in data.iterrows():
        if dimensions:
            d = {"office_code": row["Administratie_nummer"], "xml_msg": row["Dimensies"]}
        else:
            d = {"office_code": row["Twinfield_OfficeCode"], "xml_msg": row["Samenvoegen"]}
        xml_messages.append(d.copy())

    return xml_messages


def read_table(tablename: str) -> pd.DataFrame:
    """
    Parameters
    ----------
    tablename: str
        Tablename in DWH to read

    Returns
    -------
    data: pd.DataFrame
        Dataframe containing records of selected tablename
    """

    with create_engine(auth_azure()).connect() as con:
        data = pd.read_sql_table(table_name=tablename, con=con)
    logging.info(f"{len(data)} regels gelezen.")

    return data


def read_and_delete_table(tablename: str) -> pd.DataFrame:
    """

    Parameters
    ----------
    tablename: str
        tablename in DWH to read an delete

    Returns
    -------
    data: pd.DataFarme
        Dataframe containing records of selected tablename. original table will be deleted.

    """
    with create_engine(auth_azure()).connect() as con:
        query = f"SELECT TOP(1) * FROM {tablename};"
        data = pd.read_sql(query, con=con)
        data.head(n=0).to_sql(tablename, con, schema="dbo", if_exists="replace")
    logging.info(f"Tabel {tablename} gelezen. Tabel verwijderd")

    return data


def create_loonjournaalpost(run_params) -> list:
    """
    Parameters
    ----------
    run_params
        Input parameters of script (set at start of script)

    Returns
    -------
    xml_messages: list
        Xml_messages for module 'ljp'
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
    Returns
    -------
    messages: list
        xml messages for 'read_dimensions'
    """
    data = read_table("dim_requests")
    messages = []

    for index, row in data.iterrows():
        d = {"office_code": row["OFFICECODE"], "xml_msg": "", "dim_type": row["dim_type"]}
        messages.append(d.copy())

    return messages


def mapping_module_script(run_params) -> dict:
    mapping = {
        "ljp": create_loonjournaalpost(run_params),
        "vrk": create_xml_messages("verkoopjournaal_xml"),
        "memo": create_xml_messages("memo_xml"),
        "ink": create_xml_messages("inkoop_xml"),
        "salesinvoice": create_xml_messages("facturen_xml"),
        "read_dimensions": get_dimension_parameters(),
        "upload_dimensions": create_xml_messages("dimensie_wijziging_xml"),
    }

    return mapping


def create_messages_files(run_params) -> list:
    """
    Parameters
    ----------
    run_params
        input parameters of script (set at start of script)

    Returns
    -------
    messages: list
        xml messages the will be uploaded to the Twinfield server.
    """

    # if run_params.modules == "ljp":
    #     messages = create_loonjournaalpost(run_params)
    # elif run_params.modules == "vrk":
    #     messages = create_xml_messages("verkoopjournaal_xml")
    # elif run_params.modules == "memo":
    #     messages = create_xml_messages("memo_xml")
    # elif run_params.modules == "ink":
    #     messages = create_xml_messages("inkoop_xml")
    # elif run_params.modules == "salesinvoice":
    #     # messages = create_verkoopjournaalpost(run_params)
    #     messages = create_xml_messages("facturen_xml")
    # elif run_params.modules == "read_dimensions":
    #     messages = get_dimension_parameters()
    # elif run_params.modules == "upload_dimensions":
    #     messages = create_xml_messages("dimensie_wijziging_xml")
    # else:
    #     messages = {}

    # TODO: testen of deze mapping werkt, anders oude code hierboven gebruiken.
    #   Ik weet dus niet of deze gelijk de hele dictionary aanmaakt,
    #   dus alle messages aanmaakt.
    mapping = mapping_module_script(run_params)
    messages = mapping.get(run_params.modules, {})

    with open(os.path.join(run_params.pickledir, "messages.pkl"), "wb") as f:
        pickle.dump(messages, f)

    logging.info(f"aantal soap berichten {len(messages)}")

    return messages
