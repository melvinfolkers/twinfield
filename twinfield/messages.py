import os
import pickle
import numpy as np
import pandas as pd
import logging
from df_to_azure.export import auth_azure
from sqlalchemy import create_engine


def parse_line(row):
    line = f"""<line id="{row['line_id']}" journalId="0">
      <dim1>{row['grootboek']}</dim1>
      <dim2>{row['kostenplaats']}</dim2>
      <debitcredit>{row['debit_credit']}</debitcredit>
      <value>{row['bedrag_abs']}</value>
      <description>{row['omschrijving']}</description>
    </line>"""

    return line


def parse_header(row):
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


def create_line(data):
    data["line_xml"] = data.apply(lambda x: parse_line(x), axis=1)

    return data


def create_header(data):
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


def prep_data(data, f):
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


def write_xml_file(run_params, message, filename):

    new_filename = filename.lower().replace(".csv", ".xml")
    f = open(os.path.join(run_params.xmldir, new_filename), "w")
    f.write(message)
    f.close()
    logging.debug("XML is geschreven")


def transform_soap_message(run_params):

    xml_messages = []

    for filename in os.listdir(run_params.rawdir):
        if filename.endswith(".xml"):
            f = open(os.path.join(run_params.rawdir, filename), "r", encoding="utf-8")
            message = f.read()

            return message

            d = dict()
            d["office_code"] = "ZZ_1060255"  # let op deze moet variabel worden
            d["xml_msg"] = message

        xml_messages.append(d)

    return xml_messages


def convert_xml(xml_msg):

    new = xml_msg.split("\n", 1)[1]
    new = [x for x in new.split() if x != "</transaction>"]
    r = "\n".join(new)

    return r


def create_dimensions_sql():
    xml_messages = []
    data = read_and_delete_table("dimensie_wijziging_xml")
    data["soap_xml"] = data.Dimensies

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Administratie_nummer"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def create_facturen_sql():
    xml_messages = []
    data = read_and_delete_table("facturen_xml")
    data["soap_xml"] = data.Samenvoegen.apply(lambda x: convert_xml(x))
    data["soap_xml"] = data.Samenvoegen

    d = dict()
    for index, row in data.iterrows():
        d["office_code"] = row["Twinfield_OfficeCode"]
        d["xml_msg"] = row["soap_xml"]
        xml_messages.append(d.copy())

    return xml_messages


def create_memoriaal_journaalpost_sql():
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


def create_inkoopjournaalpost_sql():
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


def read_table(tablename):

    conn = auth_azure()
    engn = create_engine(conn, pool_size=10, max_overflow=20)
    data = pd.read_sql_table(table_name=tablename, con=engn)
    logging.info(f"{len(data)} regels gelezen.")

    return data


def read_and_delete_table(tablename):

    conn = auth_azure()
    engn = create_engine(conn, pool_size=10, max_overflow=20)
    data = pd.read_sql_table(table_name=tablename, con=engn)
    data.head(n=0).to_sql(tablename, engn, schema="dbo", if_exists="replace")
    logging.info(f"{len(data)} regels gelezen. tabel verwijderd")

    return data


def create_verkoopjournaalpost_sql():

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


def create_verkoopjournaalpost(run_params):

    xml_messages = []

    for filename in os.listdir(run_params.rawdir):
        if filename.endswith(".xml"):
            f = open(os.path.join(run_params.rawdir, filename), "r", encoding="utf-8")
            message = f.read()
            d = {"office_code": "ZZ_1060255", "xml_msg": message}

        xml_messages.append(d)
    return xml_messages


def create_loonjournaalpost(run_params):
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


def get_dimension_parameters(run_params):
    data = read_table("dim_requests")

    messages = []

    # if run_params.debug:
    #     for dim_type in ["DEB", "CRD"]:
    #         d = dict()
    #         d["office_code"] = "FACTURATIE007"
    #         d["xml_msg"] = ""
    #         d["dim_type"] = dim_type
    #         messages.append(d.copy())
    #     return messages

    for index, row in data.iterrows():
        d = dict()

        d["office_code"] = row["OFFICECODE"]
        d["xml_msg"] = ""
        d["dim_type"] = row["dim_type"]

        messages.append(d.copy())

    return messages


def create_messages_files(run_params) -> list:

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
        messages = get_dimension_parameters(run_params)
    elif run_params.modules == "upload_dimensions":
        messages = create_dimensions_sql()
    else:
        messages = {}

    with open(os.path.join(run_params.pickledir, "messages.pkl"), "wb") as f:
        pickle.dump(messages, f)

    logging.info(f"aantal soap berichten {len(messages)}")

    return messages
