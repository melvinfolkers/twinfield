from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
import logging

from scripts.export import push_bigquery

from credentials import auth_azure

os.chdir(currentdir)


def run_twinfield_upload():
    connectionstring = auth_azure(database="landing")
    engn = create_engine(connectionstring, pool_size=10, max_overflow=20)

    twin = import_twinfield_2019(engn)
    data = prepare_twinfield(twin)

    push_bigquery(df=data, containername="yellowstacks-217623", foldername="Yellowstacks", tablename="HappyNurse")

    return data


def import_twinfield_2019(engn):
    twin19 = pd.read_sql_query("SELECT * FROM bc.twin19 WHERE Werkmaatschappij = 'HNT'", con=engn)  # read data
    twin = twin19.loc[twin19.Status != "draft"]

    return twin


def add_grootboektype(twin, mapfile):
    with open(mapfile, "r") as fp:
        fm = json.load(fp)

    twin["grootboektype"] = twin["Dimensietype 1"].map(fm)
    twin["grootboektype"] = twin["grootboektype"].fillna(twin["Dimensietype 1"])

    return twin


def add_dagboektype(twin, mapfile):
    with open(mapfile, "r") as fp:
        fm = json.load(fp)

    twin["dagboektype"] = twin["transactie type groep"].map(fm)
    twin["dagboektype"] = twin["dagboektype"].fillna(twin["transactie type groep"])

    return twin


def transform_fieldnames(twin, mapfile):
    with open(mapfile, "r") as fp:
        fm = json.load(fp)

    twin.rename(columns=fm, inplace=True)
    df = twin[fm.values()]

    return df


def prepare_twinfield(twin):
    twin = add_grootboektype(
        twin,
        mapfile="/Users/melvinfolkers/Documents/github/py_auditfiles/scripts/mapping/twinfield/content/grootboektype.json",
    )
    twin = add_dagboektype(
        twin,
        mapfile="/Users/melvinfolkers/Documents/github/py_auditfiles/scripts/mapping/twinfield/content/dagboektype.json",
    )
    twin["version"] = "twinfield api"
    twin["grootboek"] = twin["Grootboekrek."] + " - " + twin["Grootboekrek.naam"]

    data = transform_fieldnames(
        twin, mapfile="/Users/melvinfolkers/Documents/github/py_auditfiles/scripts/mapping/twinfield/twinfield.json"
    )

    return data
