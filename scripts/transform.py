import os

import pandas as pd

from .functions import import_files, get_metadata
from .run_settings import get_twinfield_settings


def format_100(df):
    login = get_twinfield_settings()
    fields = get_metadata("100", login)
    df.rename(columns=fields["label"], inplace=True)

    # format numbers
    numbers = []
    for column in numbers:
        if column in df.columns:
            df[column] = df[column].astype(float)

    return df


def format_200(df):
    login = get_twinfield_settings()
    fields = get_metadata("200", login)
    df.rename(columns=fields["label"], inplace=True)

    # format numbers
    numbers = []
    for column in numbers:
        if column in df.columns:
            df[column] = df[column].astype(float)

    return df


def format_040_1(df):
    login = get_twinfield_settings()
    fields = get_metadata("040_1", login)
    df.rename(columns=fields["label"], inplace=True)

    # format numbers
    numbers = []
    for column in numbers:
        if column in df.columns:
            df[column] = df[column].astype(float)

    return df


def format_030_1(df):
    df.columns = [x.replace("fin.trs.", "") for x in df.columns]

    # format dates
    if "head.date" in df.columns:
        df["head.date"] = pd.to_datetime(df["head.date"], format="%Y%m%d")
    if "head.inpdate" in df.columns:
        df["head.inpdate"] = pd.to_datetime(df["head.inpdate"], format="%Y%m%d%H%M%S")

    # format numbers
    numbers = [
        "line.basevaluesigned",
        "line.valuesigned",
        "line.repvaluesigned",
        "line.vatbasevaluesigned",
        "line.quantity",
    ]

    for column in numbers:
        if column in df.columns:
            df[column] = df[column].astype(float)

    return df


def format_164(df):
    df.columns = [x.replace("fin.trs.", "") for x in df.columns]

    # format dates

    # format numbers

    return df


def maak_samenvatting(run_params):
    df = import_files(run_params, "transactions")

    aggcols = [
        "wm",
        "administratienummer",
        "head.year",
        "head.period",
        "head.status",
        "head.relationname",
        "line.dim1",
        "line.dim2",
        "line.dim2name",
    ]
    df.update(df[aggcols].fillna(""))
    agg = df.groupby(aggcols)["line.valuesigned"].sum().reset_index()

    fieldmapping = {
        "head.year": "Jaar",
        "head.period": "Periode",
        "head.status": "Status",
        "head.relationname": "Relatienaam",
        "line.dim1": "Grootboekrek.",
        "line.dim2": "Kpl./rel.",
        "line.dim2name": "Kpl.-/rel.naam",
        "line.valuesigned": "Bedrag",
    }

    agg.rename(columns=fieldmapping, inplace=True)
    agg.to_pickle(os.path.join(run_params.pickledir, "summary.pkl"))
