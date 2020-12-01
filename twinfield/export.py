import os
import pandas as pd
import logging
from df_to_azure.export import run as df_to_azure


def upload_addresses(run_params):
    directory = os.path.join(run_params.pickledir, "addresses")
    all_addresses = list()

    for file in os.listdir(directory):
        if file.endswith(".pkl"):
            df = pd.read_pickle(os.path.join(directory, file))
            all_addresses.append(df)

    data = pd.concat(all_addresses, axis=0, sort=False, ignore_index=True)
    df_to_azure(data, "dimension_address", schema="twinfield", method="create")


def append_responses(r, run_params):

    logging.info(f"start met aanbieden van {len(r)} records voor {run_params.modules}")

    if run_params.modules == "upload_dimensions":
        return None

    if run_params.modules == "vrk":
        df_to_azure(r, "responses_verkoop", schema="twinfield", method="append")
    elif run_params.modules == "ink":
        df_to_azure(r, "responses_inkoop", schema="twinfield", method="append")
    elif run_params.modules == "memo":
        df_to_azure(r, "responses_memo", schema="twinfield", method="append")
    elif run_params.modules == "salesinvoice":
        df_to_azure(r, "responses_salesinvoice", schema="twinfield", method="append")
    elif run_params.modules == "read_dimensions":
        df_to_azure(r, "dimensions", schema="twinfield", method="create")
        upload_addresses(run_params)


def upload_responses(run_params):

    if run_params.modules != "upload_dimensions":
        r = import_responses(run_params)
    else:
        return None

    if run_params.modules == "vrk":
        df_to_azure(r, "responses_verkoop", schema="twinfield", method="create")
    elif run_params.modules == "ink":
        df_to_azure(r, "responses_inkoop", schema="twinfield", method="create")
    elif run_params.modules == "memo":
        df_to_azure(r, "responses_memo", schema="twinfield", method="create")
    elif run_params.modules == "salesinvoice":
        df_to_azure(r, "responses_salesinvoice", schema="twinfield", method="create")
    elif run_params.modules == "read_dimensions":
        df_to_azure(r, "dimensions", schema="twinfield", method="create")
        upload_addresses(run_params)


def import_responses(run_params):
    dfs = list()
    for file in os.listdir(run_params.pickledir):
        if file.startswith("response_data"):
            df = pd.read_pickle(os.path.join(run_params.pickledir, file))
            dfs.append(df)

    data = pd.concat(dfs, axis=0, sort=False, ignore_index=True)

    if "description" in data.columns:
        data["description"] = data["description"].str.replace("\n", "")

    return data
